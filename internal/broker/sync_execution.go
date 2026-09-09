package broker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/synctree"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
)

type preparedSync struct {
	Owner       string
	Host        string
	Target      string
	Options     client.SyncOptions
	Destination string
	Plan        synctree.Plan
	StageID     string
	Expires     time.Time
	Release     func()
	Taken       bool
}

func (s *Service) ConfigureSync(path string) error {
	store, err := synctree.NewStore(path)
	if err != nil {
		return err
	}
	s.syncStore = store
	return nil
}

// syncJoined keeps caller-owned plan data and reservations alive until an
// admitted worker has stopped. A canceled queued callback can never start later.
func (s *Service) syncJoined(ctx context.Context, req Request, fn func(context.Context) (*proto.Response, error)) (*proto.Response, error) {
	var state atomic.Int32
	done := make(chan struct{})
	resp, err := s.DispatchScheduled(ctx, req.Host, req.Owner.Key(), LaneBulk, func(work context.Context) (*proto.Response, error) {
		if !state.CompareAndSwap(0, 1) {
			return nil, context.Canceled
		}
		defer close(done)
		return fn(work)
	})
	if !state.CompareAndSwap(0, 2) {
		<-done
	}
	return resp, err
}
func (s *Service) syncRPC(ctx context.Context, req Request, target, op, id string, params *proto.SyncParams) (*proto.SyncResult, error) {
	results := make(chan *proto.SyncResult, 1)
	_, err := s.syncJoined(ctx, req, func(work context.Context) (*proto.Response, error) {
		release, err := s.Pool.dispatchLease(work, req.Host, LaneBulk)
		if err != nil {
			return nil, err
		}
		defer release()
		result, err := s.client.SyncProtocol(work, req.Host, target, req.Owner.ClientID, req.Owner.ProjectID, op, id, params)
		if result != nil {
			results <- result
		}
		return nil, err
	})
	if err != nil {
		select {
		case result := <-results:
			return result, err
		default:
			return nil, err
		}
	}
	return <-results, nil
}
func (s *Service) syncLocal(ctx context.Context, req Request, fn func(context.Context) error) error {
	_, err := s.syncJoined(ctx, req, func(work context.Context) (*proto.Response, error) { return nil, fn(work) })
	return err
}
func (s *Service) inspectSyncDestination(ctx context.Context, req Request, target, path string) (synctree.Snapshot, error) {
	if req.Sync.Direction == "pull" {
		return synctree.Inspect(ctx, path, synctree.StageLimits)
	}
	result, err := s.syncRPC(ctx, req, target, proto.OpSyncInspect, "", &proto.SyncParams{Path: path})
	if err != nil || result == nil || result.Snapshot == nil {
		return synctree.Snapshot{}, errors.New("sync destination unavailable")
	}
	return *result.Snapshot, nil
}
func (s *Service) PrepareSync(ctx context.Context, req Request) (*client.SyncResult, error) {
	if s.syncStore == nil {
		return nil, errors.New("sync state unavailable")
	}
	if err := validateSyncRoute(req); err != nil {
		return nil, err
	}
	opts, err := client.NormalizeSyncOptions(*req.Sync)
	if err != nil {
		return nil, err
	}
	if !opts.Prepare || !opts.DryRun {
		return nil, errors.New("sync prepare requires dry_run")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	release, err := s.Ingress.Hold(req.Owner.Key(), 16<<20)
	if err != nil {
		return nil, err
	}
	retained := false
	defer func() {
		if !retained {
			release()
		}
	}()
	target, err := s.client.ProtocolTargetIdentity(req.Host)
	if err != nil {
		return nil, err
	}
	id, err := synctree.NewID()
	if err != nil {
		return nil, err
	}
	owner := req.Owner.Key()
	var stage synctree.Stage
	complete := false
	defer func() {
		if !complete {
			cleanup, stop := context.WithTimeout(context.Background(), 2*time.Second)
			defer stop()
			_ = s.syncStore.Remove(cleanup, owner, id)
		}
	}()
	if opts.Direction == "push" {
		err = s.syncLocal(ctx, req, func(work context.Context) error {
			var e error
			stage, e = s.syncStore.Capture(work, owner, id, opts.Local, opts.SymlinkPolicy)
			return e
		})
	} else {
		var result *proto.SyncResult
		result, err = s.syncRPC(ctx, req, target, proto.OpSyncStage, "", &proto.SyncParams{Action: "capture", ID: id, Path: opts.Remote, Policy: opts.SymlinkPolicy})
		if err == nil && (result == nil || result.Stage == nil || !result.Stage.Ready) {
			err = errors.New("sync source capture missing")
		}
		if err == nil {
			stage = *result.Stage
			if stage.ID != id || synctree.ValidateManifest(stage.Manifest) != nil {
				return nil, synctree.ErrStage
			}
			_, err = s.syncStore.Begin(ctx, owner, id, stage.Manifest)
			if err == nil {
				for i, e := range stage.Manifest.Entries {
					if e.Kind != "file" {
						continue
					}
					for offset := int64(0); offset < e.Size; {
						chunk, readErr := s.syncRPC(ctx, req, target, proto.OpSyncStage, "", &proto.SyncParams{Action: "read", ID: id, Index: i, Offset: offset})
						if readErr != nil {
							err = readErr
							break
						}
						if chunk == nil || len(chunk.Data) == 0 || int64(len(chunk.Data)) > e.Size-offset {
							err = synctree.ErrStage
							break
						}
						if err = s.syncStore.Put(ctx, owner, id, i, offset, chunk.Data); err != nil {
							break
						}
						offset += int64(len(chunk.Data))
					}
					if err != nil {
						break
					}
				}
			}
			if err == nil {
				_, err = s.syncStore.Seal(ctx, owner, id)
			}
			_, _ = s.syncRPC(ctx, req, target, proto.OpSyncStage, "", &proto.SyncParams{Action: "remove", ID: id})
		}
	}
	if err != nil {
		return nil, errors.New("sync source could not be retained")
	}
	sourceStage := stage
	destination := opts.Remote
	sourceSpelling := opts.Local
	if opts.Direction == "pull" {
		destination = opts.Local
		sourceSpelling = opts.Remote
	}
	snap, err := s.inspectSyncDestination(ctx, req, target, destination)
	if err != nil {
		return nil, err
	}
	directoryTarget := snap.Exists && len(snap.Manifest.Entries) > 0 && snap.Manifest.Entries[0].Kind == "directory"
	prefix := stage.SourceDirectory && !strings.HasSuffix(sourceSpelling, "/") && directoryTarget
	renameFile := ""
	if !stage.SourceDirectory && !directoryTarget {
		renameFile = filepath.Base(filepath.Clean(destination))
		destination = filepath.Dir(filepath.Clean(destination))
		snap, err = s.inspectSyncDestination(ctx, req, target, destination)
		if err != nil || !snap.Exists {
			return nil, errors.New("sync destination parent unavailable")
		}
	}
	var rootEntry *synctree.Entry
	if (!stage.SourceDirectory || prefix) && snap.Exists && len(snap.Manifest.Entries) > 0 {
		copy := snap.Manifest.Entries[0]
		rootEntry = &copy
	}
	var deletions map[string]bool
	err = s.syncLocal(ctx, req, func(work context.Context) error {
		var e error
		stage, e = s.syncStore.Rewrite(work, owner, id, func(dir string) error {
			var err error
			deletions, err = s.client.SyncDeletionPaths(work, dir, sourceStage, snap, opts, prefix)
			if err != nil {
				return err
			}
			return s.client.FilterSyncStage(work, dir, opts, sourceStage.SourceDirectory, sourceStage.SourceName, prefix, renameFile, rootEntry)
		})
		return e
	})
	if err != nil {
		return nil, errors.New("sync retained source filtering failed")
	}
	plan, err := synctree.BuildScoped(stage.Manifest, snap, deletions, opts.ConflictPolicy)
	if err != nil {
		return nil, err
	}
	encodedPlan, err := json.Marshal(&proto.SyncParams{ID: id, Path: destination, Plan: &plan})
	if err != nil || int64(len(encodedPlan)) > proto.AbsoluteRequestFrameBytes-(16<<10) {
		return nil, errors.New("sync plan exceeds protocol metadata limit")
	}
	// File imports inherit source kind before local Begin; local stage metadata is
	// used only for its retained contents after filtering.
	prepared := &preparedSync{Owner: owner, Host: req.Host, Target: target, Options: opts, Destination: destination, Plan: plan, StageID: id, Expires: time.Now().Add(5 * time.Minute), Release: release}
	if len(s.syncSummary(prepared).Stdout) > int(opts.MaxOutputBytes) {
		return nil, errors.New("sync plan exceeds review output limit; raise max_output_bytes")
	}
	s.syncMu.Lock()
	owned := 0
	for key, old := range s.syncPlans {
		if time.Now().After(old.Expires) {
			delete(s.syncPlans, key)
			old.Release()
			continue
		}
		if old.Owner == owner {
			owned++
		}
	}
	if len(s.syncPlans) >= 16 || owned >= 4 {
		s.syncMu.Unlock()
		return nil, errors.New("sync plan capacity reached")
	}
	s.syncPlans[owner+"\x00"+id] = prepared
	s.syncMu.Unlock()
	complete, retained = true, true
	time.AfterFunc(time.Until(prepared.Expires), func() {
		s.syncMu.Lock()
		defer s.syncMu.Unlock()
		if s.syncPlans[owner+"\x00"+id] == prepared {
			delete(s.syncPlans, owner+"\x00"+id)
			prepared.Release()
		}
	})
	return s.syncSummary(prepared), nil
}
func (s *Service) syncSummary(p *preparedSync) *client.SyncResult {
	var lines strings.Builder
	fmt.Fprintf(&lines, "prepared %d exact path changes; source bytes %d\n", len(p.Plan.Changes), p.Plan.Source.ContentBytes)
	entryText := func(e *synctree.Entry) string {
		if e == nil {
			return "absent"
		}
		return fmt.Sprintf("%s size=%d mode=%o mtime_ns=%d sha256=%s link=%q", e.Kind, e.Size, e.Mode, e.ModifiedNS, e.Digest, s.client.Secrets.Redact(e.Link))
	}
	for _, change := range p.Plan.Changes {
		fmt.Fprintf(&lines, "%q: %s -> %s\n", s.client.Secrets.Redact(change.Path), entryText(change.Before), entryText(change.After))
	}
	return &client.SyncResult{PlanID: p.StageID, PlanExpiresAt: p.Expires, PlanChanges: len(p.Plan.Changes), PlanDigest: p.Plan.Digest, ManifestDigest: p.Plan.Source.Digest, ManifestEntries: len(p.Plan.Source.Entries), ManifestComplete: true, DryRun: true, Stdout: lines.String()}
}
func syncOptionsBinding(opts client.SyncOptions) string {
	opts.Prepare = false
	opts.DryRun = false
	opts.ConfirmDelete = false
	opts.PlanID = ""
	opts.Host = ""
	data, _ := json.Marshal(opts)
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}
func (s *Service) preparedSync(req Request) (*preparedSync, error) {
	if req.Sync == nil {
		return nil, errors.New("sync parameters missing")
	}
	opts, err := client.NormalizeSyncOptions(*req.Sync)
	if err != nil {
		return nil, err
	}
	s.syncMu.Lock()
	p := s.syncPlans[req.Owner.Key()+"\x00"+opts.PlanID]
	s.syncMu.Unlock()
	if p == nil || time.Now().After(p.Expires) || p.Host != req.Host || syncOptionsBinding(p.Options) != syncOptionsBinding(opts) {
		return nil, errors.New("sync plan unavailable or parameters changed")
	}
	target, err := s.client.ProtocolTargetIdentity(req.Host)
	if err != nil || target != p.Target {
		return nil, errors.New("sync target changed")
	}
	return p, nil
}
func (s *Service) SyncApproval(req Request, decision Decision) (ApprovalPlan, error) {
	p, err := s.preparedSync(req)
	if err != nil {
		return ApprovalPlan{}, err
	}
	data, _ := json.Marshal(struct {
		Plan    string
		Options string
	}{p.Plan.Digest, syncOptionsBinding(p.Options)})
	h := sha256.Sum256(data)
	return ApprovalPlan{Owner: req.Owner, Host: req.Host, Operation: req.Operation, TargetDigest: p.Target, RequestDigest: hex.EncodeToString(h[:]), PolicyDigest: decision.Digest}, nil
}
func (s *Service) ExecuteSync(ctx context.Context, req Request, approved ApprovalPlan) (*client.SyncResult, *MutationIntent, error) {
	p, err := s.preparedSync(req)
	if err != nil {
		return nil, nil, err
	}
	if proto.ValidateOperationID(req.OperationID) != nil {
		return nil, nil, errors.New("sync operation identity required")
	}
	s.syncMu.Lock()
	if p.Taken || time.Now().After(p.Expires) {
		s.syncMu.Unlock()
		return nil, nil, errors.New("sync plan already consumed or expired")
	}
	p.Taken = true
	s.syncMu.Unlock()
	defer func() {
		s.syncMu.Lock()
		if s.syncPlans[p.Owner+"\x00"+p.StageID] == p {
			delete(s.syncPlans, p.Owner+"\x00"+p.StageID)
			p.Release()
		}
		s.syncMu.Unlock()
		cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.syncStore.Remove(cleanup, p.Owner, p.StageID)
	}()
	intent := MutationIntent{SyncDigest: p.Plan.Digest, OperationID: req.OperationID, Owner: req.Owner.Key(), Host: req.Host, Operation: req.Operation, RequestDigest: approved.RequestDigest, TargetDigest: approved.TargetDigest, PolicyDigest: approved.PolicyDigest, ApprovalID: approved.ApprovalID}
	if err := s.Mutations.Prepare(intent); err != nil {
		old, _ := s.Mutations.Get(intent.Owner, intent.OperationID)
		return nil, &old, err
	}
	status := func() *MutationIntent {
		m, e := s.Mutations.Get(intent.Owner, intent.OperationID)
		if e != nil {
			return &intent
		}
		return &m
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	release, err := s.Ingress.Hold(req.Owner.Key(), 8<<20)
	if err != nil {
		_ = s.Mutations.Transition(intent.Owner, intent.OperationID, "not_sent", false)
		return nil, status(), err
	}
	defer release()
	if p.Options.Direction == "push" {
		_, err = s.syncRPC(ctx, req, p.Target, proto.OpSyncStage, "", &proto.SyncParams{Action: "begin", ID: p.StageID, Manifest: &p.Plan.Source})
		if err == nil {
			for i, e := range p.Plan.Source.Entries {
				if e.Kind != "file" {
					continue
				}
				for offset := int64(0); offset < e.Size; {
					data, readErr := s.syncStore.Read(ctx, intent.Owner, p.StageID, i, offset)
					if readErr != nil {
						err = readErr
						break
					}
					_, err = s.syncRPC(ctx, req, p.Target, proto.OpSyncStage, "", &proto.SyncParams{Action: "put", ID: p.StageID, Index: i, Offset: offset, Data: data})
					if err != nil {
						break
					}
					if len(data) == 0 {
						err = synctree.ErrStage
						break
					}
					offset += int64(len(data))
				}
				if err != nil {
					break
				}
			}
		}
		if err == nil {
			_, err = s.syncRPC(ctx, req, p.Target, proto.OpSyncStage, "", &proto.SyncParams{Action: "seal", ID: p.StageID})
		}
	}
	if err == nil {
		err = s.Mutations.Transition(intent.Owner, intent.OperationID, "dispatched", false)
	}
	var executionOutcome *synctree.Outcome
	if err == nil {
		if p.Options.Direction == "push" {
			var result *proto.SyncResult
			result, err = s.syncRPC(ctx, req, p.Target, proto.OpSyncCommit, req.OperationID, &proto.SyncParams{ID: p.StageID, Path: p.Destination, Plan: &p.Plan})
			if result != nil {
				executionOutcome = result.Outcome
			}
		} else {
			err = s.syncLocal(ctx, req, func(work context.Context) error {
				outcome, e := s.syncStore.Execute(work, intent.Owner, p.StageID, p.Destination, intent.OperationID, p.Plan)
				executionOutcome = &outcome
				return e
			})
		}
	}
	if executionOutcome == nil && err == nil {
		err = errors.New("sync outcome missing")
	}
	if executionOutcome != nil && (executionOutcome.OperationID != intent.OperationID || executionOutcome.Digest != p.Plan.Digest) {
		executionOutcome = nil
		err = errors.New("sync outcome identity mismatch")
	}
	if err != nil {
		state := "ambiguous"
		if status().State == "prepared" {
			state = "not_sent"
		} else if executionOutcome != nil && (executionOutcome.State == "failed" || executionOutcome.State == "completed") {
			state = "completed"
		}
		_ = s.Mutations.Transition(intent.Owner, intent.OperationID, state, executionOutcome != nil && executionOutcome.State == "completed")
		return nil, status(), err
	}
	if executionOutcome.State != "completed" {
		_ = s.Mutations.Transition(intent.Owner, intent.OperationID, "ambiguous", false)
		return nil, status(), errors.New("sync outcome unresolved")
	}
	if err := s.Mutations.Transition(intent.Owner, intent.OperationID, "completed", true); err != nil {
		return nil, status(), ErrMutationStorage
	}
	result := s.syncSummary(p)
	result.DryRun = false
	result.OperationID = req.OperationID
	s.syncMu.Lock()
	if s.syncPlans[intent.Owner+"\x00"+p.StageID] == p {
		delete(s.syncPlans, intent.Owner+"\x00"+p.StageID)
		p.Release()
	}
	s.syncMu.Unlock()
	// The durable outcome stays separate from ephemeral source staging.
	_ = s.syncStore.Remove(ctx, intent.Owner, p.StageID)
	if p.Options.Direction == "push" {
		_, _ = s.syncRPC(ctx, req, p.Target, proto.OpSyncStage, "", &proto.SyncParams{Action: "remove", ID: p.StageID})
	}
	return result, status(), nil
}

// ResolveSyncMutation only reads the already recorded operation. Neither a
// missing outcome nor an ambiguous one permits re-execution after reconnect.
func (s *Service) ResolveSyncMutation(ctx context.Context, owner Owner, m MutationIntent) (MutationIntent, error) {
	if m.Owner != owner.Key() || !isSyncOperation(m.Operation) {
		return m, errors.New("sync mutation owner mismatch")
	}
	if m.State != "ambiguous" {
		return m, nil
	}
	target, err := s.client.ProtocolTargetIdentity(m.Host)
	if err != nil || target != m.TargetDigest {
		return m, errors.New("sync recovery target changed")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var outcome *synctree.Outcome
	if m.Operation == "sync.push" {
		result, err := s.syncRPC(ctx, Request{Owner: owner, Host: m.Host}, target, proto.OpSyncStage, "", &proto.SyncParams{Action: "outcome", OutcomeID: m.OperationID})
		if err != nil || result == nil {
			return m, nil
		}
		outcome = result.Outcome
	} else {
		got, err := s.syncStore.Outcome(ctx, owner.Key(), m.OperationID)
		if err != nil {
			return m, nil
		}
		outcome = &got
	}
	if outcome == nil || outcome.OperationID != m.OperationID || outcome.Digest != m.SyncDigest {
		return m, errors.New("sync recovery identity mismatch")
	}
	if outcome.State == "completed" || outcome.State == "failed" {
		if err := s.Mutations.Transition(owner.Key(), m.OperationID, "completed", outcome.State == "completed"); err != nil {
			current, getErr := s.Mutations.Get(owner.Key(), m.OperationID)
			if getErr != nil || current.State != "completed" || !sameMutationBinding(current, m) {
				return m, err
			}
		}
		return s.Mutations.Get(owner.Key(), m.OperationID)
	}
	return m, nil
}
