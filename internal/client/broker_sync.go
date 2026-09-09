package client

import (
	"context"
	"errors"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/synctree"
	"io"
	"os"
	"path/filepath"
	"time"
)

// SyncProtocol transfers raw staged bytes only inside the broker. The caller
// authorizes owner, target and action before scheduling; none of these payloads
// may be forwarded to frontend/audit responses without their usual redaction.
func (c *Client) SyncProtocol(ctx context.Context, host, target, clientID, projectID, op, operationID string, params *proto.SyncParams) (*proto.SyncResult, error) {
	if target == "" || clientID == "" || projectID == "" || params == nil {
		return nil, errors.New("sync request binding missing")
	}
	if op != proto.OpSyncInspect && op != proto.OpSyncStage && op != proto.OpSyncCommit {
		return nil, errors.New("invalid sync operation")
	}
	if operationID != "" && proto.ValidateOperationID(operationID) != nil {
		return nil, proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
	}
	response, _, err := c.doBuiltForLane(ctx, host, target, true, func(operationIdentity) (*builtRequest, error) {
		return &builtRequest{rawSync: true, CallerID: proto.PrincipalID(clientID, projectID), StableOperationID: operationID, Request: &proto.Request{Op: op, ProjectID: projectID, Sync: params}}, nil
	})
	if response != nil {
		return response.Sync, err
	}
	if err == nil {
		err = errors.New("remote sync operation returned no result")
	}
	return nil, err
}

// FilterSyncStage runs rsync only against retained local content. The execution
// path later applies its fixed plan; it never re-runs rsync deletion discovery.
func (c *Client) FilterSyncStage(ctx context.Context, dir string, opts SyncOptions, sourceDirectory bool, sourceName string, prefix bool, renameFile string, rootEntry *synctree.Entry) error {
	if !filepath.IsLocal(sourceName) || sourceName == "." || filepath.Base(sourceName) != sourceName {
		return synctree.ErrStage
	}
	if renameFile != "" && (!filepath.IsLocal(renameFile) || renameFile == "." || filepath.Base(renameFile) != renameFile) {
		return synctree.ErrStage
	}
	input := filepath.Join(dir, "input")
	output := filepath.Join(dir, "output")
	if err := os.Mkdir(input, 0700); err != nil {
		return err
	}
	if err := os.Mkdir(output, 0700); err != nil {
		return err
	}
	source := filepath.Join(dir, "data")
	if sourceDirectory {
		named := filepath.Join(input, sourceName)
		if err := os.Rename(source, named); err != nil {
			return err
		}
		source = named
		if !prefix {
			source += string(os.PathSeparator)
		}
	} else {
		source = filepath.Join(source, sourceName)
	}
	args := []string{"-a"}
	for _, ex := range opts.Exclude {
		args = append(args, "--exclude", ex)
	}
	args = append(args, "--", source, output+string(os.PathSeparator))
	if err := runRsync(ctx, args, io.Discard, io.Discard); err != nil {
		return err
	}
	if !sourceDirectory && renameFile != "" && renameFile != sourceName {
		if _, err := os.Lstat(filepath.Join(output, sourceName)); err == nil {
			if err := os.Rename(filepath.Join(output, sourceName), filepath.Join(output, renameFile)); err != nil {
				return err
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if err := synctree.RemoveCaptured(filepath.Join(dir, "data")); err != nil {
		return err
	}
	if err := os.Rename(output, filepath.Join(dir, "data")); err != nil {
		return err
	}
	if err := synctree.RemoveCaptured(input); err != nil {
		return err
	}
	if rootEntry != nil {
		if err := os.Chmod(filepath.Join(dir, "data"), os.FileMode(rootEntry.Mode)); err != nil {
			return err
		}
		stamp := time.Unix(0, rootEntry.ModifiedNS)
		if err := os.Chtimes(filepath.Join(dir, "data"), stamp, stamp); err != nil {
			return err
		}
	}
	return nil
}

// SyncDeletionPaths lets the installed rsync evaluate its own filter and
// directory-prefix semantics against private, content-free skeletons. The
// resulting removed path set is retained in the approval plan; execution never
// invokes rsync or discovers a fresh deletion set.
func (c *Client) SyncDeletionPaths(ctx context.Context, dir string, source synctree.Stage, dest synctree.Snapshot, opts SyncOptions, prefix bool) (map[string]bool, error) {
	removed := map[string]bool{}
	if !filepath.IsLocal(source.SourceName) || source.SourceName == "." || filepath.Base(source.SourceName) != source.SourceName {
		return nil, synctree.ErrStage
	}
	if !opts.Delete || !source.SourceDirectory || !dest.Exists {
		return removed, nil
	}
	if err := synctree.ValidateManifest(source.Manifest); err != nil {
		return nil, err
	}
	if err := synctree.ValidateManifest(dest.Manifest); err != nil {
		return nil, err
	}
	scratch, err := os.MkdirTemp(dir, "deletions-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(scratch)
	input := filepath.Join(scratch, "input")
	if err := os.Mkdir(input, 0700); err != nil {
		return nil, err
	}
	input = filepath.Join(input, source.SourceName)
	output := filepath.Join(scratch, "output")
	skeleton := func(path string, m synctree.Manifest) error {
		if err := os.Mkdir(path, 0700); err != nil {
			return err
		}
		root, err := os.OpenRoot(path)
		if err != nil {
			return err
		}
		defer root.Close()
		for _, e := range m.Entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			if e.Path == "" {
				continue
			}
			if e.Kind == "directory" {
				err = root.Mkdir(e.Path, 0700)
			} else {
				var f *os.File
				f, err = root.OpenFile(e.Path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
				if err == nil {
					err = f.Close()
				}
			}
			if err != nil {
				return err
			}
		}
		return nil
	}
	if err := skeleton(input, source.Manifest); err != nil {
		return nil, err
	}
	if err := skeleton(output, dest.Manifest); err != nil {
		return nil, err
	}
	if !prefix {
		input += string(os.PathSeparator)
	}
	args := []string{"-r", "--delete"}
	for _, ex := range opts.Exclude {
		args = append(args, "--exclude", ex)
	}
	args = append(args, "--", input, output+string(os.PathSeparator))
	if err := runRsync(ctx, args, io.Discard, io.Discard); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(output)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	for _, e := range dest.Manifest.Entries {
		if e.Path == "" {
			continue
		}
		// A former directory replaced by a file has inaccessible descendants;
		// BuildScoped includes those descendants as explicit replacements.
		if _, err := root.Lstat(e.Path); errors.Is(err, os.ErrNotExist) {
			removed[e.Path] = true
		}
	}
	return removed, nil
}
