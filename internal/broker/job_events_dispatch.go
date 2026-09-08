package broker

import "github.com/CIPFZ/rdev/internal/proto"

type JobEventQuery struct {
	ID     string         `json:"id"`
	Cursor JobEventCursor `json:"cursor,omitempty"`
	Limit  int            `json:"limit,omitempty"`
}

// RecordJobResponse validates ownership before any observation is persisted or
// published. Shared wait calls execute this once in the observation worker,
// including when every frontend subscriber has already disconnected.
func (s *Service) RecordJobResponse(host, owner string, req *proto.Request, resp *proto.Response) error {
	if err := s.Jobs.RecordResponse(host, owner, req, resp); err != nil {
		return err
	}
	if resp == nil || !resp.OK || resp.Job == nil {
		return nil
	}
	var infos []*proto.JobInfo
	if resp.Job.Info != nil {
		infos = append(infos, resp.Job.Info)
	}
	infos = append(infos, resp.Job.List...)
	for _, waited := range resp.Job.Waited {
		if waited != nil && waited.Info != nil {
			infos = append(infos, waited.Info)
		}
	}
	ref := OperationReference(Request{Wire: &proto.Request{OperationID: resp.OperationID}})
	var events []ownedJobEvent
	for _, info := range infos {
		if info == nil {
			continue
		}
		state := info.State
		if state == "" {
			state = proto.JobUnknown
		}
		event := ownedJobEvent{Owner: owner, JobEvent: JobEvent{Host: host, JobID: info.ID, State: state, PID: info.PID, Operation: req.Op, OperationRef: ref}}
		if state == proto.JobExited {
			event.ExitCode = info.ExitCode
		}
		events = append(events, event)
	}
	if req.Op == proto.OpJobRm {
		for _, id := range append(append([]string(nil), resp.Job.Removed...), resp.Job.Missing...) {
			events = append(events, ownedJobEvent{Owner: owner, JobEvent: JobEvent{Host: host, JobID: id, State: "removed", Operation: req.Op, OperationRef: ref}})
		}
	}
	if _, err := s.Events.Record(events); err != nil {
		return err
	}
	for _, info := range infos {
		if err := s.ResolveMutationJob(host, owner, info); err != nil {
			return err
		}
	}
	return nil
}
