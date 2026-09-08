package broker

type Lane string

const (
	LaneControl Lane = "control"
	LaneExec    Lane = "exec"
	LaneBulk    Lane = "bulk"
)

// LaneForOperation is selected by the broker, never by the frontend.
func LaneForOperation(operation string) Lane {
	switch operation {
	case "exec", "job_start", "job_wait":
		return LaneExec
	case "sync.push", "sync.pull", "write", "write_file", "read_file":
		return LaneBulk
	default:
		return LaneControl
	}
}
