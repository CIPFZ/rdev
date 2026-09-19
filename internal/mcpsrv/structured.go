package mcpsrv

import (
	"context"
	"errors"
	"regexp"
	"strconv"
	"strings"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type GitStatusIn struct {
	Host string `json:"host"`
	Cwd  string `json:"cwd,omitempty"`
}
type GitStatusResult struct {
	Branch   string   `json:"branch,omitempty"`
	Ahead    int      `json:"ahead"`
	Behind   int      `json:"behind"`
	Entries  []string `json:"entries,omitempty"`
	Clean    bool     `json:"clean"`
	ExitCode int      `json:"exit_code"`
}

type SystemdIn struct {
	Host          string `json:"host"`
	Service       string `json:"service" jsonschema:"Unit name, without shell syntax"`
	Action        string `json:"action" jsonschema:"status, logs, or restart"`
	Cwd           string `json:"cwd,omitempty"`
	Tail          int    `json:"tail,omitempty"`
	OperationID   string `json:"operation_id,omitempty"`
	ApprovalToken string `json:"approval_token,omitempty"`
}
type SystemdResult struct {
	Service     string `json:"service"`
	Action      string `json:"action"`
	ActiveState string `json:"active_state,omitempty"`
	SubState    string `json:"sub_state,omitempty"`
	MainPID     int    `json:"main_pid,omitempty"`
	LoadState   string `json:"load_state,omitempty"`
	Logs        string `json:"logs,omitempty"`
	ExitCode    int    `json:"exit_code"`
}

type PortCheckIn struct {
	Host string `json:"host"`
	Cwd  string `json:"cwd,omitempty"`
	Port int    `json:"port,omitempty" jsonschema:"Optional TCP port; zero returns all listeners"`
}
type PortListener struct {
	Local   string `json:"local"`
	Process string `json:"process,omitempty"`
}
type PortCheckResult struct {
	Available bool           `json:"available"`
	Port      int            `json:"port,omitempty"`
	Listeners []PortListener `json:"listeners,omitempty"`
	Source    string         `json:"source"`
}

var safeUnitName = regexp.MustCompile(`^[A-Za-z0-9_.@:-]+$`)

func execDirect(ctx context.Context, c *client.Client, host, cwd string, argv []string) (*proto.ExecResult, error) {
	r, err := c.Exec(ctx, client.ExecOptions{Host: host, Cwd: cwd, Argv: argv, LoginShell: boolPtr(false), TimeoutSec: 30, MaxOutputBytes: 256 << 10})
	if err != nil {
		return nil, err
	}
	return r.ExecResult, nil
}
func boolPtr(v bool) *bool { return &v }

func parseGitStatus(out *proto.ExecResult) GitStatusResult {
	r := GitStatusResult{ExitCode: out.ExitCode}
	lines := strings.Split(strings.TrimSuffix(out.Stdout, "\n"), "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "## ") {
			branch := strings.TrimPrefix(line, "## ")
			if i := strings.Index(branch, " ["); i >= 0 {
				meta := strings.TrimSuffix(branch[i+2:], "]")
				branch = branch[:i]
				for _, item := range strings.Split(meta, ", ") {
					if strings.HasPrefix(item, "ahead ") {
						r.Ahead, _ = strconv.Atoi(strings.TrimPrefix(item, "ahead "))
					}
					if strings.HasPrefix(item, "behind ") {
						r.Behind, _ = strconv.Atoi(strings.TrimPrefix(item, "behind "))
					}
				}
			}
			if i := strings.Index(branch, "..."); i >= 0 {
				branch = branch[:i]
			}
			r.Branch = branch
			continue
		}
		r.Entries = append(r.Entries, line)
	}
	r.Clean = len(r.Entries) == 0 && r.ExitCode == 0
	return r
}

func registerStructured(s *mcp.Server, c *client.Client) {
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_git_status", Description: "Read structured git status through fixed argv (git status --porcelain=v1 -b); no shell is evaluated."}, func(ctx context.Context, _ *mcp.CallToolRequest, in GitStatusIn) (*mcp.CallToolResult, GitStatusResult, error) {
		r, err := execDirect(ctx, c, in.Host, in.Cwd, []string{"git", "status", "--porcelain=v1", "-b"})
		if err != nil {
			return nil, GitStatusResult{}, err
		}
		out := parseGitStatus(r)
		return nil, out, nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_systemd", Description: "Inspect logs or restart one systemd unit using fixed argv. Service names are validated and no shell is evaluated."}, func(ctx context.Context, _ *mcp.CallToolRequest, in SystemdIn) (*mcp.CallToolResult, SystemdResult, error) {
		return nil, runSystemd(ctx, c, in), nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_port_check", Description: "Check listening TCP sockets using ss with a netstat fallback, returning structured listeners."}, func(ctx context.Context, _ *mcp.CallToolRequest, in PortCheckIn) (*mcp.CallToolResult, PortCheckResult, error) {
		if in.Port < 0 || in.Port > 65535 {
			return nil, PortCheckResult{}, proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
		}
		return nil, runPortCheck(ctx, c, in), nil
	})
}

func runSystemd(ctx context.Context, c *client.Client, in SystemdIn) SystemdResult {
	r := SystemdResult{Service: in.Service, Action: in.Action}
	if !safeUnitName.MatchString(in.Service) || (in.Action != "status" && in.Action != "logs" && in.Action != "restart") {
		r.ExitCode = 2
		return r
	}
	var argv []string
	switch in.Action {
	case "status":
		argv = []string{"systemctl", "show", in.Service, "--no-pager", "--property=ActiveState,SubState,MainPID,LoadState"}
	case "logs":
		tail := in.Tail
		if tail <= 0 || tail > 10000 {
			tail = 200
		}
		argv = []string{"journalctl", "-u", in.Service, "--no-pager", "-n", strconv.Itoa(tail), "-o", "short-iso"}
	case "restart":
		argv = []string{"systemctl", "restart", in.Service}
	}
	out, err := execDirect(ctx, c, in.Host, in.Cwd, argv)
	if err != nil {
		r.ExitCode = 1
		return r
	}
	r.ExitCode = out.ExitCode
	if in.Action == "logs" {
		r.Logs = out.Stdout
		return r
	}
	for _, line := range strings.Split(out.Stdout, "\n") {
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		switch k {
		case "ActiveState":
			r.ActiveState = v
		case "SubState":
			r.SubState = v
		case "MainPID":
			r.MainPID, _ = strconv.Atoi(v)
		case "LoadState":
			r.LoadState = v
		}
	}
	return r
}

func runPortCheck(ctx context.Context, c *client.Client, in PortCheckIn) PortCheckResult {
	r := PortCheckResult{Available: true, Source: "ss", Port: in.Port}
	out, err := execDirect(ctx, c, in.Host, in.Cwd, []string{"ss", "-H", "-ltnp"})
	if err != nil {
		out, err = execDirect(ctx, c, in.Host, in.Cwd, []string{"netstat", "-an"})
		r.Source = "netstat"
	}
	if err != nil {
		return r
	}
	for _, line := range strings.Split(out.Stdout, "\n") {
		f := strings.Fields(line)
		if len(f) < 4 {
			continue
		}
		local := f[3]
		process := ""
		if len(f) > 5 {
			process = strings.Join(f[5:], " ")
		}
		if in.Port != 0 && listenerPort(local) != in.Port {
			continue
		}
		r.Listeners = append(r.Listeners, PortListener{Local: local, Process: process})
	}
	r.Available = len(r.Listeners) == 0
	return r
}

func listenerPort(local string) int {
	i := strings.LastIndex(local, ":")
	if i < 0 {
		return 0
	}
	n, _ := strconv.Atoi(strings.Trim(local[i+1:], "[]"))
	return n
}

func registerBrokerStructured(s *mcp.Server, socket string, owner broker.Owner) {
	// The broker path keeps the same fixed argv contract and policy/audit trail.
	call := func(ctx context.Context, in ExecIn, argv []string) (*proto.ExecResult, error) {
		if in.Host == "" {
			return nil, errors.New("host required")
		}
		resp, err := callBroker(ctx, socket, owner, broker.Request{Owner: owner, Host: in.Host, Operation: proto.OpExec, Approval: in.ApprovalToken, Wire: &proto.Request{Op: proto.OpExec, OperationID: in.OperationID, ClientID: owner.ClientID, ProjectID: owner.ProjectID, Exec: &proto.ExecParams{Argv: argv, Cwd: in.Cwd, LoginShell: false, TimeoutSec: 30, MaxOutputBytes: 256 << 10}}})
		if err != nil {
			return nil, err
		}
		if resp.Wire == nil || resp.Wire.Exec == nil {
			return nil, errors.New("broker exec returned no result")
		}
		return resp.Wire.Exec, nil
	}
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_git_status", Description: "Read structured git status through the shared broker using fixed argv."}, func(ctx context.Context, _ *mcp.CallToolRequest, in GitStatusIn) (*mcp.CallToolResult, GitStatusResult, error) {
		out, err := call(ctx, ExecIn{Host: in.Host, Cwd: in.Cwd}, []string{"git", "status", "--porcelain=v1", "-b"})
		if err != nil {
			return nil, GitStatusResult{}, err
		}
		return nil, parseGitStatus(out), nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_systemd", Description: "Inspect or restart one systemd unit through the shared broker."}, func(ctx context.Context, _ *mcp.CallToolRequest, in SystemdIn) (*mcp.CallToolResult, SystemdResult, error) {
		if !safeUnitName.MatchString(in.Service) || (in.Action != "status" && in.Action != "logs" && in.Action != "restart") {
			return nil, SystemdResult{}, proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
		}
		var argv []string
		if in.Action == "status" {
			argv = []string{"systemctl", "show", in.Service, "--no-pager", "--property=ActiveState,SubState,MainPID,LoadState"}
		} else if in.Action == "logs" {
			tail := in.Tail
			if tail <= 0 {
				tail = 200
			}
			argv = []string{"journalctl", "-u", in.Service, "--no-pager", "-n", strconv.Itoa(tail), "-o", "short-iso"}
		} else {
			argv = []string{"systemctl", "restart", in.Service}
		}
		out, err := call(ctx, ExecIn{Host: in.Host, Cwd: in.Cwd, OperationID: in.OperationID, ApprovalToken: in.ApprovalToken}, argv)
		if err != nil {
			return nil, SystemdResult{}, err
		}
		r := SystemdResult{Service: in.Service, Action: in.Action, ExitCode: out.ExitCode}
		if in.Action == "logs" {
			r.Logs = out.Stdout
		} else {
			for _, line := range strings.Split(out.Stdout, "\n") {
				k, v, ok := strings.Cut(line, "=")
				if !ok {
					continue
				}
				switch k {
				case "ActiveState":
					r.ActiveState = v
				case "SubState":
					r.SubState = v
				case "MainPID":
					r.MainPID, _ = strconv.Atoi(v)
				case "LoadState":
					r.LoadState = v
				}
			}
		}
		return nil, r, nil
	})
	mcp.AddTool(s, &mcp.Tool{Name: "rdev_port_check", Description: "Check listening sockets through the shared broker."}, func(ctx context.Context, _ *mcp.CallToolRequest, in PortCheckIn) (*mcp.CallToolResult, PortCheckResult, error) {
		if in.Port < 0 || in.Port > 65535 {
			return nil, PortCheckResult{}, proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
		}
		out, err := call(ctx, ExecIn{Host: in.Host, Cwd: in.Cwd}, []string{"ss", "-H", "-ltnp"})
		source := "ss"
		if err != nil {
			out, err = call(ctx, ExecIn{Host: in.Host, Cwd: in.Cwd}, []string{"netstat", "-an"})
			source = "netstat"
		}
		if err != nil {
			return nil, PortCheckResult{}, err
		}
		r := PortCheckResult{Available: true, Source: source, Port: in.Port}
		for _, line := range strings.Split(out.Stdout, "\n") {
			f := strings.Fields(line)
			if len(f) < 4 {
				continue
			}
			p := ""
			if len(f) > 5 {
				p = strings.Join(f[5:], " ")
			}
			if in.Port != 0 && listenerPort(f[3]) != in.Port {
				continue
			}
			r.Listeners = append(r.Listeners, PortListener{Local: f[3], Process: p})
		}
		r.Available = len(r.Listeners) == 0
		return nil, r, nil
	})
}
