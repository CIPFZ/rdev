// Command rdev proxies commands to remote development machines.
//
// It runs either as an MCP server for Claude Code (`rdev serve`) or as a plain
// CLI for humans and CI. Both paths share internal/client, so behaviour cannot
// diverge between them.
//
// Agent binaries for supported remote platforms are embedded here and uploaded
// on demand, so a new machine needs no manual setup.
package main

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/CIPFZ/rdev/internal/broker"
	"github.com/CIPFZ/rdev/internal/buildinfo"
	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/mcpsrv"
	"github.com/CIPFZ/rdev/internal/proto"
	"github.com/CIPFZ/rdev/internal/session"
	"github.com/CIPFZ/rdev/internal/support"
	"github.com/CIPFZ/rdev/internal/transport"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// agents holds prebuilt rdev-agent binaries, one per remote platform.
//
// Embedding avoids a chicken-and-egg problem: bootstrapping the agent cannot
// require a Go toolchain on either side. `make agents` regenerates these.
//
//go:embed all:agents
var agents embed.FS

func lookupAgent(goos, goarch string) (*transport.AgentBinary, error) {
	name := fmt.Sprintf("agents/rdev-agent-%s-%s", goos, goarch)
	data, err := agents.ReadFile(name)
	if err != nil {
		return nil, fmt.Errorf("embedded agent %s missing (run 'make agents')", name)
	}
	sum := sha256.Sum256(data)
	return &transport.AgentBinary{Data: data, SHA256: hex.EncodeToString(sum[:])}, nil
}

// printVersion reports this binary's identity and that of every agent inside it.
//
// The agent checksums are the point. `rdev version` used to print a hardcoded
// "rdev 0.1.0", which cannot answer the question that actually comes up when a
// remote agent misbehaves: is the agent embedded in this rdev the one built from
// this source? A build whose embedded agents are older than its own code is
// possible -- `go build ./cmd/rdev` does not rebuild cmd/rdev/agents/ -- and
// without these lines it is invisible from the outside.
//
// Twelve hex characters: enough to compare against `sha256sum` on a remote host or
// against `make check-agents` output, short enough to read off a screen.
func printVersion() {
	fmt.Printf("rdev %s\n", buildinfo.Stamp())

	names, err := agents.ReadDir("agents")
	if err != nil {
		fmt.Println("embedded agents: none (run 'make agents')")
		return
	}
	fmt.Println("embedded agents:")
	for _, e := range names {
		data, err := agents.ReadFile("agents/" + e.Name())
		if err != nil {
			continue
		}
		sum := sha256.Sum256(data)
		fmt.Printf("  %-28s %s  %d bytes\n",
			e.Name(), hex.EncodeToString(sum[:])[:12], len(data))
	}
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	if os.Args[1] == "ping" && os.Getenv("RDEV_BROKER_SOCKET") != "" {
		if err := brokerPing(context.Background(), os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	c := client.New(lookupAgent)
	// A malformed registry should not block a session: warn and continue with
	// ad-hoc destinations, which still work without any config file.
	if err := c.Hosts.Load(); err != nil {
		fmt.Fprintf(os.Stderr, "rdev: warning: %v\n", err)
	}
	defer c.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var err error
	switch os.Args[1] {
	case "serve":
		err = cmdServe(ctx, c)
	case "exec":
		err = cmdExec(ctx, c, os.Args[2:])
	case "job":
		err = cmdJob(ctx, c, os.Args[2:])
	case "read":
		err = cmdRead(ctx, c, os.Args[2:])
	case "ls":
		err = cmdList(ctx, c, os.Args[2:])
	case "write":
		err = cmdWrite(ctx, c, os.Args[2:])
	case "secrets":
		err = cmdSecrets(ctx, c, os.Args[2:])
	case "sync":
		err = cmdSync(ctx, c, os.Args[2:])
	case "state":
		err = cmdState(ctx, c, os.Args[2:])
	case "hosts":
		err = cmdHosts(ctx, c, os.Args[2:])
	case "ping":
		err = cmdPing(ctx, c, os.Args[2:])
	case "capability":
		err = cmdCapability(ctx, c, os.Args[2:])
	case "env":
		err = cmdEnv(ctx, c, os.Args[2:])
	case "version", "-version", "--version":
		printVersion()
	case "support":
		err = printJSON(c, support.Snapshot())
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "rdev: unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}

	if err != nil {
		var envelope *proto.ErrorEnvelope
		if errors.As(err, &envelope) {
			fmt.Fprintln(os.Stderr, cliErrorLine(c, envelope))
		} else {
			fmt.Fprintf(os.Stderr, "rdev: %s\n", c.Secrets.Redact(err.Error()))
		}
		os.Exit(1)
	}
}

func brokerPing(ctx context.Context, args []string) error {
	if len(args) < 1 {
		return errors.New("usage: rdev ping <host>")
	}
	owner := broker.Owner{ClientID: os.Getenv("RDEV_CLIENT_ID"), ProjectID: os.Getenv("RDEV_PROJECT_ID")}
	if err := owner.Validate(); err != nil {
		return fmt.Errorf("broker principal: %w", err)
	}
	c, err := broker.DialClient(ctx, os.Getenv("RDEV_BROKER_SOCKET"), owner)
	if err != nil {
		return err
	}
	defer c.Close()
	resp, err := c.Do(broker.Request{Owner: owner, Operation: "ping", Host: args[0], Wire: &proto.Request{Op: proto.OpPing}})
	if err != nil {
		return err
	}
	if !resp.OK {
		return errors.New(resp.Error)
	}
	return json.NewEncoder(os.Stdout).Encode(resp.Wire)
}

func cliErrorLine(c *client.Client, envelope *proto.ErrorEnvelope) string {
	return fmt.Sprintf(
		"rdev: code=%s category=%s retry=%s retryable=%t execution_state=%s operation_id=%s terminal=%t message=%s",
		envelope.Code, envelope.Category, envelope.Retry, envelope.Retryable, envelope.ExecutionState,
		envelope.OperationID, envelope.Terminal, c.Secrets.Redact(envelope.Message),
	)
}

func usage() {
	fmt.Fprint(os.Stderr, `rdev - remote dev environment proxy

USAGE
  rdev serve                              run as an MCP server (for Claude Code)
  rdev ping    <host>
  rdev capability <host> [-refresh]
  rdev env inspect <host> [-refresh]
  rdev exec    <host> [-cwd DIR] [-timeout N] -- <argv...>
  rdev job     start  <host> [-cwd DIR] [-label L] -- <argv...>
  rdev job     list   <host> [-limit N]
  rdev job     status <host> <job-id>
  rdev job     logs   <host> <job-id> [-stream stdout|stderr] [-tail N] [-grep S]
  rdev job     wait   <host> <job-id>... [-any] [-timeout N] [-tail N]
  rdev job     stop   <host> <job-id> [-signal TERM|KILL] [-grace N]
  rdev job     rm     <host> [<job-id>] [-older-than SEC] [-keep-last N]
  rdev read    <host> <path> [-limit N]
  rdev ls      <host> [<path>] [-limit N]
  rdev write   <host> <path> [-mode 644]        (content from stdin)
  rdev sync    <host> push|pull <local> <remote> [-exclude P]... [-dry-run] [-delete]
  rdev state   inspect|migrate|repair <host> [-dry-run]
  rdev hosts   [list|trust|approve-project <sha256>|add <name> <addr> [-port N] [-cwd DIR] [-remote-dir D]
                                       [-env K=V]... [-secret NAME=PATH]...
                                       [-no-login] [-force-agent-upload]
                                       [-global] [-save]]
  rdev secrets set-from-file <name> <path> [-host H]
  rdev secrets check <host> <name> [-path P] -- <argv...>
  rdev secrets list
  rdev version                            build id + every embedded agent's SHA-256
  rdev support                            machine-readable support tiers and non-goals

HOST
  A registered alias, or an ssh destination like user@1.2.3.4:2222.
  Aliases are read from ./.rdev/hosts.json (this directory only) and then
  ~/.rdev/hosts.json (everywhere); the project file wins on name collisions.
  "hosts add" defaults to project scope; pass -global for cross-project use.

NOTES
  Everything after -- is passed as a literal argv array; no shell parses it,
  so quotes, $(...), and spaces are safe without escaping.

  "secrets" has no "set": the store is in-memory and per-process, so a value
  registered by a CLI command is gone when it exits. The MCP rdev_secrets tool
  does offer it, because "rdev serve" stays alive to use the value.
`)
}

func cmdState(ctx context.Context, c *client.Client, args []string) error {
	if len(args) < 2 {
		return errors.New("usage: rdev state inspect|migrate|repair <host> [-dry-run]")
	}
	sub, host := args[0], args[1]
	fs, err := parseFlags(args[2:], map[string]bool{"dry-run": true}, nil)
	if err != nil {
		return err
	}
	dry := fs.bools["dry-run"]
	var out any
	switch sub {
	case "inspect":
		out, err = c.StateInspect(ctx, host)
	case "migrate":
		out, err = c.StateMigrate(ctx, host, dry)
	case "repair":
		out, err = c.StateRepair(ctx, host, dry)
	default:
		return fmt.Errorf("unknown state subcommand %q", sub)
	}
	if err != nil {
		return err
	}
	return printJSON(c, out)
}

// splitArgv separates flags from the argv that follows "--".
//
// The separator is required for command-running subcommands: it is what makes
// "no shell parses your command" true all the way from the CLI.
func splitArgv(args []string) (flags []string, argv []string, err error) {
	for i, a := range args {
		if a == "--" {
			return args[:i], args[i+1:], nil
		}
	}
	return args, nil, errors.New("missing -- separator before the command")
}

// flagSet is a tiny flag parser for "-key value" and "-bool" forms.
//
// Hand-rolled rather than using package flag because each subcommand mixes
// positionals and flags in an order flag.Parse rejects.
type flagSet struct {
	vals   map[string]string
	bools  map[string]bool
	repeat map[string][]string
	pos    []string
}

func parseFlags(args []string, boolFlags map[string]bool, repeatFlags map[string]bool) (*flagSet, error) {
	fs := &flagSet{
		vals:   map[string]string{},
		bools:  map[string]bool{},
		repeat: map[string][]string{},
	}
	for i := 0; i < len(args); i++ {
		a := args[i]
		if !strings.HasPrefix(a, "-") {
			fs.pos = append(fs.pos, a)
			continue
		}
		key := strings.TrimLeft(a, "-")
		if boolFlags[key] {
			fs.bools[key] = true
			continue
		}
		if i+1 >= len(args) {
			return nil, fmt.Errorf("flag -%s needs a value", key)
		}
		i++
		if repeatFlags[key] {
			fs.repeat[key] = append(fs.repeat[key], args[i])
		} else {
			fs.vals[key] = args[i]
		}
	}
	return fs, nil
}

func (f *flagSet) str(k string) string { return f.vals[k] }
func (f *flagSet) num(k string) int {
	n, _ := strconv.Atoi(f.vals[k])
	return n
}

// ---------- serve ----------

func cmdServe(ctx context.Context, c *client.Client) error {
	srv := mcpsrv.New(c)
	// Stdio is the transport Claude Code uses. Anything written to stdout would
	// corrupt the JSON-RPC stream, so all diagnostics go to stderr.
	return srv.Run(ctx, &mcp.StdioTransport{})
}

// ---------- exec ----------

func cmdExec(ctx context.Context, c *client.Client, args []string) error {
	flagArgs, argv, err := splitArgv(args)
	if err != nil {
		return err
	}
	fs, err := parseFlags(flagArgs, map[string]bool{"no-login": true}, nil)
	if err != nil {
		return err
	}
	if len(fs.pos) < 1 {
		return errors.New("usage: rdev exec <host> [-cwd DIR] -- <argv...>")
	}
	if len(argv) == 0 {
		return errors.New("no command given after --")
	}

	opts := client.ExecOptions{
		Host:       fs.pos[0],
		Argv:       argv,
		Cwd:        fs.str("cwd"),
		TimeoutSec: fs.num("timeout"),
	}
	if fs.bools["no-login"] {
		no := false
		opts.LoginShell = &no
	}

	res, err := c.Exec(ctx, opts)
	if err != nil {
		return err
	}
	// Mirror the remote streams onto ours so rdev composes with local pipes.
	stdout := []byte(res.Stdout)
	stderr := []byte(res.Stderr)
	if res.StdoutB64 {
		stdout, err = base64.StdEncoding.DecodeString(res.Stdout)
		if err != nil {
			return proto.NewError(proto.CodeInvalidFrame, res.OperationID, proto.StateCompleted)
		}
	}
	if res.StderrB64 {
		stderr, err = base64.StdEncoding.DecodeString(res.Stderr)
		if err != nil {
			return proto.NewError(proto.CodeInvalidFrame, res.OperationID, proto.StateCompleted)
		}
	}
	_, _ = os.Stdout.Write(stdout)
	_, _ = os.Stderr.Write(stderr)
	if res.StdoutTruncation.Truncated || res.StderrTruncation.Truncated {
		fmt.Fprint(os.Stderr, execTruncationNotice(res))
	}
	if res.TimedOut {
		return fmt.Errorf("timed out after %ds", opts.TimeoutSec)
	}
	if res.ExitCode != 0 {
		os.Exit(res.ExitCode) // propagate so shell && / || behave as expected
	}
	return nil
}

func execTruncationNotice(res *client.ExecResult) string {
	if res == nil || (!res.StdoutTruncation.Truncated && !res.StderrTruncation.Truncated) {
		return ""
	}
	return fmt.Sprintf(
		"\nrdev: output truncated operation_id=%s terminal=%t execution_state=%s stdout_retained=%d stdout_original=%d stdout_dropped=%d stderr_retained=%d stderr_original=%d stderr_dropped=%d\n",
		res.OperationID, res.Terminal, res.Execution,
		res.StdoutTruncation.RetainedBytes, res.StdoutTruncation.OriginalBytes, res.StdoutTruncation.DroppedBytes,
		res.StderrTruncation.RetainedBytes, res.StderrTruncation.OriginalBytes, res.StderrTruncation.DroppedBytes)
}

// ---------- job ----------

func cmdJob(ctx context.Context, c *client.Client, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: rdev job start|list|status|logs|stop ...")
	}
	sub, rest := args[0], args[1:]

	switch sub {
	case "start":
		flagArgs, argv, err := splitArgv(rest)
		if err != nil {
			return err
		}
		fs, err := parseFlags(flagArgs, map[string]bool{"no-login": true}, nil)
		if err != nil {
			return err
		}
		if len(fs.pos) < 1 || len(argv) == 0 {
			return errors.New("usage: rdev job start <host> [-cwd DIR] [-label L] -- <argv...>")
		}
		opts := client.JobStartOptions{
			Host: fs.pos[0], Argv: argv, Cwd: fs.str("cwd"), Label: fs.str("label"),
		}
		resources := &proto.ResourceEnvelope{FDs: fs.num("fds"), WallTimeoutSec: fs.num("wall-timeout"), JobCount: fs.num("job-count")}
		if resources.FDs != 0 || resources.WallTimeoutSec != 0 || resources.JobCount != 0 {
			opts.Resources = resources
		}
		if fs.bools["no-login"] {
			no := false
			opts.LoginShell = &no
		}
		info, err := c.JobStart(ctx, opts)
		if err != nil {
			return err
		}
		return printJSON(c, info)

	case "list":
		fs, err := parseFlags(rest, nil, nil)
		if err != nil {
			return err
		}
		if len(fs.pos) < 1 {
			return errors.New("usage: rdev job list <host> [-limit N]")
		}
		res, err := c.JobList(ctx, fs.pos[0], fs.num("limit"))
		if err != nil {
			return err
		}
		return printJSON(c, res)

	case "status":
		if len(rest) < 2 {
			return errors.New("usage: rdev job status <host> <job-id>")
		}
		info, err := c.JobStatus(ctx, rest[0], rest[1])
		if err != nil {
			return err
		}
		return printJSON(c, info)

	case "logs":
		fs, err := parseFlags(rest, nil, nil)
		if err != nil {
			return err
		}
		if len(fs.pos) < 2 {
			return errors.New("usage: rdev job logs <host> <job-id> [-stream S] [-tail N] [-grep S]")
		}
		res, err := c.JobLogs(ctx, client.JobLogsOptions{
			Host: fs.pos[0], ID: fs.pos[1], Stream: fs.str("stream"),
			TailLines: fs.num("tail"), Grep: fs.str("grep"),
		})
		if err != nil {
			return err
		}
		fmt.Println(res.Logs)
		if res.LogsTruncation.Truncated {
			fmt.Fprintf(os.Stderr,
				"rdev: logs truncated operation_id=%s terminal=%t execution_state=%s retained=%d original=%d dropped=%d\n",
				res.OperationID, res.Terminal, res.Execution,
				res.LogsTruncation.RetainedBytes, res.LogsTruncation.OriginalBytes, res.LogsTruncation.DroppedBytes)
		}
		return nil

	case "stop":
		fs, err := parseFlags(rest, nil, nil)
		if err != nil {
			return err
		}
		if len(fs.pos) < 2 {
			return errors.New("usage: rdev job stop <host> <job-id> [-signal S] [-grace N]")
		}
		info, err := c.JobStop(ctx, fs.pos[0], fs.pos[1], fs.str("signal"), fs.num("grace"))
		if err != nil {
			return err
		}
		return printJSON(c, info)

	case "wait":
		fs, err := parseFlags(rest, map[string]bool{"any": true}, nil)
		if err != nil {
			return err
		}
		if len(fs.pos) < 2 {
			return errors.New("usage: rdev job wait <host> <job-id>... [-any] [-timeout N] [-tail N]")
		}
		opts := client.JobWaitOptions{
			Host:       fs.pos[0],
			WaitAny:    fs.bools["any"],
			TimeoutSec: fs.num("timeout"),
			TailOnExit: fs.num("tail"),
		}
		// Several ids share one deadline in a single call, rather than costing a
		// blocking round trip each.
		if len(fs.pos) == 2 {
			opts.ID = fs.pos[1]
		} else {
			opts.IDs = fs.pos[1:]
		}

		res, err := c.JobWait(ctx, opts)
		if err != nil {
			return err
		}

		if len(res.Waited) > 0 {
			if err := printJSON(c, res); err != nil {
				return err
			}
			if res.TimedOut {
				return fmt.Errorf("still running after %dms; wait again", res.WaitedMS)
			}
			// Exit non-zero if any job failed, so shell && / || works off a batch.
			for _, w := range res.Waited {
				if w.Err != "" {
					return fmt.Errorf("job %s: %s", w.ID, w.Err)
				}
				if w.Info != nil && w.Info.ExitCode != 0 {
					os.Exit(w.Info.ExitCode)
				}
			}
			return nil
		}

		if res.Logs != "" {
			fmt.Fprintln(os.Stderr, res.Logs)
		}
		if res.LogsTruncation.Truncated {
			fmt.Fprintf(os.Stderr,
				"rdev: wait logs truncated operation_id=%s terminal=%t execution_state=%s retained=%d original=%d dropped=%d\n",
				res.OperationID, res.Terminal, res.Execution,
				res.LogsTruncation.RetainedBytes, res.LogsTruncation.OriginalBytes, res.LogsTruncation.DroppedBytes)
		}
		if err := printJSON(c, res.Info); err != nil {
			return err
		}
		if res.TimedOut {
			return fmt.Errorf("still running after %dms; wait again", res.WaitedMS)
		}
		// Mirror the job's exit code so shell && / || work off a remote job.
		if res.Info.ExitCode != 0 {
			os.Exit(res.Info.ExitCode)
		}
		return nil

	case "rm":
		fs, err := parseFlags(rest, nil, nil)
		if err != nil {
			return err
		}
		if len(fs.pos) < 1 {
			return errors.New("usage: rdev job rm <host> [<job-id>] [-older-than SEC] [-keep-last N]")
		}
		opts := client.JobRmOptions{
			Host:         fs.pos[0],
			OlderThanSec: fs.num("older-than"),
			KeepLast:     fs.num("keep-last"),
		}
		if len(fs.pos) > 1 {
			opts.ID = fs.pos[1]
		}
		res, err := c.JobRm(ctx, opts)
		if err != nil {
			return err
		}
		return printJSON(c, res)
	}
	return fmt.Errorf("unknown job subcommand %q", sub)
}

// ---------- files ----------

func cmdList(ctx context.Context, c *client.Client, args []string) error {
	fs, err := parseFlags(args, nil, nil)
	if err != nil {
		return err
	}
	if len(fs.pos) < 1 {
		return errors.New("usage: rdev ls <host> [<path>] [-limit N]")
	}
	path := "."
	if len(fs.pos) > 1 {
		path = fs.pos[1]
	}
	res, err := c.List(ctx, fs.pos[0], path, fs.num("limit"))
	if err != nil {
		return err
	}
	return printJSON(c, res)
}

func cmdRead(ctx context.Context, c *client.Client, args []string) error {
	fs, err := parseFlags(args, nil, nil)
	if err != nil {
		return err
	}
	if len(fs.pos) < 2 {
		return errors.New("usage: rdev read <host> <path> [-limit N]")
	}
	res, err := c.ReadFile(ctx, fs.pos[0], fs.pos[1], int64(fs.num("offset")), int64(fs.num("limit")))
	if err != nil {
		return err
	}
	if res.ContentB64 {
		return errors.New("remote file contains binary data; use rdev sync to fetch it")
	}
	fmt.Print(res.Content)
	if res.Truncation.Truncated {
		fmt.Fprintf(os.Stderr,
			"\nrdev: read truncated operation_id=%s terminal=%t execution_state=%s retained=%d original=%d dropped=%d\n",
			res.OperationID, res.Terminal, res.Execution,
			res.Truncation.RetainedBytes, res.Truncation.OriginalBytes, res.Truncation.DroppedBytes)
	}
	return nil
}

func cmdWrite(ctx context.Context, c *client.Client, args []string) error {
	fs, err := parseFlags(args, map[string]bool{"append": true}, nil)
	if err != nil {
		return err
	}
	if len(fs.pos) < 2 {
		return errors.New("usage: rdev write <host> <path> [-mode 644] < content")
	}
	// Content comes from stdin so the shell never has to quote a file body.
	body, err := readAllStdin()
	if err != nil {
		return err
	}
	var mode uint32
	if m := fs.str("mode"); m != "" {
		parsed, err := strconv.ParseUint(m, 8, 32)
		if err != nil {
			return fmt.Errorf("invalid -mode %q (expected octal like 644)", m)
		}
		mode = uint32(parsed)
	}
	res, err := c.WriteFile(ctx, client.WriteFileOptions{
		Host: fs.pos[0], Path: fs.pos[1], Content: body, Mode: mode, Append: fs.bools["append"],
	})
	if err != nil {
		return err
	}
	return printJSON(c, res)
}

func readAllStdin() (string, error) {
	var sb strings.Builder
	buf := make([]byte, 32<<10)
	for {
		n, err := os.Stdin.Read(buf)
		if int64(sb.Len())+int64(n) > proto.AbsoluteRequestFrameBytes {
			return "", proto.NewError(proto.CodeLimitExceeded, "", proto.StateNotSent)
		}
		sb.Write(buf[:n])
		if err != nil {
			if err.Error() == "EOF" || n == 0 {
				return sb.String(), nil
			}
			return sb.String(), nil
		}
	}
}

// ---------- sync ----------

func cmdSync(ctx context.Context, c *client.Client, args []string) error {
	fs, err := parseFlags(args,
		map[string]bool{"dry-run": true, "delete": true, "confirm-delete": true},
		map[string]bool{"exclude": true})
	if err != nil {
		return err
	}
	if len(fs.pos) < 4 {
		return errors.New("usage: rdev sync <host> push|pull <local> <remote> [-exclude P]... [-dry-run] [-delete] [-confirm-delete] [-symlink-policy preserve|follow|skip] [-conflict-policy overwrite|skip|fail] [-max-output-bytes N]")
	}
	var maxOutputBytes int64
	if raw, ok := fs.vals["max-output-bytes"]; ok {
		var parseErr error
		maxOutputBytes, parseErr = strconv.ParseInt(raw, 10, 64)
		if parseErr != nil {
			return proto.NewError(proto.CodeInvalidRequest, "", proto.StateNotSent)
		}
	}
	res, err := c.Sync(ctx, client.SyncOptions{
		Host: fs.pos[0], Direction: fs.pos[1], Local: fs.pos[2], Remote: fs.pos[3],
		Exclude: fs.repeat["exclude"], DryRun: fs.bools["dry-run"], Delete: fs.bools["delete"],
		ConfirmDelete: fs.bools["confirm-delete"], SymlinkPolicy: fs.str("symlink-policy"), ConflictPolicy: fs.str("conflict-policy"),
		MaxOutputBytes: maxOutputBytes,
	})
	if err != nil {
		return err
	}
	stdout, stderr := []byte(res.Stdout), []byte(res.Stderr)
	if res.StdoutB64 {
		stdout, err = base64.StdEncoding.DecodeString(res.Stdout)
		if err != nil {
			return proto.NewError(proto.CodeInvalidFrame, "", proto.StateCompleted)
		}
	}
	if res.StderrB64 {
		stderr, err = base64.StdEncoding.DecodeString(res.Stderr)
		if err != nil {
			return proto.NewError(proto.CodeInvalidFrame, "", proto.StateCompleted)
		}
	}
	_, _ = os.Stdout.Write(stdout)
	_, _ = os.Stderr.Write(stderr)
	if res.Truncated {
		fmt.Fprint(os.Stderr, syncTruncationNotice(res))
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("rsync exited %d", res.ExitCode)
	}
	return nil
}

func syncTruncationNotice(res *client.SyncResult) string {
	if res == nil || !res.Truncated {
		return ""
	}
	return fmt.Sprintf(
		"\nrdev: sync output truncated stdout_retained=%d stdout_original=%d stdout_dropped=%d stderr_retained=%d stderr_original=%d stderr_dropped=%d\n",
		res.StdoutTruncation.RetainedBytes, res.StdoutTruncation.OriginalBytes, res.StdoutTruncation.DroppedBytes,
		res.StderrTruncation.RetainedBytes, res.StderrTruncation.OriginalBytes, res.StderrTruncation.DroppedBytes)
}

// ---------- hosts ----------

func cmdHosts(ctx context.Context, c *client.Client, args []string) error {
	if len(args) > 0 && args[0] == "trust" {
		return printJSON(c, c.Hosts.ProjectTrustStatus())
	}
	if len(args) > 0 && args[0] == "approve-project" {
		if len(args) != 2 {
			return errors.New("usage: rdev hosts approve-project <sha256>")
		}
		trust, err := c.Hosts.ApproveProject(args[1])
		out, err := projectApprovalOutput(trust, err)
		if err != nil {
			return err
		}
		return printJSON(c, out)
	}
	if len(args) == 0 || args[0] == "list" {
		type row struct {
			Name       string            `json:"name"`
			Addr       string            `json:"addr"`
			Port       int               `json:"port,omitempty"`
			RemoteDir  string            `json:"remote_dir,omitempty"`
			Cwd        string            `json:"cwd,omitempty"`
			Env        map[string]string `json:"env,omitempty"`
			LoginShell bool              `json:"login_shell"`
			Secrets    map[string]string `json:"secrets,omitempty"`
			// Surfaced because it suppresses the downgrade refusal: a host that
			// keeps flipping its agent should show why without reading the file.
			ForceAgentUpload bool   `json:"force_agent_upload,omitempty"`
			Scope            string `json:"scope"`
		}
		var rows []row
		for _, n := range c.Hosts.Names() {
			snapshot, err := c.Hosts.Inspect(n)
			if err != nil {
				continue
			}
			h, st := snapshot.Host, snapshot.State
			rows = append(rows, row{
				h.Name, h.Addr, h.Port, h.RemoteDir,
				st.Cwd, st.Env, st.LoginShell, st.Secrets,
				h.ForceAgentUpload, string(snapshot.Scope),
			})
		}
		return printJSON(c, rows)
	}

	if args[0] == "add" {
		fs, err := parseFlags(args[1:],
			map[string]bool{"save": true, "global": true, "no-login": true, "force-agent-upload": true},
			map[string]bool{"env": true, "secret": true})
		if err != nil {
			return err
		}
		if len(fs.pos) < 2 {
			return errors.New("usage: rdev hosts add <name> <addr> [-port N] [-cwd DIR] [-remote-dir D] [-env K=V]... [-secret NAME=PATH]... [-no-login] [-force-agent-upload] [-global] [-save]")
		}
		// Project scope is the default: a host registered while working in a repo
		// almost always belongs to that repo. -global opts into cross-project
		// visibility explicitly.
		scope := session.ScopeProject
		if fs.bools["global"] {
			scope = session.ScopeGlobal
		}
		port := 0
		if rawPort := fs.str("port"); rawPort != "" {
			parsed, err := strconv.Atoi(rawPort)
			if err != nil {
				return fmt.Errorf("invalid port %q", rawPort)
			}
			port = parsed
		}

		env := map[string]string{}
		for _, kv := range fs.repeat["env"] {
			k, v, ok := strings.Cut(kv, "=")
			if !ok {
				return fmt.Errorf("-env expects K=V, got %q", kv)
			}
			env[k] = v
		}
		// Only the path is recorded; the value is read over the connection each
		// session, so no credential lands in hosts.json.
		secretPaths := map[string]string{}
		for _, kv := range fs.repeat["secret"] {
			k, v, ok := strings.Cut(kv, "=")
			if !ok {
				return fmt.Errorf("-secret expects NAME=PATH, got %q", kv)
			}
			secretPaths[k] = v
		}

		cwd := fs.str("cwd")
		var cwdPatch *string
		if cwd != "" {
			cwdPatch = &cwd
		}
		var loginShell *bool
		if fs.bools["no-login"] {
			no := false
			loginShell = &no
		}
		host := transport.Host{
			Name: fs.pos[0], Addr: fs.pos[1], Port: port,
			RemoteDir:        fs.str("remote-dir"),
			ForceAgentUpload: fs.bools["force-agent-upload"],
		}
		result, err := c.Hosts.ApplyHostUpdate(session.HostUpdate{
			Name: fs.pos[0], Host: &host, Scope: scope, SetScope: true,
			Cwd: cwdPatch, Env: env, LoginShell: loginShell, Secrets: secretPaths,
			Persist: fs.bools["save"],
		})
		if err != nil {
			warning, committed := session.ConfigWriteCommittedWarning(err)
			if !committed {
				return err
			}
			fmt.Fprintf(os.Stderr, "warning: %s\n", c.Secrets.Redact(warning))
		}
		if result.SavedTo != "" {
			note := "visible in every project"
			if result.Scope == session.ScopeProject {
				note = "visible only in this directory"
			}
			fmt.Fprintf(os.Stderr, "saved to %s (%s)\n", c.Secrets.Redact(result.SavedTo), note)
		}
		return cmdHosts(ctx, c, []string{"list"})
	}
	return fmt.Errorf("unknown hosts subcommand %q", args[0])
}

type approvalOutput struct {
	session.ProjectTrust
	Warning string `json:"warning,omitempty"`
}

func projectApprovalOutput(trust session.ProjectTrust, err error) (approvalOutput, error) {
	out := approvalOutput{ProjectTrust: trust}
	if err == nil {
		return out, nil
	}
	if warning, committed := session.ConfigWriteCommittedWarning(err); committed {
		out.Warning = warning
		return out, nil
	}
	return approvalOutput{}, err
}

func cmdPing(ctx context.Context, c *client.Client, args []string) error {
	if len(args) < 1 {
		return errors.New("usage: rdev ping <host>")
	}
	res, err := c.Ping(ctx, args[0])
	if err != nil {
		return err
	}
	return printJSON(c, res)
}

func cmdCapability(ctx context.Context, c *client.Client, args []string) error {
	if len(args) < 1 {
		return errors.New("usage: rdev capability <host> [-refresh]")
	}
	fs, err := parseFlags(args[1:], map[string]bool{"refresh": true}, nil)
	if err != nil {
		return err
	}
	res, err := c.CapabilityProbe(ctx, args[0], fs.bools["refresh"])
	if err != nil {
		return err
	}
	return printJSON(c, res)
}

func cmdEnv(ctx context.Context, c *client.Client, args []string) error {
	if len(args) == 0 || args[0] != "inspect" || len(args) < 2 {
		return errors.New("usage: rdev env inspect <host> [-refresh]")
	}
	fs, err := parseFlags(args[2:], map[string]bool{"refresh": true}, nil)
	if err != nil {
		return err
	}
	res, err := c.CapabilityProbe(ctx, args[1], fs.bools["refresh"])
	if err != nil {
		return err
	}
	return printJSON(c, res)
}

// cmdSecrets manages the redaction store.
//
// The store is in-memory and per-process, so registering a secret here only
// affects this one command -- useless on its own. It exists to verify that a
// credential file resolves and that redaction covers the value, which is
// otherwise only testable through a live MCP session.
//
// There is deliberately no `secrets set <name> <value>`, even though the MCP tool
// has that action. Not for shell-history reasons, though that is a real secondary
// concern: it simply cannot work. The MCP action is useful because `rdev serve` is
// a long-lived process that goes on to use the registered value, whereas this
// command would register a secret, print its length, and exit -- taking the store
// with it. Offering it would look like the MCP capability while doing nothing, so
// the gap is documented instead of filled.
func cmdSecrets(ctx context.Context, c *client.Client, args []string) error {
	if len(args) == 0 {
		return errors.New(secretsUsage)
	}

	switch args[0] {
	case "set-from-file":
		fs, err := parseFlags(args[1:], nil, nil)
		if err != nil {
			return err
		}
		if len(fs.pos) < 2 {
			return errors.New("usage: rdev secrets set-from-file <name> <path> [-host H]")
		}
		name, path := fs.pos[0], fs.pos[1]

		if host := fs.str("host"); host != "" {
			if err := c.SetSecretFromRemoteFile(ctx, host, name, path); err != nil {
				return err
			}
			fmt.Fprintln(os.Stderr, c.Secrets.Redact(fmt.Sprintf("registered %q from %s:%s", name, host, path)))
		} else {
			if err := c.SetOutputSecretFromFile(name, path); err != nil {
				return err
			}
			fmt.Fprintln(os.Stderr, c.Secrets.Redact(fmt.Sprintf("registered %q from local:%s", name, path)))
		}
		// Never print the value. Length is enough to confirm the right file was
		// read without putting the credential in a terminal or a CI log.
		if n, ok, err := c.SecretLength(fs.str("host"), name); err != nil {
			return err
		} else if ok {
			fmt.Fprintf(os.Stderr, "value length %d\n", n)
		}
		return printJSON(c, map[string]any{"names": c.Secrets.Names(), "secrets": c.Secrets.Descriptors()})

	case "check":
		// Runs a command and reports whether redaction caught the value, which is
		// the only way to be sure the registered secret matches the remote one.
		flagArgs, argv, err := splitArgv(args[1:])
		if err != nil {
			return err
		}
		fs, err := parseFlags(flagArgs, nil, nil)
		if err != nil {
			return err
		}
		if len(fs.pos) < 2 || len(argv) == 0 {
			return errors.New("usage: rdev secrets check <host> <name> [-path P] -- <argv...>")
		}
		host, name := fs.pos[0], fs.pos[1]
		if path := fs.str("path"); path != "" {
			if err := c.SetSecretFromRemoteFile(ctx, host, name, path); err != nil {
				return err
			}
		}
		if _, ok, err := c.SecretLength(host, name); err != nil {
			return err
		} else if !ok {
			return fmt.Errorf("secret %q is not registered; pass -path to read it first", name)
		}
		res, err := c.Exec(ctx, client.ExecOptions{Host: host, Argv: argv, TimeoutSec: 60})
		if err != nil {
			return err
		}
		placeholder := "<redacted:" + name + ">"
		masked := strings.Contains(res.Stdout, placeholder) || strings.Contains(res.Stderr, placeholder)
		fmt.Print(res.Stdout)
		fmt.Fprint(os.Stderr, res.Stderr)
		return printJSON(c, map[string]any{"redacted": masked, "exit_code": res.ExitCode})

	case "list":
		return printJSON(c, map[string]any{"names": c.Secrets.Names(), "secrets": c.Secrets.Descriptors()})
	}
	// Name the valid subcommands. "unknown secrets subcommand" alone sends the
	// reader to the source, and the one people reach for -- `set` -- is absent by
	// design, so silence about it reads as a missing feature.
	if args[0] == "set" {
		return errors.New(
			"rdev has no `secrets set`: this store is in-memory and per-process, so a value " +
				"registered by one CLI command is gone when it exits.\n" +
				"To check that a credential file resolves and that redaction covers it:\n" +
				"  rdev secrets set-from-file <name> <path> [-host H]\n" +
				"  rdev secrets check <host> <name> [-path P] -- <argv...>\n" +
				"The MCP rdev_secrets tool does offer action=set, because `rdev serve` is a " +
				"long-lived process that goes on to use the value.")
	}
	return fmt.Errorf("unknown secrets subcommand %q\n%s", args[0], secretsUsage)
}

// secretsUsage lists what `rdev secrets` actually accepts. Shared by the empty-args
// and unknown-subcommand paths so the two cannot drift apart.
const secretsUsage = `usage:
  rdev secrets set-from-file <name> <path> [-host H]   register from a file and report its length
  rdev secrets check <host> <name> [-path P] -- <argv...>   run a command and report whether redaction caught the value
  rdev secrets list                                    names currently registered in this process`

func printJSON(c *client.Client, v any) error {
	b, err := json.MarshalIndent(c.Secrets.RedactValue(v), "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}
