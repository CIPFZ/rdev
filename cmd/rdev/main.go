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
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/CIPFZ/rdev/internal/client"
	"github.com/CIPFZ/rdev/internal/mcpsrv"
	"github.com/CIPFZ/rdev/internal/session"
	"github.com/CIPFZ/rdev/internal/transport"
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

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
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
	case "hosts":
		err = cmdHosts(ctx, c, os.Args[2:])
	case "ping":
		err = cmdPing(ctx, c, os.Args[2:])
	case "version", "-version", "--version":
		fmt.Printf("rdev %s\n", mcpsrv.Version)
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "rdev: unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(2)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "rdev: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `rdev - remote dev environment proxy

USAGE
  rdev serve                              run as an MCP server (for Claude Code)
  rdev ping    <host>
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
  rdev hosts   [list|add <name> <addr> [-port N] [-cwd DIR] [-remote-dir D]
                                       [-env K=V]... [-secret NAME=PATH]...
                                       [-no-login] [-global] [-save]]
  rdev secrets set-from-file <name> <path> [-host H]
  rdev secrets check <host> <name> [-path P] -- <argv...>

HOST
  A registered alias, or an ssh destination like user@1.2.3.4:2222.
  Aliases are read from ./.rdev/hosts.json (this directory only) and then
  ~/.rdev/hosts.json (everywhere); the project file wins on name collisions.
  "hosts add" defaults to project scope; pass -global for cross-project use.

NOTES
  Everything after -- is passed as a literal argv array; no shell parses it,
  so quotes, $(...), and spaces are safe without escaping.
`)
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
	fmt.Fprint(os.Stdout, res.Stdout)
	fmt.Fprint(os.Stderr, res.Stderr)
	if res.TimedOut {
		return fmt.Errorf("timed out after %ds", opts.TimeoutSec)
	}
	if res.ExitCode != 0 {
		os.Exit(res.ExitCode) // propagate so shell && / || behave as expected
	}
	return nil
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
		if fs.bools["no-login"] {
			no := false
			opts.LoginShell = &no
		}
		info, err := c.JobStart(ctx, opts)
		if err != nil {
			return err
		}
		return printJSON(info)

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
		return printJSON(res)

	case "status":
		if len(rest) < 2 {
			return errors.New("usage: rdev job status <host> <job-id>")
		}
		info, err := c.JobStatus(ctx, rest[0], rest[1])
		if err != nil {
			return err
		}
		return printJSON(info)

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
		return printJSON(info)

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
			if err := printJSON(res); err != nil {
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
		if err := printJSON(res.Info); err != nil {
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
		return printJSON(res)
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
	return printJSON(res)
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
		return fmt.Errorf("%s is binary; use rdev sync to fetch it", fs.pos[1])
	}
	fmt.Print(res.Content)
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
	return printJSON(res)
}

func readAllStdin() (string, error) {
	var sb strings.Builder
	buf := make([]byte, 32<<10)
	for {
		n, err := os.Stdin.Read(buf)
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
		map[string]bool{"dry-run": true, "delete": true},
		map[string]bool{"exclude": true})
	if err != nil {
		return err
	}
	if len(fs.pos) < 4 {
		return errors.New("usage: rdev sync <host> push|pull <local> <remote> [-exclude P]... [-dry-run] [-delete]")
	}
	res, err := c.Sync(ctx, client.SyncOptions{
		Host: fs.pos[0], Direction: fs.pos[1], Local: fs.pos[2], Remote: fs.pos[3],
		Exclude: fs.repeat["exclude"], DryRun: fs.bools["dry-run"], Delete: fs.bools["delete"],
	})
	if err != nil {
		return err
	}
	fmt.Print(res.Stdout)
	if res.Stderr != "" {
		fmt.Fprint(os.Stderr, res.Stderr)
	}
	if res.ExitCode != 0 {
		return fmt.Errorf("rsync exited %d", res.ExitCode)
	}
	return nil
}

// ---------- hosts ----------

func cmdHosts(ctx context.Context, c *client.Client, args []string) error {
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
			Scope      string            `json:"scope"`
		}
		var rows []row
		for _, n := range c.Hosts.Names() {
			h, err := c.Hosts.Host(n)
			if err != nil {
				continue
			}
			st := c.Hosts.State(n)
			rows = append(rows, row{
				h.Name, h.Addr, h.Port, h.RemoteDir,
				st.Cwd, st.Env, st.LoginShell, st.Secrets, string(c.Hosts.ScopeOf(n)),
			})
		}
		return printJSON(rows)
	}

	if args[0] == "add" {
		fs, err := parseFlags(args[1:], map[string]bool{"save": true, "global": true, "no-login": true},
			map[string]bool{"env": true, "secret": true})
		if err != nil {
			return err
		}
		if len(fs.pos) < 2 {
			return errors.New("usage: rdev hosts add <name> <addr> [-port N] [-cwd DIR] [-remote-dir D] [-env K=V]... [-secret NAME=PATH]... [-no-login] [-global] [-save]")
		}
		// Project scope is the default: a host registered while working in a repo
		// almost always belongs to that repo. -global opts into cross-project
		// visibility explicitly.
		scope := session.ScopeProject
		if fs.bools["global"] {
			scope = session.ScopeGlobal
		}

		c.Hosts.Add(transport.Host{
			Name: fs.pos[0], Addr: fs.pos[1], Port: fs.num("port"),
			RemoteDir: fs.str("remote-dir"),
		})
		c.Hosts.SetScope(fs.pos[0], scope)

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
		if cwd != "" || len(env) > 0 || len(secretPaths) > 0 || fs.bools["no-login"] {
			c.Hosts.Update(fs.pos[0], func(st *session.State) {
				if cwd != "" {
					st.Cwd = cwd
				}
				if len(env) > 0 {
					st.Env = session.MergeEnv(st.Env, env)
				}
				if len(secretPaths) > 0 {
					st.Secrets = session.MergeEnv(st.Secrets, secretPaths)
				}
				if fs.bools["no-login"] {
					st.LoginShell = false
				}
			})
		}
		if fs.bools["save"] {
			if err := c.Hosts.Save(scope); err != nil {
				return err
			}
			var path string
			if scope == session.ScopeProject {
				path, _ = session.ProjectConfigPath()
				fmt.Fprintf(os.Stderr, "saved to %s (visible only in this directory)\n", path)
			} else {
				path, _ = session.ConfigPath()
				fmt.Fprintf(os.Stderr, "saved to %s (visible in every project)\n", path)
			}
		}
		return cmdHosts(ctx, c, []string{"list"})
	}
	return fmt.Errorf("unknown hosts subcommand %q", args[0])
}

func cmdPing(ctx context.Context, c *client.Client, args []string) error {
	if len(args) < 1 {
		return errors.New("usage: rdev ping <host>")
	}
	res, err := c.Ping(ctx, args[0])
	if err != nil {
		return err
	}
	return printJSON(res)
}

// cmdSecrets manages the redaction store.
//
// The store is in-memory and per-process, so registering a secret here only
// affects this one command -- useless on its own. It exists to verify that a
// credential file resolves and that redaction covers the value, which is
// otherwise only testable through a live MCP session.
func cmdSecrets(ctx context.Context, c *client.Client, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: rdev secrets set-from-file <name> <path> [-host H] [-check-cmd ...]")
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
			fmt.Fprintf(os.Stderr, "registered %q from %s:%s\n", name, host, path)
		} else {
			if err := c.Secrets.SetFromFile(name, path); err != nil {
				return err
			}
			fmt.Fprintf(os.Stderr, "registered %q from local:%s\n", name, path)
		}
		// Never print the value. Length is enough to confirm the right file was
		// read without putting the credential in a terminal or a CI log.
		if v, ok := c.Secrets.Get(name); ok {
			fmt.Fprintf(os.Stderr, "value length %d\n", len(v))
		}
		return printJSON(map[string]any{"names": c.Secrets.Names()})

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
		if _, ok := c.Secrets.Get(name); !ok {
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
		return printJSON(map[string]any{"redacted": masked, "exit_code": res.ExitCode})

	case "list":
		return printJSON(map[string]any{"names": c.Secrets.Names()})
	}
	return fmt.Errorf("unknown secrets subcommand %q", args[0])
}

func printJSON(v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}
