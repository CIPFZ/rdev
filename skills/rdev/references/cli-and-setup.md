# rdev CLI and setup reference

Run `rdev version`, inspect `rdev hosts list`, establish host-key trust and
project approval, then run `rdev ping HOST`. Use structured argv after `--` for
exec and jobs. Use `rdev policy check` and `rdev doctor [HOST]` for read-only
diagnostics. A missing host, path, credential or approval must be requested from
the caller; never invent one. Agents may provide bootstrap passwords only through an inherited private `-password-fd` and explicit `-confirm`; passwords and trust prompts are never MCP parameters.

`rdev agent status|plan HOST` is also read-only; unavailable fields are reported
as `unknown` and no upload or repair is triggered. `bootstrap-key`, `setup` and `bootstrap-key remove` can run agent-only with
`-password-fd FD -confirm`; without those flags a noninteractive call fails.
Never pass a password through argv, environment, ordinary stdin, logs, jobs or MCP. exec/job inherit
standard streams and do not provide PTY, TUI or persistent-session guarantees.

For source files, use the same digest-bound edit workflow through either
interface. MCP callers use `rdev_read(include_digest=true)` followed by
`rdev_edit(base_digest=...)`. CLI callers use
`rdev read HOST PATH -include-digest`, followed by
`rdev edit HOST PATH -kind patch|lines|replace|search -base-digest DIGEST [< payload]`.
Patch and replace read literal stdin; lines reads a JSON array of `LineEdit`
objects; search takes `-search TEXT -replacement TEXT` and replaces one exact
match unless `-replace-all` is set. All kinds support the same digest guard,
preview/diff (`-preview` or `rdev_edit_preview`), and optional atomic
backup/rollback (`-backup`, `rdev_edit_rollback`). A conflict or hunk mismatch
requires a fresh read and regenerated edit. After an ambiguous mutation, query `rdev_operation_status` or
`rdev mutation status HOST OPERATION_ID` before retrying.

`rdev capability HOST` returns the remote build stamp, negotiated operations,
execution profile and detected common tools. Prefer `rdev_git_status`,
`rdev_systemd` and `rdev_port_check` for structured checks; they use fixed
argv and fall back from `ss` to `netstat` for socket inspection.
