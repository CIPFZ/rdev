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

For source files, the MCP edit surface is the preferred agent workflow: call
`rdev_read` with `include_digest=true`, then call `rdev_edit` with the returned
`base_digest`. It supports strict `patch`, one-based inclusive `lines`, and
complete `replace` edits. A conflict or hunk mismatch requires a fresh read and
regenerated edit. The CLI currently has no `rdev edit` subcommand; use the
documented `rdev_write` path only when MCP is unavailable.
