# rdev CLI and setup reference

Run `rdev version`, inspect `rdev hosts list`, establish host-key trust and
project approval, then run `rdev ping HOST`. Use structured argv after `--` for
exec and jobs. Use `rdev policy check` and `rdev doctor [HOST]` for read-only
diagnostics. A missing host, path, credential or approval must be requested from
the user; never invent one. Passwords and interactive trust prompts are terminal
only and are never MCP parameters.

`rdev agent status|plan HOST` is also read-only; unavailable fields are reported
as `unknown` and no upload or repair is triggered. `bootstrap-key`, `setup` and
`bootstrap-key remove` require a real interactive terminal. Never pass a password
through argv, environment, ordinary stdin, logs, jobs or MCP. exec/job inherit
standard streams and do not provide PTY, TUI or persistent-session guarantees.
