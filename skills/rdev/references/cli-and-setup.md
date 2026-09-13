# rdev CLI and setup reference

Run `rdev version`, inspect `rdev hosts list`, establish host-key trust and
project approval, then run `rdev ping HOST`. Use structured argv after `--` for
exec and jobs. Use `rdev policy check` and `rdev doctor [HOST]` for read-only
diagnostics. A missing host, path, credential or approval must be requested from
the user; never invent one. Passwords and interactive trust prompts are terminal
only and are never MCP parameters.
