---
name: rdev
description: Use rdev to safely operate remote development hosts from an AI coding agent. Applies when a task needs remote shell commands, files, jobs, synchronization, or shared broker orchestration through rdev.
---

# rdev

Use this skill when the user asks you to work on a remote development machine through `rdev`, its MCP server, or a shared `rdevd` broker.

## Operating rules

- Treat the user's requested host, remote working directory, and operation as authoritative. Never invent a host, path, credential, approval token, or policy setting.
- Prefer the rdev MCP tools when the client exposes them. Otherwise use the `rdev` CLI with equivalent operations.
- Keep command arguments structured. For CLI commands, pass the command after `--`; quote values that contain spaces, quotes, `$`, `~`, or shell metacharacters. For MCP, pass an argv array and do not add an unnecessary shell layer.
- Before the first remote operation, verify availability with `rdev version` (or the client equivalent), inspect the registered hosts, and establish the required host trust and project approval. Then run `rdev ping <host>`.
- If no usable host is configured, ask the user for an SSH alias or `user@host`, port when non-default, and remote working directory. Do not proceed by guessing.
- Choose the narrowest operation that fits the task: foreground exec for short commands, a bounded background job for long-running work, read/write/list for individual files, and sync for directory changes.
- For source changes, prefer the agent-oriented edit transaction below over shell heredocs or whole-file rewrites. Use `rdev_write` for binary data, new files, or an intentional complete replacement.
- Use explicit timeouts. A foreground exec defaults to 60 seconds; job observation defaults to 300 seconds; a new job's wall timeout defaults to 3600 seconds. A timeout while waiting does not cancel a job.
- For file or directory changes, preview synchronization before applying it when the mode supports previews. Preserve the user's requested direction (`push` or `pull`) and exclusions.
- In shared/broker mode, use only operations advertised by the current server. Fleet operations require a plan, digest-bound approval, and execution; never fall back to a local SSH loop when the broker rejects Fleet.
- Treat permission, trust, approval, compatibility, and capability errors as actionable state. Explain the error and the smallest next step instead of retrying blindly.
- Never expose secrets in command output or logs. Use the rdev secrets mechanism when credentials are needed, and keep credentials out of argv, environment values, and files unless the user explicitly requests that handling.
- After mutations, verify the resulting state with a focused read, status, job result, or sync preview. Report the host, operation, result, and any unresolved ambiguity.

## Agent file editing

Use `rdev_edit` as a read-compare-edit transaction. It is designed for an agent to make a small, reviewable change without losing concurrent work:

1. Read the complete text snapshot with `rdev_read` and `include_digest=true`. Set a `limit` large enough for the whole file and confirm `eof=true`; the returned digest covers the whole file, not only a truncated response.
2. Send one `rdev_edit` request with that digest as `base_digest`. Use `kind=patch` for normal code changes, `kind=lines` when the user gives explicit line ranges, and `kind=replace` when deliberately rebuilding the complete file. Do not mix edit kinds in one request.
3. For `lines`, line numbers are one-based and ranges are inclusive. `end_line=0` inserts before `start_line`; a start line after the final line appends. `expected`, when supplied, must match the selected original text exactly. Replacements are literal and line endings are not normalized.
4. For `patch`, use a strict unified diff (or a bare hunk beginning with `@@`). Context and old-line counts must match the snapshot exactly. Fuzzy matching, overlapping edits, unrelated file headers, binary data, and ambiguous hunks are rejected rather than guessed.
5. If the edit reports `edit.conflict`, `edit.mismatch`, or `edit.overlap`, reread the file and regenerate the edit from the new snapshot. Never replay the old request blindly. If transport fails after the server may have applied the edit, reread and compare the result before retrying.
6. Treat the successful `new_digest` as the next base for a follow-up edit, then run a focused read or test. For binary or non-UTF-8 files, use `rdev_write`/`rdev_sync` instead of `rdev_edit`.

The current `rdev_edit` surface is exposed through MCP. The CLI has no equivalent edit subcommand; when only the CLI is available, use the documented `rdev_write` flow and verify the result carefully.

## Selecting an interface

When an MCP server is available, use the corresponding structured operation:

- `rdev_exec` for a foreground argv command.
- `rdev_job_start`, `rdev_job_wait`, `rdev_job_status`, and `rdev_job_logs` for bounded background work.
- `rdev_read`, `rdev_write`, `rdev_edit`, and `rdev_list` for files and directories. For source edits, call `rdev_read` with `include_digest=true`, then use `rdev_edit` with that `base_digest`; prefer `patch` for normal code changes, `lines` for explicit one-based line ranges, and `replace` for a complete rewrite. On conflict or hunk mismatch, reread and regenerate instead of retrying the old edit.
- `rdev_sync` for push/pull operations; use preview and approval in broker mode.
- `rdev_session`, `rdev_ping`, `rdev_capability`, `rdev_agent_plan`, and `rdev_support` for setup and support checks.
- `rdev_fleet` only for broker-managed multi-host job plans.

When only the CLI is available, consult the repository README for the exact command syntax and use the same decision rules. Do not assume that a CLI command or MCP tool exists in both standalone and broker modes; check the active support matrix when the mode is unclear.

## Repository references

Read only the reference needed for the current operation:

- [CLI and setup reference](references/cli-and-setup.md) for installation, setup, CLI syntax, and MCP boundaries.
- [Broker operations](references/broker-operations.md) for shared mode, principals, policy, approvals, Fleet, and recovery.
- [Compatibility contract](references/compatibility.md) when protocol, release, upgrade, or compatibility behavior is involved.
