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
- Use explicit timeouts. A foreground exec defaults to 60 seconds; job observation defaults to 300 seconds; a new job's wall timeout defaults to 3600 seconds. A timeout while waiting does not cancel a job.
- For file or directory changes, preview synchronization before applying it when the mode supports previews. Preserve the user's requested direction (`push` or `pull`) and exclusions.
- In shared/broker mode, use only operations advertised by the current server. Fleet operations require a plan, digest-bound approval, and execution; never fall back to a local SSH loop when the broker rejects Fleet.
- Treat permission, trust, approval, compatibility, and capability errors as actionable state. Explain the error and the smallest next step instead of retrying blindly.
- Never expose secrets in command output or logs. Use the rdev secrets mechanism when credentials are needed, and keep credentials out of argv, environment values, and files unless the user explicitly requests that handling.
- After mutations, verify the resulting state with a focused read, status, job result, or sync preview. Report the host, operation, result, and any unresolved ambiguity.

## Selecting an interface

When an MCP server is available, use the corresponding structured operation:

- `rdev_exec` for a foreground argv command.
- `rdev_job_start`, `rdev_job_wait`, `rdev_job_status`, and `rdev_job_logs` for bounded background work.
- `rdev_read`, `rdev_write`, and `rdev_list` for files and directories.
- `rdev_sync` for push/pull operations; use preview and approval in broker mode.
- `rdev_session`, `rdev_ping`, `rdev_capability`, and `rdev_support` for setup and support checks.
- `rdev_fleet` only for broker-managed multi-host job plans.

When only the CLI is available, consult the repository README for the exact command syntax and use the same decision rules. Do not assume that a CLI command or MCP tool exists in both standalone and broker modes; check the active support matrix when the mode is unclear.

## Repository references

Read only the reference needed for the current operation:

- [README.md](../../README.md) for installation, standalone setup, CLI syntax, and the MCP tool overview.
- [Broker operations](../../docs/rdevd-operations.md) for shared mode, principals, policy, approvals, Fleet, and recovery.
- [Compatibility contract](../../docs/phase8-acceptance.md) when protocol, release, upgrade, or compatibility behavior is involved.
