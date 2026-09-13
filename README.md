# rdev — 远程开发环境代理工具

给本地 AI Agent 和开发者使用的远程执行工具。CLI 与 MCP 提供结构化命令、文件、同步和后台 job；共享模式由 `rdevd` 管理连接、权限、审批、配额和审计。

## 直接交给 Agent 的 Prompt

将下面这段 prompt 发给 Claude Code、Codex 或其他能调用 MCP 的 agent，即可让它完成安装检查、主机配置和首次连接；agent 只有在缺少 SSH 目标或需要用户确认时才应提问：

```text
请使用 rdev 帮我操作远程开发机。先检查 rdev 是否可用并运行 `rdev version`；如果未安装，请按照项目 README 的用户级安装方式安装。然后检查已注册的 host；如果没有目标，请向我索要 SSH 主机别名或 user@host、端口和工作目录，再用 `rdev hosts add` 配置。首次连接前完成 host trust 和 project approval，先执行 `rdev ping` 验证连接。之后根据我的任务选择 rdev_exec、rdev_job_start、rdev_read/write、rdev_sync 或其他合适工具。长任务使用后台 job，文件变更先用同步预览；不要猜测主机、路径、凭据或审批参数，遇到权限或兼容性问题先说明原因和可执行的下一步。
```

如果当前 agent 已加载 `rdev` MCP，则无需手工输入 CLI 命令；如果只使用终端，请继续阅读下面的快速开始。

## 快速开始

构建使用 `go.mod` 固定的 Go 1.26.8 工具链；Go 语言版本基线为 1.25。需要本地支持 SSHSIG 的 OpenSSH（8.2+）；standalone 同步和共享 rsync preview 路径还需要本地、远端的 rsync。

首次连接前须配置管理员发行策略；当前开发构建需要显式 unsigned-dev opt-in。私有 policy 示例、签名候选与旧安装迁移步骤见 [Phase8 验收与操作](docs/phase8-acceptance.md#contracts-and-operation)。共享模式使用 rdevd 的策略。

```bash
make GO="$(command -v go)" all daemon
export PATH="$PWD/bin:$PATH"
claude mcp add rdev --scope user -- "$PWD/bin/rdev" serve

# standalone：在项目目录注册主机，保存的是连接配置
rdev hosts add dev user@1.2.3.4 -port 36000 -cwd '~/myproject' -save
rdev hosts trust
rdev hosts approve-project <上一步显示的sha256>
rdev ping dev
rdev exec dev -- printf '%s\n' '中文 "quoted" $(not-run)'
```

本地源码开发也可以使用 `make dev-build`：它会在缺少时创建一个权限为 0600、七天后过期的 `dev` unsigned policy，然后按正确顺序构建嵌入 agent、CLI 和 daemon。已有的管理员 policy 不会被覆盖；正式或共享环境应改用管理员签名 policy。

首次连接自动选择并上传 agent，无需远端安装 Go。Linux/macOS bootstrap 使用 POSIX shell、`uname`、`dd`、`chmod`、`mv` 及 SHA-256 工具；实验性的 Windows amd64 远端使用 OpenSSH Server 与 Windows PowerShell 5.1。SSH host key 和认证由用户管理。Windows 操作及验收边界见 [Phase9](docs/phase9-acceptance.md)。

多人或多个 AI 客户端共享主机时，按 [rdevd 运维说明](docs/rdevd-operations.md) 配置 daemon、principal 凭证和默认拒绝的 policy。客户端设置 `RDEV_BROKER_SOCKET`、`RDEV_CLIENT_ID`、`RDEV_PROJECT_ID`、`RDEV_PRINCIPAL_TOKEN` 后，同一 CLI 或 `rdev serve` 使用共享模式。身份令牌只证明身份，不自动授予业务权限；管理员签名密钥不能交给客户端。

## 安装到本机用户环境

当前项目以源码构建为主，尚未提供 Homebrew、安装包或 `rdev update`/`rdev upgrade` 自更新命令。构建完成后，建议把实体二进制复制到用户级目录，使 Claude Code 和其他 agent 不依赖开发目录：

```bash
make GO="$(command -v go)" all daemon
install -m 755 bin/rdev "$HOME/.local/bin/rdev"
install -m 755 bin/rdevd "$HOME/.local/bin/rdevd"
export PATH="$HOME/.local/bin:$PATH"
rdev version
claude mcp add rdev --scope user -- "$HOME/.local/bin/rdev" serve
```

升级时获取新的源码或发布产物，完成校验后重复构建和 `install` 即可；远程 agent 会在客户端重新连接时按版本、能力和 release policy 协商升级。生产部署请先阅读 [Phase8 验收与操作](docs/phase8-acceptance.md) 和 [rdevd 运维说明](docs/rdevd-operations.md)。

## 适合什么场景

`rdev` 适合需要让本地 AI agent 安全、可恢复地操作一台或多台远程开发机的场景：前台命令、后台 job、文件读写、目录同步、凭据隔离和 Fleet 批量编排都通过同一 host 抽象提供。它使用 SSH 作为传输，不要求远端预装 Go；简单的一次性命令仍可直接使用 SSH。

第一次使用建议依次执行 `rdev hosts add`、`rdev hosts trust`、`rdev hosts approve-project`、`rdev ping`，再运行 `rdev exec` 或 `rdev sync`。共享 broker、审批和多主机编排属于进阶部署，不影响 standalone 单机使用。

## 两层配置：工具全局，主机按项目

standalone 的主机配置分两层：

| 位置 | 可见范围 |
|---|---|
| `~/.rdev/hosts.json` | 当前本机用户的各项目 |
| `<项目>/.rdev/hosts.json` | 以该目录启动的 CLI/MCP 进程 |

加载顺序是 global → project；project 文件只有在用户批准其绝对路径和当前 SHA-256 后才会加载，批准后的同名主机才覆盖 global。内容变化使批准失效。`hosts add` 默认 project scope；跨项目配置加 `-global`。MCP 用 `rdev_session` 查询 `project_trust`，确认后传 `approve_project_digest`。批准记录位于 `~/.rdev/trusted-projects.json`。

配置持久化使用私有目录、文件权限检查和原子替换；host 地址、用户、端口、state namespace 或 scope 改变会推进 generation，清理旧 sticky state 和该 alias 的 scoped secret，并使旧连接失效。`remote_dir` 必须是规范的 home-relative 路径，允许前缀 `~/`，拒绝绝对路径、遍历和 shell 元字符。

共享模式的 host registry 由管理员加载，业务客户端不能编辑共享 host/session。管理员修改私有 registry 后重启 `rdevd`；客户端逐请求传 `cwd`/`env`。unsupported 路由明确失败，不创建私有 SSH 客户端回退。`rdev support` 和 `rdev_support` 可提前查询这些边界及替代操作。

## Fleet：共享批量编排

Fleet 首版只支持 `job_start`，由 `rdevd` 持久执行，完成判定是 job 终态。CLI/MCP 退出或观察中断不会取消计划。没有 broker 时明确拒绝，不回退为本地 SSH 循环。其他单机 operation 不在首版 allowlist 内。

管理员先用 `rdev fleet inventory-list` 查询 revision，再执行 `rdev fleet inventory-import -revision N`，从 daemon 的可信全局 host registry 初始化或同步 inventory。相同连接/session 身份的 alias 合并为一个随机稳定 HostID。修改 labels、alias 使用完整 inventory 的 CAS 更新：`rdev fleet inventory-update -file inventory.json`。HostID 和 labels 不授予权限；普通调用者只发现自己有权访问的目标。

selector 支持 `alias=dev-a,dev-b`、`id=HOST_ID[,HOST_ID]`、`label:env=test&role=worker` 和显式 `all`。空值、无匹配及超过 128 个目标都拒绝；排序和去重以 HostID 为准。计划冻结身份、连接摘要、操作与 rollout，所有执行均须审批，包括 `all` 和超过 20 个目标的大范围计划。

```json
{"selector":"alias=dev-a,dev-b","operation":"job_start","job":{"spec":{"argv":["/usr/bin/true"],"login_shell":false},"resources":{"wall_timeout_sec":300}},"rollout":{"strategy":"canary","canary":1,"wave_size":10,"max_parallel":4,"max_failures":0,"on_threshold":"pause"}}
```

```sh
rdev fleet plan -file spec.json
# 查看全部目标分页；用返回的 plan_id 与 digest 明确批准同一个快照。
rdev fleet approve PLAN -digest SHA -ttl 60
rdev fleet execute PLAN -digest SHA -approval TOKEN
rdev fleet results PLAN -offset 0 -limit 32
rdev fleet pause PLAN
rdev fleet resume PLAN
rdev fleet cancel PLAN
rdev fleet reconcile PLAN
rdev fleet retry PLAN FAILED_HOST_ID
```

`retry` 生成需要独立审批的新子计划，只覆盖明确指定且允许重试的目标；成功或 ambiguous 的尝试不能重试。`reconcile` 查询原 mutation/job 结果，不重新提交命令。状态/结果 CLI 退出码：全部完成成功为 0，失败/取消为 1，未完成或有 ambiguous 为 2；控制命令返回 0 仅表示请求已接受。分页结果只含必要元数据，原始输出通过单 job 工具另行授权查询。详细的 wave、阈值、授权和恢复语义见 [运维说明](docs/rdevd-operations.md#fleet-inventory-and-durable-plans)。

## MCP 工具

以当前 server 的 `tools/list` 为准：standalone 与 broker 模式有不同工具集和授权要求。

| 工具 | 用途与边界 |
|---|---|
| `rdev_exec` | `argv` 数组形式的前台命令 |
| `rdev_job_start` / `_wait` / `_list` / `_status` / `_logs` / `_stop` / `_rm` | 有界后台任务、观察和清理 |
| `rdev_read` / `rdev_write` / `rdev_list` | 文件读写和目录列表 |
| `rdev_fleet` | broker 持久 job_start 编排：预览、审批、执行、分页查询、暂停/恢复/取消及明确失败子集重试；standalone 明确拒绝 |
| `rdev_sync` | push/pull；broker 使用预览、保留计划及审批执行 |
| `rdev_secrets` | standalone 内存凭据；broker principal-owned 凭据 |
| `rdev_session` | 仅 standalone 的 host/session 查询和编辑 |
| `rdev_support` | 静态支持矩阵、前端边界及可选 runtime 探测 |
| `rdev_compat` | 当前构建的协议、错误、config/state 兼容契约 |
| `rdev_state` | 整个 host state root 的管理员检查、迁移和修复 |
| `rdev_storage_status` / `rdev_storage_doctor` / `rdev_storage_gc` | standalone 的受管存储检查和清理；共享 MCP 未注册这些工具 |
| `rdev_ping` / `rdev_capability` | broker 的连接检查和原始 capability probe |
| `rdev_broker_status` / `rdev_broker_pool` | broker 的 owner-scoped 使用量和单独授权的全局连接池统计 |
| `rdev_mutation_status` / `rdev_job_events` | broker 的 mutation outcome 查询及 job 状态历史 |

`rdev_state` 报告可能包含其他 owner 的 root-relative state record 路径，因此共享模式要求独立的 `state_inspect` / `state_migrate` / `state_repair` 管理员授权，推荐 exact-host grant。迁移和修复包括 dry-run 都保留 digest-bound approval。MCP 默认 `dry_run=true`；CLI 用 `-dry-run` 显式预览。

## 命令、参数与超时

命令以 `argv` 数组传入。默认 login shell 用 `exec "$@"` 位置参数 trampoline 加载 profile，再执行原 argv；profile 是受信任的用户代码。`-no-login` 选择直接执行。需要管道时显式传 `["sh", "-c", "a | b"]`，该脚本文本由调用者负责。

CLI 的 `--` 分隔 rdev 参数和命令 argv；之后不再由 rdev 解析。**本地 shell 仍会先处理引号、变量、通配符和命令替换**，所以字面 `$()`、空格、`~` 等必须正确引用。MCP JSON 数组没有本地 shell 这一层。

参数解析保留 `-flag` / `--flag` 和交错位置参数，拒绝未知 flag、缺值、空字符串值、非法数字、溢出、越界、额外 operand 及冲突组合。单值 flag 和布尔开关不能重复；CLI 布尔开关不接受 `=false`。`-exclude` 可重复并保留顺序；exec/job start 和 host 配置的 `-env NAME=VALUE`、host 配置的 `-secret NAME=PATH` 可重复，但同一键不能重复。以 `-` 开头的字符串 flag 值使用 `-key=value`。daemon 的标准 flag 接口也拒绝重复参数，并保留其标准布尔值语法。

所有入口使用同一超时契约：

| 预算 | 缺省或显式 `0` | 正值与上限 | 到期行为 |
|---|---|---|---|
| 前台 exec：CLI `-timeout` / MCP `timeout_sec` | 60 秒 | 1–3600 秒 | 终止该命令进程组，报告超时和已保留输出 |
| job wait：CLI `-timeout` / MCP `timeout_sec` | 300 秒 | 1–3600 秒 | 仅停止本次观察，返回 `timed_out`，不终止 job |
| 新后台 job：CLI `-wall-timeout` / MCP `resources.wall_timeout_sec` | 3600 秒 | 1–3600 秒 | supervisor 执行 job 运行时限 |

负值、溢出、超上限和显式无限均拒绝；`0` 不表示无限。连接建立和请求 context/deadline 是独立预算，可能更早结束；exec 的运行时限不等于 SSH 建连的总时限。前台取消隔离于其他客户端；后台 job 不因连接断开或一次 wait 到期而结束，但仍受自身运行时限约束。

这是兼容性行为变化：旧调用中缺省/零值的无限 exec 和无限新 job 现在使用上述有限默认值。已有显式合法正值继续有效；升级不会追溯修改已经运行的旧 supervisor。exec/wait 向兼容旧 agent 发送规范化后的显式值，避免旧零值语义重新放开预算。新 job 还要求 agent 协商 `job_resource_envelope` feature；不支持该 feature 的旧 agent 在发送 job start 前明确拒绝，须先升级 agent。已有 job 的 status/wait/stop 和公共 read/ping 不因此停止兼容。

MCP job start 接受 `resources`；结果保留 `requested_resources`、`effective_resources`、`resource_limit`。可请求 FD 和 job 数量限制，但受目标能力和硬限约束。当前 CPU、内存、PID 的整棵进程树预算不支持，不能把探测到 cgroup 当成已经能够执行这些限制。

```bash
rdev exec dev -timeout 30 -- sqlite3 db "SELECT json_extract(x,'\$.score') FROM runs"
rdev job start dev -label batch -env MODE=batch -wall-timeout 600 -- ./run.sh
rdev job wait dev <id> -timeout 60 -tail 20
rdev job logs dev <id> -grep ERROR -tail 50
rdev job stop dev <id> -signal TERM -grace 5
rdev job rm dev -keep-last 5 -older-than 86400
rdev read dev '~/app/config.yaml'
rdev ls dev '~/app' -limit 50
printf '%s\n' content | rdev write dev /tmp/f.txt
rdev sync dev push -exclude .git -dry-run -- -leading-local /remote/dst
rdev sync dev push -- './目录 with spaces' /remote/dst
rdev state inspect dev
rdev state migrate dev -dry-run
rdev support dev -refresh
rdev env inspect dev
rdev compat
```

`write` 和共享 `secret set` 读取有大小上限的 stdin；EOF 才代表正常结束。非 EOF 错误，包括“部分数据同时报错”，会失败并丢弃已读内容，在 standalone/broker 两条路径上都不提交部分业务写入。同步的本地路径可在 `--` 后以裸 `-` 开头，支持空格和 Unicode；远端根路径沿用受限的安全字符语法，不能套用本地路径的宽松规则。

## 地址与 SSH

配置入口、registry、连接身份及最终 SSH 参数使用同一地址解析器：

| 形式 | 含义 |
|---|---|
| `service-deploy` | `hosts add` 的 SSH config alias；直接业务调用的裸单词必须先注册为 rdev alias |
| `host.example` / `192.0.2.1` / `user@host` | DNS、IPv4 或指定 SSH 用户 |
| `user@host:2222` | 内嵌端口 |
| `2001:db8::22` | 整体是裸 IPv6 地址，末段不是端口 |
| `user@[2001:db8::1]:2222` | 带端口的 IPv6 |
| `[::1]` 配合独立 `-port 2222` | IPv6 地址与独立端口 |

```bash
rdev hosts add ipv6 'user@[2001:db8::1]:2222' -save
rdev hosts add jump-target service-deploy -save
```

内嵌端口与独立非零 `port` 不能同时给出，即使数值相同；CLI 显式端口范围为 1–65535。空主机、畸形括号、非法端口、option-shaped 地址、空白/控制字符均拒绝。规范地址与端口进入 registry 和连接 key，用户/端口不同保持连接身份隔离。SSH 使用无括号 IPv6 destination，rsync 的 `host:path` 使用加括号的 IPv6。

OpenSSH 的 `BatchMode` 禁止交互认证提示。首次连接前按用户的 SSH host-key 策略登记并核对指纹；rdev 不关闭 host-key 检查。SHA-256 用于验证 agent 上传内容，不能替代 SSH 主机身份，也不是发布签名。

## 共享连接、权限与后台状态

MCP 使用官方 SDK 的 JSON-RPC 2.0 over stdio。内部 client↔broker 和 client/broker↔agent 使用各自版本握手及按 ID 关联的 NDJSON 协议，**不是 JSON-RPC**。

```text
AI / SDK ── MCP JSON-RPC stdio ── rdev
CLI ────────────────────────────┘
 standalone: rdev ── SSH + agent NDJSON ── rdev-agent
 shared:     rdev ── Unix socket + broker NDJSON ── rdevd
                    rdevd ── SSH + agent NDJSON ── rdev-agent
```

standalone 的连接池属于当前进程；共享模式由一个 `rdevd` 持有基础 agent transport，多个前端复用它，并按需建立独立 bulk lane。standalone 的基础连接保留到显式断开、失效或进程关闭；broker 区分 active host 限额、warm pool、warm idle TTL、最后客户端退出后的 grace 和 bulk idle TTL。清理遵守在途 lease，不会因为一个客户端退出就关闭其他客户端仍使用的 transport。后台 supervisor 独立于这些连接的生命周期。关闭 agent channel 不承诺结束用户或其他进程共用的 OpenSSH master；master 仍遵守其 ControlPersist 策略。

broker 凭证绑定精确 `(client_id, project_id)`，policy 默认拒绝并支持 exact-host grant。任意 exec、写入、job start/stop/rm 和其他 mutation 不能靠客户端 `Risk=false` 绕过审批。审批绑定 owner、目标身份、有效请求摘要和 policy snapshot；audit 使用摘要及最小化 metadata，不记录原始 secret、命令输出或任意请求正文。详见[运维说明](docs/rdevd-operations.md)。这些权限控制不隔离能访问同一 OS 账户私有文件的进程。

相同 job 的等待共享远端观察，每个客户端保留独立观察时限和取消生命周期；订阅到期会释放计费，job 继续运行。job 所属 owner/project 由 broker 管理，普通业务查询不能跨 owner 观察或控制。`pool.health` 和 state 管理等全局/host-wide 管理接口需要单独授权。

新 job 先建立 durable intent 和可恢复身份，再启动 supervisor。已知 outcome 可查询并复用；无法证明是否执行的 mutation 保持 `possibly_executed` / `ambiguous_outcome`，不能自动换 ID 重放。agent 的通用短期去重缓存仍有容量和 TTL，不能把它理解为任意命令的永久 exactly-once 保证。

supervisor 记录退出码，支持跨 SSH 断连和 serving agent 重启查询；主机重启、supervisor 被杀或损坏状态可能使结果变成 unknown/orphaned，不能保证恢复丢失的退出码。job 日志和受管磁盘有上限、截断账本及清理契约；这不限制任意业务程序在受管目录外写入的磁盘。`job rm` 不删除仍运行的任务，按持久进程身份及 job 锁检查，并区分 removed/missing/skipped。

同步遵守排除保护、路径布局、symlink/conflict 策略和资源计费。standalone `--delete` 需要预览检查和显式确认；broker 先 `-prepare` 获取保留计划，再用 `-plan` 与精确审批执行，删除额外要求 `sync.delete` 和 `-confirm-delete`。取消同步会终止该 rsync 的进程组，不关闭其他客户端的共享基础 transport。

## 凭据与脱敏

standalone 的手工/声明式 secret store 是进程内存态；host 配置只保存声明路径。连接初始化先原子读取并验证全部声明 secret，再发布 ready 连接，失败时 fail closed。同一 host identity 的手工值优先，重连刷新声明值。`secret:name` 只解析当前 host 的精确 scoped 值，不回退到别的 host 或 output-only 值。

broker 的 `secret set`、`set_from_file`、`list`、`delete` 是 principal-owned 持久凭据接口，凭据版本保存在 daemon 的私有 `<socket>.secrets` 文件中（0600）。备份、访问和清理该文件应按私有凭据处理。引用还需 `secret.use`；远端文件导入同时需要 `secret.set_from_file` 和 `read_file` 授权及审批。共享模式暂不支持把管理员 registry 的声明式 secret 自动委派给业务 principal，替代方式是显式导入 principal-owned 凭据。

```bash
# broker 模式：值经 stdin 输入；设置/导入/删除仍需对应授权和审批
rdev secret set dev apptoken < /private/token
rdev secret set_from_file dev apptoken '~/.config/myapp/token'
rdev secret list dev
rdev secret delete dev apptoken

# standalone：这些命令用于验证本进程的注册及脱敏
rdev secrets set-from-file apptoken /private/token -host dev
rdev secrets check dev apptoken -path '~/.config/myapp/token' -- env
```

返回边界递归脱敏已注册值，standalone MCP 另有结果中间件兜底。轮换/删除时保留在途操作的旧脱敏快照；broker 使用 principal/host 绑定的 credential archive。secret 最少 6 bytes，远端导入最多 64 KiB，空值、二进制、截断和读取失败不会发布部分注册。

脱敏用于降低意外 echo/dump 的泄漏风险：支持原值和一定条件下的空白折行；不承诺拦截部分值、重新编码、hash、大小写变换或主动窃取。未注册凭据不会自动识别，远端日志文件也不会因客户端脱敏而被清洗。exec/read/sync 的二进制返回在脱敏边界解码后处理，输出截断报告明确记录保留和丢弃的字节，不构成丢弃数据的归档。

## 平台支持

`rdev support` / `rdev_support` 使用 `internal/support` 同一数据源，区分静态 supported/experimental/unsupported、build-only、历史基线和真实运行验证。带 host 的探测只说明当前目标能力，不能升级整个平台的认证等级。

| 组合 | 本地 rdev/rdevd | 远端 agent |
|---|---|---|
| Linux amd64 | standalone/shared 真实运行验证 | Ubuntu 真实 SSH、同步、取消、job 验证 |
| Linux arm64 | build-only | build-only |
| macOS arm64 | 本机 cgo/launchd 及 shared controller → Linux amd64 真实 SSH 已验证；macOS 作为远端 agent 未验证 | build-only |
| macOS amd64 | build-only | build-only |
| 原生 Windows amd64 | unsupported | Phase9 实验实现；交叉构建通过，原生运行与真实 SSH 验收待完成 |

**macOS arm64 作为本地 controller 的 runtime 已验证；macOS 远端 agent、macOS amd64 和 logout/reboot 生命周期仍未验证。** 证据见 [macOS controller 验收](docs/macos-controller-acceptance.md)。Darwin 配置的 fd-native ACL 检查依赖 cgo，无该能力时 fail closed。OpenSSH 需要 `BatchMode`、`ControlMaster`、`ControlPath`、`ControlPersist`；最低版本尚未完成正式认证。复杂 ProxyCommand 是实验能力；PTY/TUI、通用端口转发、完整 ACL/xattr/owner 保真和 remote-to-remote sync 不在当前支持范围。

shared `support HOST` 返回当前 principal 的权限快照；`permission_denied` 与平台 unsupported 分开。`scope=broker` 表示无 host 的本地 broker 接口；`secret.use`、`sync.delete` 的 `callable=false` 表示附加权限，不是独立路由。没有 `capability_probe` 授权时不触发 SSH，也不返回其他 owner 的状态、registry 或 execution profile。权限许可不替代 runtime 能力、资源准入或 mutation approval。

## 兼容与发布检查

`rdev compat` / `rdev_compat` 从实际版本常量、错误注册表、配置结构和校验逻辑生成机器可读契约，覆盖 client/agent、client/broker、错误 code、host/broker config、project trust 和持久 state。

standalone client/agent 当前协商 2–3，client/broker 协商 1，broker/agent 要求协议 3 保留 principal 身份。协议 2 只保留公共 unary 操作；依赖 v3 feature 的操作明确拒绝，不能获得 v3 取消、streaming 或去重保证。不相交或非法版本区间在业务请求前失败。N/N-1 必须绑定实际发行源码与二进制；协议/schema 范围只说明协商边界，不代表发行组合已经认证；旧 broker 上没有的新路由会失败，不回退为 standalone。

host/broker config 当前仍是无版本 JSON shape，两者对未知字段的处理不同，以 `compat` 为准。state 迁移只按声明的 legacy→current 方向进行；未来 schema、损坏记录和不支持组合不会被静默当作当前版本。错误 code 和 retry/execution-state 含义属于契约，未知 code 不应被猜成可重试。

agent 上传须先通过管理员 release policy：默认 `~/.config/rdev/release-policy.json`，共享模式由 rdevd 读取自己的可信配置。stable/beta 验证 SSHSIG、渠道、版本 pin 与实际内嵌字节；unsigned dev 必须显式 opt-in。项目配置不能提供信任根或放宽渠道。签名 rollback 还需管理员为具体目标和 digest 授权，force 不绕过校验。远端每个安装目录使用跨进程锁、真实 hello/state readiness、原子切换与已知可用版本回退。详见 [Phase8 验收与操作](docs/phase8-acceptance.md)。

```bash
make GO="$(command -v go)" release-gate
make GO="$(command -v go)" verify-release
# 可用 RDEV_RELEASE_OUT 指定私有输出目录，默认 bin/release
```

release gate 固定 `govulncheck v1.8.0` 和 `go.mod` 的 Go 工具链，联网查询依赖更新、验证 module checksum，对支持的构建平台和实际二进制执行漏洞检查。当前依赖包含 MCP SDK v1.7.0 和 `golang.org/x/sys v0.44.0`。可用更新需要评审，不自动全部升级。

产物包含实际二进制、`manifest.json`、CycloneDX `sbom.cdx.json`、未签名 `provenance.intoto.json`、源码快照和审计报告；验证器核对产物摘要、构建信息、模块依赖及 metadata 对应关系。它是可执行的本地 gate，没有宣称托管 CI 已运行或 provenance 已由可信签名者认证。联网失败或 skipped 不是通过，实际执行证据以验收记录为准。

Phase8 已加入独立 SSHSIG 签名/验签入口、实际链接依赖的 `THIRD_PARTY_NOTICES.txt`、渠道策略和升级事务；完整工程验收、正式发行身份、真实托管 CI、平台矩阵和 24h 认证状态见 [验收表](docs/phase8-acceptance.md)。默认 gate 仍生成 unsigned 本地产物，不发布 release 或部署。 新增真实二进制安装/回滚、磁盘 ENOSPC/EROFS、SSH ControlMaster 丢失和历史源码兼容检查的命令与证据也在该验收表中；它们分别保留实际通过范围和未运行项。

## 开发与验证

```bash
make GO="$(command -v go)" all daemon
make GO="$(command -v go)" check       # vet、全仓单测、当前源码与嵌入agent一致性
"$(command -v go)" test -race ./...
rdev version                         # 构建stamp及嵌入agent摘要
```

修改 agent 后必须重建嵌入副本；直接 `go build ./cmd/rdev` 不会重建 `cmd/rdev/agents/`。`make check-agents` 通过同工具链、同 stamp 的内容重建比较检查一致性。构建 stamp 使用 commit 时间，dirty tree 标记为不可排序。

真实 SSH、daemon 生命周期、独立进程与官方 MCP SDK、取消、同步、owner 隔离、共享等待及混合负载的命令和最终证据集中维护在验收与 runtime 文档，不在 README 复制历史测试数量或旧结果。性能门槛须避开并行重型构建/压测。独立本地 IPv6 harness 可用 `RDEV_RUN_IPV6=1 go test ./internal/client -run '^TestLocalIPv6SSHAndSync$'`，只在隔离的 `::1` sshd 使用临时新建密钥。

## 布局

```text
cmd/rdev/            CLI 与 MCP 入口，嵌入agent
cmd/rdevd/           共享daemon及服务生命周期
cmd/rdev-agent/      远端执行、job supervisor和state维护
internal/proto/      agent/broker版本与错误、操作契约
internal/transport/  SSH参数、连接和安全bootstrap
internal/client/     连接池、会话应用、同步和脱敏
internal/broker/     principal、policy、approval、audit、资源和owner状态
internal/mcpsrv/     MCP工具与结果投影
internal/session/   host配置、信任、身份generation
internal/secrets/   scoped凭据和脱敏
internal/support/   支持矩阵及runtime发现
internal/compat/    机器可读兼容契约
internal/release/   发布检查、SBOM和provenance校验
```

## 许可

项目代码使用 MIT，见 [LICENSE](LICENSE)。依赖各有自己的许可证，以固定版本附带的 LICENSE/NOTICE 为准。MCP SDK 含 Apache-2.0/MIT 许可代码；其他模块的许可不能由本项目 MIT 自动替代。

Go 二进制会链接使用到的依赖代码，包括 MCP SDK；`go.mod` 引用且未 vendor 不代表分发的二进制不含这些代码。分发源码或二进制时须遵守相应许可、版权及适用 NOTICE 义务。release gate 按六个实际二进制的链接模块版本收集原文 LICENSE/NOTICE/COPYING/COPYRIGHT 和 Go 工具链许可，生成有摘要绑定的 `THIRD_PARTY_NOTICES.txt`；最终分发须携带对应文件与 SBOM。实际收集结果与认证边界见 Phase8 验收表。

## 项目文档

- [rdevd 运维说明](docs/rdevd-operations.md)：凭证、权限、审批、服务安装和故障恢复。
- [Phase6 验收记录](docs/phase6-acceptance.md)：入口、兼容、供应链及最终验证。
- [Phase5 验收记录](docs/phase5-acceptance.md)及[运行验证索引](docs/phase5-runtime-evidence.md)：生产代码基线、独立评审和实际证据。
- [架构与分阶段计划](docs/rdev-evolution-security-plan.md)：原始验收要求、当前阶段和延期边界。
- [安全说明](SECURITY.md)与[Phase0–1 独立审查](docs/security/phase0-1-codex-security-review.md)。
