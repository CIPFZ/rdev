# rdev 架构演进、安全加固与分阶段任务计划

> 状态：实施中；Batch A（Phase 0–1 当前批次）已在 `c9a796cc8ea57aee2afbca13671d27b360baaee5` 完成独立审查与验收
>
> 建档日期：2026-08-27
>
> 审查基线：`11e9c588c857277b768799fbf5d62c223697ff6e`
> 范围：全部 32 个 Git 跟踪文件；离线静态审查，未连接生产远端，未刷新在线依赖漏洞库

## 1. 文档目的

本文档归档 rdev 当前架构、已确认的安全问题和工程缺陷，并定义从“单个本地 Agent 管理少量远端”演进到“多个本地 Agent 稳定共享百台远端”的实施计划。

计划遵循以下顺序：

1. 先封住项目配置、SSH bootstrap 和密钥边界。
2. 再修正请求重试、取消、资源限制等协议语义。
3. 为单进程加入有界连接池和百机生命周期管理。
4. 最后引入用户级 `rdevd`，让多个本地 Agent 共享连接而不互相阻塞。

本文不是版本承诺。每个阶段只有在验收条件通过后，才能进入下一阶段。

## 2. 当前架构基线

```text
CLI / stdio MCP
       │
       ▼
internal/client
 ├─ session：host、cwd、sticky env、项目/全局配置
 ├─ secrets：secret 引用、环境注入、结果脱敏
 └─ transport.Conn：连接池、请求 ID、并发派发
       │
       ▼
OpenSSH ControlMaster + 长期 SSH channel
       │
       ▼
rdev-agent（ID 关联的 NDJSON 协议）
 ├─ exec/read/write/list
 └─ job supervisor、日志和持久状态

rdev_sync ──> 独立 rsync 进程，但复用 ControlMaster
```

### 2.1 已解决的问题

- 普通命令使用结构化 `argv` 和 `exec.Command`，login shell 使用固定 `exec "$@"` trampoline，避免把用户命令拼成 shell 字符串。
- 请求带 ID，同一 agent channel 可并发处理请求，慢请求不会占住整个连接。
- 长任务通过 supervisor、`setsid` 和持久 job 状态跨 SSH 断线存活。
- 前台命令支持超时、输出上限、截断状态和字节统计。
- agent 上传包含哈希检查、原子替换和降级保护。
- `rsync`、probe 和 bootstrap 可复用 OpenSSH ControlMaster，减少重复认证。

### 2.2 当前复用边界

当前连接池属于单个 `Client` 进程。多个本地 AI Agent 即使指向同一远端，也分别维护：

- 本地 `ssh` 子进程和 rdev transport 状态；
- 远端 `rdev-agent` serve 进程；
- secret Store、pending request 和重连状态；
- 各自的并发和健康判断。

相同 `Addr:Port` 的 OpenSSH 调用可能通过确定性的 ControlPath 共享底层 ControlMaster，但这只复用了 TCP/认证，不等于共享 rdev-agent 会话和连接生命周期。

## 3. 目标架构

### 3.1 用户级本地 broker

最终目标是在本机增加用户级 `rdevd`：

```text
Agent A ─┐
Agent B ─┼─ Unix Domain Socket ── rdevd ── Host Connection Manager
Agent C ─┘                     │
                               ├─ host-01：共享控制/执行连接
                               ├─ host-02：COLD，无连接
                               └─ host-03：基础连接 + 按需 bulk lane
```

`rdevd` 是 SSH、远端 agent、secret 和连接状态的唯一所有者。CLI/MCP 只作为 broker client，不再各自直接维护 SSH。

### 3.2 SSH 复用层次

“一条 SSH”必须区分以下层次：

```text
TCP connection
  └─ SSH transport / ControlMaster
       ├─ SSH channel：rdev-agent
       ├─ SSH channel：bootstrap/probe
       └─ SSH channel：rsync 或 bulk transfer
            └─ rdev-agent channel 内再复用多个 request ID
```

一条 SSH transport 可以承载多个 channel；一个 rdev-agent channel 也可以并发处理多个请求。因此共享 transport 不等于任务串行。

真正可能造成阻塞的环节是：

- 单 TCP 拥塞窗口被大文件传输占用；
- SSH 加解密 CPU 或 socket buffer 饱和；
- agent 的 handler semaphore 饱和；
- 大响应在单 writer 上序列化，延迟小响应；
- 一个客户端占满全部远端执行 slot。

### 3.3 连接 lane

每台活跃主机默认只维持一条基础 transport，并按流量分类：

| Lane | 用途 | 生命周期 | 调度要求 |
| --- | --- | --- | --- |
| control | ping、status、cancel、job start/stop、小响应操作 | 随基础连接存在 | 最高优先级，不得被 bulk 阻塞 |
| exec | 普通 exec/read/write/list | 随基础连接存在 | 有界并发、跨客户端公平 |
| bulk | rsync、大文件、持续日志流 | 有需求时建立，短 TTL | 最多 1 条/host，空闲快速关闭 |

当 bulk traffic 影响控制延迟时，bulk lane 使用独立 SSH transport；没有大流量时不额外占用连接。

### 3.4 百机连接生命周期

```text
COLD
  │ request
  ▼
DIALING ── failure ──> BACKOFF
  │ success                │ retry/new request
  ▼                        └──────────────┐
WARM <────────────────────────────────────┘
  │ idle TTL / LRU eviction
  ▼
EVICTING
  │
  ▼
COLD
```

建议初始默认值，最终以压测数据调整：

```yaml
connection_pool:
  max_warm_hosts: 16
  max_concurrent_dials: 6
  idle_ttl: 180s
  max_idle_ttl: 600s

per_host:
  max_inflight: 12
  max_inflight_per_client: 4
  max_queue: 256

bulk:
  max_lanes_per_host: 1
  idle_ttl: 45s

reconnect:
  base_backoff: 500ms
  max_backoff: 30s
  jitter: true
```

连接策略：

- 配置主机不等于连接主机；首次请求时才拨号。
- warm host 数量有全局上限，空闲连接按 LRU 淘汰。
- 同一 canonical host 的拨号使用 singleflight。
- 全局限制并发拨号，避免网络恢复后的重连风暴。
- 失败使用指数退避、随机抖动和 circuit breaker。
- 不对全部配置主机做高频 heartbeat；只验证活跃连接或请求到达后的陈旧连接。
- 关闭 transport 不终止已脱离 SSH 的后台 job。

### 3.5 智能断开与连接租约

连接回收不能只依赖“创建后经过固定时间”，而应依据是否仍有使用者、是否存在在途工作、连接重建成本和连接池压力共同决定。

每条连接维护以下状态：

- `lease_count`：正在使用连接的 client/request/watch 数量；
- `inflight`：已经发送、尚未得到最终结果的请求数；
- `queued`：等待该连接执行的请求数；
- `last_activity_at`：最后一次成功请求、响应或有意义的 transport 活动；
- `last_client_detached_at`：最后一个本地 client 断开的时间；
- `dial_cost`：近期拨号、认证、ProxyJump 和 bootstrap 延迟的移动统计；
- `health`：healthy、suspect、backoff 或 draining；
- `pinned`：用户显式要求预热/保活的少量关键主机。

安全断开条件必须同时满足：

```text
lease_count == 0
&& inflight == 0
&& queued == 0
&& bootstrap/upload/transaction 未进行
&& idle_age >= effective_idle_ttl
```

后台 job 已经脱离 SSH，不自动持有连接 lease。只有仍在进行的前台 exec、文件传输、job wait/watch 或协议事务才持有 lease。

驱逐流程：

1. 在同一把状态锁下把连接从 `WARM` 改为 `DRAINING`，阻止驱逐与新请求竞态。
2. 新请求到达时，如果关闭尚未开始，则取消 draining 并恢复 `WARM`；否则排队等待重新连接。
3. 向远端 agent 发送有界的 graceful shutdown，停止接收新请求。
4. 关闭 agent channel，等待短暂 drain deadline 后强制回收本地 ssh 子进程。
5. 只有 `rdevd` 明确拥有该 ControlMaster 时才主动执行 master exit；当前多进程模式不能关闭可能由其他进程共享的 master。
6. 记录 disconnect reason、idle age、连接寿命、未完成请求数和关闭耗时。

`effective_idle_ttl` 初期采用确定性策略，避免一开始引入难以解释的自适应算法：

- pool 未达上限：使用普通 `idle_ttl`，昂贵的 ProxyJump 连接可使用较长 TTL；
- pool 达到上限：对无 lease 的连接按 LRU 提前驱逐；
- 最后一个本地 client 退出：进入较短 grace，例如 30 秒；
- 连接处于 suspect、认证配置变化或 host key 策略变化：立即停止复用；
- pinned host：不受普通 idle TTL 影响，但仍受 hard idle、健康失败和显式 disconnect 约束。

建议增加配置：

```yaml
disconnect:
  last_client_grace: 30s
  stale_probe_after: 60s
  drain_timeout: 2s
  eviction_jitter: 15s
  allow_pinned_hosts: true
```

不要为全部配置主机定期发心跳。连接闲置超过 `stale_probe_after` 后，在下一次真实请求复用前执行有短超时的 ping；活跃连接继续依赖 SSH keepalive。这样 100 台已配置但未使用的主机不会产生周期性网络和进程开销。

### 3.6 Canonical connection key

当前 ControlPath 只基于 `Addr:Port`，目标设计至少包含：

- resolved host 和 port；
- SSH user；
- proxy jump/proxy command 配置身份；
- identity/profile；
- remote state namespace；
- 影响连接安全语义的策略版本。

不同别名解析到同一连接身份时允许复用；不同用户、身份或跳板配置不得误复用。可以利用受控的 `ssh -G` 结果生成 canonical key，但不得把私钥内容纳入 key 或日志。

### 3.7 可观测性设计

可观测性属于连接管理和协议设计的一部分，从 Phase 0 开始建设。目标是回答以下问题：

- 当前连接了多少主机，分别处于什么状态，为什么没有被回收？
- 冷连接、warm reuse、重连和 bootstrap 分别花了多久？
- 哪个 host/client/op 占用了并发、队列和流量？
- 请求是在本地排队、SSH transport、远端执行还是响应写回阶段变慢？
- 连接断开属于 idle eviction、健康失败、远端退出、本地主动关闭还是协议错误？
- bulk traffic 是否影响 control latency？
- 资源增长来自 ssh 子进程、文件描述符、goroutine、buffer、job log 还是等待者？

#### 3.7.1 结构化日志

日志使用稳定事件名和结构化字段，默认写 stderr 或本机轮转文件；`rdevd` 可接入系统日志。关键事件包括：

- `connection.state_changed`、`connection.dial_started/succeeded/failed`；
- `connection.evicted`、`connection.health_failed`、`connection.backoff`；
- `request.queued/started/completed/canceled/ambiguous`；
- `job.started/exited/orphaned/reclaimed`；
- `security.config_rejected`、`security.secret_load_failed`、`protocol.frame_rejected`。

所有日志携带可关联但不泄密的 `connection_id`、`client_id`、`request_id`、`operation_id`、`host_key_hash` 和 `op`。默认禁止记录：

- 完整 argv、stdin、stdout、stderr；
- env value、secret name/value；
- 私钥路径、认证材料和完整本地/远端敏感路径；
- 未经脱敏的远端错误。

日志必须在 sink 前经过统一 redactor，并采用字段 allowlist，而不是依赖调用者自行避免敏感字段。

#### 3.7.2 Metrics

Metrics 首先通过 `rdev status --json`、`rdev doctor --json` 和 broker Unix socket 提供；Prometheus/OpenTelemetry exporter 可选启用，默认不开放无认证 TCP 监听。

建议初始配置：

```yaml
observability:
  log_level: info
  log_format: json
  log_max_size: 20MiB
  log_backups: 5
  metrics_exporter: local
  trace_sample_ratio: 0.01
  diagnostics_history: 100
```

错误、连接状态变化和安全事件不采样；高频成功请求可采样或只进入 metrics。debug 日志必须显式开启，并继续遵守相同的敏感字段禁令。

建议指标：

| 范围 | 指标示例 |
| --- | --- |
| Pool | configured/warm/cold/dialing/backoff/draining host 数，warm hit ratio，LRU eviction 数 |
| Connection | dial/probe/bootstrap/handshake latency，connection lifetime，idle age，disconnect reason，reconnect count |
| Request | total、inflight、queue depth、queue wait、duration、timeout、cancel、retry、ambiguous outcome |
| Traffic | request/response bytes，truncated bytes，control/exec/bulk bytes |
| Job | running、started、exited、orphaned、stale PID、log bytes、GC bytes |
| Process | RSS、CPU、goroutine、FD、ssh child process、buffer bytes |
| Security | config reject、secret load failure、redaction hit、oversized frame、symlink reject |

Label 必须是低基数集合。允许 `op`、result、state、reason、lane，以及经过规范化或 hash 的有限 host identity；禁止用 request ID、完整路径、命令、错误文本或任意 project name 作为 metric label。

#### 3.7.3 Tracing 与诊断快照

一次请求的 trace 至少覆盖：

```text
client request
  -> broker queue
  -> acquire connection lease
  -> dial/probe/bootstrap（如需要）
  -> remote operation
  -> response decode/redaction
  -> client response
```

`rdev doctor --json` 应输出可安全分享的诊断快照：版本、协议版本、连接状态摘要、近期失败分类、资源使用、配置来源摘要和脱敏后的建议，不输出 secret、命令内容或原始远端输出。

初始 SLO/演进指标：

- 配置 100 host 且无请求时，远端连接数最终回到 0；
- warm reuse ratio、cold dial p50/p95、control request p95/p99 可持续观察；
- pool 达上限时不发生请求饥饿或无界队列；
- bulk 运行时 control latency 不超过无 bulk 基线的 2 倍；
- 网络整体故障/恢复时拨号并发不超过配置值；
- 本机和远端 managed storage 在清理宽限期外不超过 hard budget；
- 长时间 soak test 中 RSS、FD、goroutine 和 ssh child 数回归稳定平台。

### 3.8 磁盘预算与清理策略

磁盘治理分为两类独立预算，不能用一个全局数字混在一起：

| 位置 | rdev 管理的数据 | 主要风险 |
| --- | --- | --- |
| 本机 | `rdevd` 日志、诊断历史、trace/exporter spool、临时下载或升级文件 | 长期运行后占满本机磁盘 |
| 远端 host | `state/jobs` 下的 stdout、stderr、metadata、lock、临时 agent 文件 | 批量任务输出填满远端磁盘，影响其他服务 |

rdev 只清理自己管理的 state/cache/log，不自动删除用户通过 `rdev_write`、`exec` 或 `sync` 创建的任意业务文件。

建议初始配置：

```yaml
storage:
  local:
    max_bytes: 1GiB
    retention: 7d
    high_watermark: 0.85
    low_watermark: 0.70
  remote_state:
    max_bytes: 5GiB
    min_free_bytes: 2GiB
    retention: 7d
    keep_last_jobs: 100
    high_watermark: 0.85
    low_watermark: 0.70
  per_job:
    max_stdout_bytes: 128MiB
    max_stderr_bytes: 128MiB
    on_log_limit: truncate_oldest
  cleanup:
    interval: 5m
    max_delete_per_run: 1GiB
    dry_run: false
```

数值是初始建议，必须可在 global、host 和 project policy 层覆盖，同时设置系统硬上限，防止不可信项目配置把配额改为无限。

#### 3.8.1 水位和清理顺序

- 达到 high watermark 后触发 GC，持续清理到 low watermark，避免在阈值附近频繁抖动。
- `min_free_bytes` 优先于百分比预算；低于安全余量时拒绝启动新的持久 job，并明确返回 `disk_pressure`。
- 单次 GC 有删除预算和时间预算，避免清理本身长期占用磁盘 I/O。
- 多个 agent/broker 可能并存时，GC 必须使用远端 state lock；最终由单一 `rdevd` owner 负责调度。

默认清理顺序：

1. bootstrap、写入和升级遗留的可确认临时文件；
2. 已过 retention 的已结束 job 日志；
3. 超出 `keep_last_jobs` 的最旧已结束 job；
4. 已结束 job 的轮转日志分片；
5. 仍不满足安全余量时，拒绝新持久 job 并告警。

不得自动删除：

- 运行中 job 的 metadata、PID identity、lock 和控制记录；
- 用户业务目录或任意不在受管 state root 内的路径；
- 无法确认 ownership/format 的未知文件；
- 唯一能用于恢复/停止运行中 job 的记录。

#### 3.8.2 运行中日志达到上限

当前 job 把子进程 stdout/stderr 直接写入普通文件，无法执行可靠的滚动配额。目标实现由 supervisor 持续 drain pipe，并写入有界分片或 ring file，保证清理日志时不反向阻塞子进程。

`on_log_limit` 支持：

- `truncate_oldest`：默认，保留最新日志，累计 `dropped_bytes` 并设置 `logs_truncated=true`；
- `stop_job`：对必须保留完整审计日志的任务，在达到上限时停止 job；
- `discard_new`：保留开头，继续 drain 但丢弃新字节，同样必须暴露截断状态；
- `unlimited`：只允许可信 global policy 显式开启，项目配置不能开启。

任何策略都不能静默丢失日志。`job_status`、`job_logs` 和最终退出结果必须包含限制、已保存字节、原始字节、丢弃字节、首次截断时间和策略。

#### 3.8.3 存储观测和管理接口

新增安全接口：

- `rdev storage status [--host HOST] --json`：预算、已用空间、文件系统可用空间、最大消费者、下次 GC；
- `rdev storage gc --host HOST --dry-run`：只列出候选和预计释放空间；
- `rdev storage gc --host HOST`：按同一策略执行有界清理；
- `rdev doctor`：汇总 disk pressure、quota hit、GC failure 和不可识别 state。

指标至少包含：

- `storage_managed_bytes`、`storage_free_bytes`、`storage_budget_bytes`；
- `job_log_bytes`、`job_log_dropped_bytes`、`storage_quota_hits_total`；
- `storage_gc_runs_total{result}`、`storage_gc_deleted_bytes_total`、`storage_gc_duration`；
- `storage_pressure{level}` 和按固定 reason 分类的清理失败。

host 标签仍遵守低基数策略；文件名、job ID、路径不能进入 metric label，只能在经过脱敏且有界的诊断明细中出现。

### 3.9 Fleet inventory 与批量编排

百机模式不能让上层 Agent 自己对 host 列表写循环。Fleet 层需要把目标选择、并发、波次、失败阈值和结果聚合定义为可恢复的数据模型。

核心对象：

```text
HostRecord
  immutable_host_id
  aliases[]
  labels{env, region, role, owner, ...}
  connection_profile
  policy_refs[]

FleetPlan
  plan_id
  selector
  resolved_host_snapshot[]
  operation_digest
  rollout_policy
  failure_policy
  approval_ref

HostRun
  plan_id + host_id
  state / attempt / operation_id / result
```

设计原则：

- selector 在计划开始时解析为不可变 host snapshot；执行中 inventory 变化不能悄悄扩大目标。
- 空 selector、`all` 或匹配数量超过阈值时必须显式确认，不能默认作用于全部主机。
- 每台 host 使用独立稳定 `operation_id`，恢复和重试不能重复已成功的 mutation。
- FleetPlan 状态包括 `planned/running/paused/completed/failed/canceled`，状态持久化并可恢复。
- 结果模型必须保留成功、失败、跳过、取消、ambiguous 和 unreachable，不能用单一 exit code 抹平部分失败。
- batch cancel 只取消未开始和可取消的在途操作；已提交但结果不明的 host 标记 ambiguous。

rollout policy 至少支持：

```yaml
rollout:
  strategy: waves       # all_at_once | waves | canary
  max_parallel: 10
  canary: 1
  wave_size: 10
  pause_between_waves: 30s
  max_failures: 2
  max_failure_ratio: 0.10
  on_threshold: pause   # pause | cancel_remaining
```

首版 inventory 以受信任的静态配置为主，预留 provider 接口；云服务发现、CMDB 和 Kubernetes 不进入首版核心。动态 provider 返回的数据必须先生成 snapshot 和 diff，再进入计划。

### 3.10 Capability、策略、审批与审计

共享 `rdevd` 不能把一个本地 Agent 的权限隐式授予所有 client。broker 必须在请求进入队列前执行 policy decision。

策略输入：

```text
principal: local user / client_id / project_id
target: host_id + labels + connection profile
operation: op + risk flags + path class
context: interactive/automation, approval token, current policy version
```

基础 capability：

- `host.inspect`、`connection.use`；
- `exec.foreground`、`exec.background`、`exec.signal`；
- `file.read`、`file.write`、`file.delete`；
- `sync.push`、`sync.pull`、`sync.delete`；
- `job.read`、`job.stop`、`job.remove`、`job.admin`；
- `secret.use:<scope>`；
- `fleet.plan`、`fleet.execute`、`fleet.approve`；
- `broker.admin`。

策略要求：

- 共享 broker 默认 deny，显式授予最小 capability；单用户兼容模式可以提供清晰的宽权限 profile。
- path policy 在规范化和 symlink policy 确定后判断，不能对原始字符串做前缀匹配。
- destructive flag 包括 delete、递归覆盖、KILL、批量 mutation、扩大主机范围和放宽资源限制。
- 高风险操作使用短期 approval token；token 绑定 principal、target snapshot、operation digest、过期时间和 policy version，不能复用于不同操作。
- policy 更新只影响新请求；在途请求记录开始时的 policy digest。

审计日志记录 actor、目标 snapshot、operation digest、decision、approval、开始/结束状态和 reason code。默认不记录 secret、完整 argv、env 或输出；需要命令审计时保存经过 policy 允许的摘要或 hash。审计文件使用 0600、轮转、保留期和可选 hash chain，但不把同一 OS 用户下的本地文件描述成不可篡改安全边界。

### 3.11 远端 CPU、内存、进程与网络资源治理

磁盘之外，每个 job 还需要显式资源 envelope：

```yaml
resources:
  wall_timeout: 1h
  cpu_quota: 200%        # 2 cores
  memory_max: 2GiB
  pids_max: 256
  nofile: 1024
  max_jobs_per_host: 16
  max_jobs_per_client: 4
  nice: 5
  network_class: default
```

实现采用 capability discovery：

1. Linux 优先使用 cgroup v2 或 `systemd-run --user --scope`；
2. 无 cgroup 时使用 `setrlimit`、nice、process group 和 wall timeout 提供软限制；
3. Darwin 使用可用的 rlimit/process group 能力；
4. 无法保证的限制必须返回 `unsupported_resource_control`，不能伪装为已应用。

限制的 effective value 由 global hard cap、host policy、project request 取最严格值。项目配置不能放宽 global/host cap。OOM、timeout、PID limit、外部 SIGKILL 和主机重启必须成为不同的结构化终态。

host pressure 包括 load、可用内存、磁盘压力、运行 job 数和 broker queue。pressure 只用于排队/拒绝 rdev 管理的工作，不声称隔离同 SSH 账号直接启动的外部进程。

GPU、容器网络整形和内核级带宽限制预留扩展点，不作为首版必需能力。

### 3.12 State schema、迁移、恢复和修复

所有持久文件必须具有显式 schema，而不是依赖 Go struct 当前形状：

```json
{
  "schema_version": 3,
  "writer_version": "0.4.0",
  "record_type": "job-meta",
  "record_id": "...",
  "created_at": "...",
  "payload": {}
}
```

state root 包含 manifest，记录 schema、remote agent identity、namespace、最近成功迁移和 migration lock。规则如下：

- 读取未知 future schema 时 fail closed，不覆盖、不降级写入。
- 迁移必须持有 owner lock，按 `N -> N+1` 顺序执行，不能跨版本猜测。
- 每步采用 write-temp、fsync、rename；批量迁移前生成有界 backup/manifest。
- 迁移失败恢复旧 manifest；无法确认的数据移入 `quarantine/`，不自动删除。
- 多版本 agent 同时访问时，只有 manifest 指定的 writer 可以写；其他版本只读或明确拒绝。
- metadata 损坏不能让整个 job list 失败；单项标记 corrupt 并给出 repair 建议。

新增：

- `rdev state inspect --host HOST --json`；
- `rdev state migrate --host HOST --dry-run`；
- `rdev state repair --host HOST --dry-run`；
- `rdev state quarantine list/restore/remove`。

时间语义：持久化 RFC3339 时间和 duration，不以跨主机 wall clock 决定安全关键顺序；检测显著 clock skew 并暴露指标。job ID 的唯一性不能依赖时间戳。

### 3.13 文件传输完整性与恢复

把传输分为 `read/write object` 和 `sync tree` 两类契约。

单文件写入流程：

```text
create operation_id
-> write same-filesystem staging file
-> stream chunks with offset + digest
-> fsync staging
-> verify size/digest
-> apply mode/mtime policy
-> atomic rename
-> fsync parent
```

要求：

- 支持分块和断点续传，resume 必须绑定 operation ID、目标 identity、size 和 digest。
- 同一目标的并发写使用 target lock 或 compare-and-swap precondition，禁止静默最后写者覆盖。
- 中断 staging 文件进入受管临时区，受 retention 和 storage budget 清理。
- 默认不跟随目标 symlink；是否允许 symlink、hardlink、device、FIFO 必须显式配置。
- binary 不再依赖一个巨大 base64 JSON；流式层应支持有界 binary chunk 或明确编码开销。

目录同步先生成 manifest/diff：path、type、size、mtime、mode、digest 状态。`--delete` 只能删除计划 snapshot 中明确列出的目标，并受 capability/approval 约束。同步结果报告 copied/skipped/deleted/conflicted/failed，支持带宽、并发和磁盘预算。

首版不支持 remote A 到 remote B 的直接传输，也不承诺 owner/xattr/ACL 的全平台一致保留；这些必须列入支持矩阵。

### 3.14 流式协议、进度与背压

现有一请求一回复扩展为有序事件流：

```json
{"type":"accepted","request_id":"...","stream_id":"..."}
{"type":"data","stream_id":"...","seq":1,"channel":"stdout","data":"..."}
{"type":"progress","stream_id":"...","seq":2,"completed":10,"total":100}
{"type":"final","stream_id":"...","seq":3,"result":{}}
```

协议要求：

- `accepted/data/progress/final/error` 有明确状态机，单个 stream 只有一个 final terminal event。
- 每帧有硬上限，stream 有最大未确认字节和本地/远端 buffer budget。
- 通过 window/ack 或等价 credit 机制实现背压；慢 client 不能拖垮其他 stream。
- stdout、stderr、progress 和 control 分 lane；cancel/final 不得排在大数据后无限等待。
- client 断开后按 operation 类型选择 cancel、detach 为 job 或继续并记录 owner，不允许隐式决定。
- 丢帧、重复帧、乱序、final/cancel 竞态有确定处理规则。

首版可以继续使用 NDJSON control frame，并为 data chunk 使用 base64；如果性能不满足，再引入 length-prefixed binary frame。协议版本协商必须声明 streaming、resume、cancel 和 compression capability。

### 3.15 结构化错误模型

CLI、MCP、broker 和 remote agent 共享稳定错误 envelope：

```json
{
  "code": "transport.ambiguous_outcome",
  "category": "transport",
  "message": "human-readable summary",
  "retry": "unsafe",
  "execution_state": "possibly_executed",
  "operation_id": "...",
  "details": {},
  "suggested_action": "query operation status"
}
```

字段：

- `code`：版本化、稳定、适合程序判断；
- `category`：config/auth/transport/protocol/policy/resource/storage/remote/process/internal；
- `retry`：safe、after_backoff、after_user_action、unsafe、never；
- `execution_state`：not_sent、sent、accepted、possibly_executed、completed；
- `details`：按 code 定义 schema，禁止任意远端错误原文进入；
- `cause_chain`：有界且已脱敏。

远端命令非零退出是正常 result，不等同 transport error。未知 error code 必须仍能按 category 和 retry 退化处理。CLI exit code、MCP error 和 broker response 从同一 error code registry 投影，避免三套语义漂移。

### 3.16 `rdevd` 安装、运行和升级生命周期

目标平台使用用户级服务，不要求 root：

- macOS：launchd user agent；
- Linux：systemd user service；无 systemd 时允许显式 foreground/autostart fallback；
- socket 和 lock 位于用户私有 runtime 目录，权限 0700/0600。

生命周期：

```text
STARTING -> RECOVERING -> READY -> DRAINING -> STOPPED
                     \-> DEGRADED
```

要求：

- single-instance lock 与 socket stale recovery；
- readiness 在 config/state migration 和 policy 加载完成后才成立；
- config reload 采用 parse/validate/build/swap，失败保留旧配置；
- shutdown 先拒绝新 mutation，再 drain 有界请求，最后关闭 owned transport；
- broker crash 后从 state 恢复 operation/job，不自动重放 ambiguous mutation；
- 最后一个 client 离开后可保持轻量 daemon，但远端连接按 grace/TTL 回到 0；
- client/broker 进行协议和 feature negotiation，旧 client 不得触发新语义的危险降级。

升级采用新进程 readiness 后 socket handoff 或短暂 drain/restart；首版若不做零停机，必须明确返回维护状态并保证后台远端 job 不受影响。

### 3.17 发布、远端 agent 升级和供应链

发布产物包括 CLI、broker、四平台 remote agent、manifest、SBOM、签名和 provenance。

要求：

- CI 固定 Go/toolchain 和依赖，执行 test、race、vet、fuzz smoke、`govulncheck` 和可复现构建比较。
- manifest 记录每个二进制的 platform、version、protocol range、SHA-256 和签名。
- 本机在上传前验证内嵌/下载 agent 与 manifest；远端安装后重新验证 digest。
- upgrade 使用 host lock、staging、health check、atomic switch 和保留一个已知可用版本。
- 新 agent handshake 失败时自动回滚；不能因一个 client 较旧而反复覆盖新 agent。
- 支持 stable/beta/dev channel，但 project config 不能静默切换到更宽松或未签名 channel。
- release 生成 migration compatibility matrix 和 rollback note。

签名不能替代 SSH host identity；两者分别保护发布物来源和传输目标。

### 3.18 支持矩阵与明确非目标

目标支持矩阵：

| 能力 | Tier 1 | Tier 2/实验 | 首版非目标 |
| --- | --- | --- | --- |
| 本机 | macOS、Linux + OpenSSH | Windows/WSL | 原生 Windows ControlMaster 兼容承诺 |
| 远端 | Linux amd64/arm64、Darwin amd64/arm64 | 其他 Unix | Windows remote agent |
| 网络 | ssh alias、IPv4/IPv6、ProxyJump | 复杂 ProxyCommand | 自建 VPN/隧道管理 |
| 命令 | 非交互 argv、显式 login profile | 受限 stdin streaming | 完整 PTY、终端 resize、交互式 sudo |
| 文件 | regular file、受控 symlink policy | mode/mtime | device、FIFO、跨平台 ACL/xattr 完全一致 |
| 转发 | 复用用户现有 OpenSSH 配置 | 独立 bulk lane | SSH agent forwarding、通用端口转发管理 |

正式发布前必须为 OpenSSH、OS 和文件系统定义最低测试版本。未测试组合返回 unsupported 或 best-effort 标记，不能暗示完整支持。

### 3.19 Execution profile 与环境一致性

login shell 只能改善 PATH，不保证可重复环境。定义 `ExecutionProfile`：

```yaml
profile:
  shell_mode: login_exec   # direct | login_exec | explicit_shell
  shell_path: /bin/bash
  cwd: /workspace
  path: [/usr/local/bin, /usr/bin, /bin]
  locale: C.UTF-8
  env_allowlist: [HTTP_PROXY]
  toolchain_probe: [go, git, python3]
```

effective profile 由 global/host/project/request 合并，并生成 profile digest 写入请求和 job metadata。`rdev env inspect HOST --json` 返回 shell、PATH、HOME、locale、umask、OS/arch、磁盘、cgroup 能力和工具版本，但不返回环境中的 secret value。

规则：

- direct mode 不加载 profile，行为最可预测；login mode 明确承认用户 shell profile 是受信任代码。
- PATH、locale 和 cwd 在执行前验证，错误使用结构化 code。
- capability cache 带 TTL 和 probe version，主机或 profile 变化后失效。
- job retry 必须使用原 profile digest；profile 已变化时要求显式选择旧快照或新 profile。
- 不试图从任意交互 shell 自动复制所有环境变量到持久配置。

### 3.20 集成、混沌、fuzz 和性能认证

测试体系分层：

| 层 | 内容 |
| --- | --- |
| Unit | parser、state machine、policy、quota、retry、migration |
| Fuzz | NDJSON/frame、config、path、error envelope、manifest、CLI flags |
| Integration | 真 OpenSSH/sshd、ProxyJump、ControlMaster、agent upload、rsync |
| Fault injection | 丢包、延迟、半关闭、响应前断线、磁盘满、只读 FS、进程被杀 |
| Scale | 100 host、20 client、连接淘汰、重连风暴、百万请求 metric cardinality |
| Soak | 24h broker、job/log GC、连接反复冷热切换、资源回归 |
| Compatibility | client/broker/agent N、N-1 和 migration/rollback |

真实 SSH harness 使用隔离 sshd/container/VM 和专用测试密钥，不接触开发者生产 `~/.ssh`。网络故障通过可控 proxy 或 transport adapter 注入；测试禁止依赖随机 sleep 判断竞态，应使用 barrier/fake clock/eventually deadline。

发布 Gate 至少保存以下基线：cold/warm latency、control p95/p99、dial success、reconnect burst、RSS/FD/goroutine、managed disk、throughput、fairness 和 error distribution。性能回归超过约定预算时需要显式批准。

## 4. 已确认安全发现

严重度统计：3 High、4 Medium、5 Low；均为高置信度静态验证结果。

| ID | 严重度 | 问题 | 主要证据 | 影响 | 目标阶段 |
| --- | --- | --- | --- | --- | --- |
| SEC-001 | High | 项目配置中的 SSH `Addr` 可注入本机 ssh 选项 | [`internal/transport/conn.go:374`](../internal/transport/conn.go#L374) | `ProxyCommand` 等选项可造成本机代码执行 | Phase 1 |
| SEC-002 | High | `RemoteDir` 被插入远端 shell 程序 | [`internal/transport/conn.go:248`](../internal/transport/conn.go#L248) | 首次 probe/bootstrap 即可远端命令注入 | Phase 1 |
| SEC-003 | High | secret Store 只有全局名称，没有 host scope | [`internal/secrets/secrets.go:27`](../internal/secrets/secrets.go#L27) | 主机 A 的凭据可能被注入主机 B | Phase 2 |
| SEC-004 | Medium | JSON 序列化后才做字面脱敏 | [`internal/mcpsrv/server.go:99`](../internal/mcpsrv/server.go#L99) | 含特殊字符的秘密可绕过脱敏 | Phase 2 |
| SEC-005 | Medium | 连接先发布，随后才加载声明密钥 | [`internal/client/client.go:115`](../internal/client/client.go#L115) | 首次并发请求可能输出未脱敏秘密 | Phase 2 |
| SEC-006 | Medium | 传输失败后无差别重放非幂等请求 | [`internal/client/client.go:191`](../internal/client/client.go#L191) | 重复命令、job、追加或删除 | Phase 3（已完成） |
| SEC-007 | Medium | 远端响应帧和 stderr 缓冲无硬上限 | [`internal/transport/conn.go:714`](../internal/transport/conn.go#L714) | 恶意远端 agent 可造成本机 OOM | Phase 3（已完成） |
| SEC-008 | Low | 陈旧 job PID 复用时可能误杀无关进程 | [`cmd/rdev-agent/jobs_run.go:128`](../cmd/rdev-agent/jobs_run.go#L128) | 终止同账号的无关进程组 | Phase 4 |
| SEC-009 | Low | 超过 64 KiB 的远端密钥被静默截断并登记 | [`internal/client/client.go:525`](../internal/client/client.go#L525) | 错误凭据或不完整脱敏 | Phase 2 |
| SEC-010 | Low | 1–5 字节密钥可注入但不会脱敏 | [`internal/secrets/secrets.go:124`](../internal/secrets/secrets.go#L124) | PIN/短口令进入工具结果 | Phase 2 |
| SEC-011 | Low | job 元数据和日志使用 0755/0644 | [`cmd/rdev-agent/jobs_run.go:43`](../cmd/rdev-agent/jobs_run.go#L43) | 多用户远端可能读取 argv 和日志 | Phase 4 |
| SEC-012 | Low | 项目配置持久化跟随符号链接 | [`internal/session/session.go:241`](../internal/session/session.go#L241) | 显式保存时覆盖本机可写目标 | Phase 1 |

### 4.1 未按安全漏洞计分但仍需修复的项目

- `rsync` 本地 operand 前没有 `--`。现有参数链不足以确认本机命令执行，但前导 `-` 路径会被解释为选项。
- job ID 没有语法和目录 containment 校验。当前调用者已经拥有同一远端账号的任意命令/文件权限，因此归类为工程边界缺陷。
- caller 可指定极大 read/output/line limit，`job_wait` 还绕过普通 handler semaphore。由于调用者本就拥有远端任意执行权，归类为资源治理缺陷。
- ControlMaster socket 目录可预测，且对既有目录未验证 owner、mode 和 symlink。归类为本机 hardening。

## 5. 工程缺陷清单

| ID | 优先级 | 问题 | 风险 | 目标阶段 |
| --- | --- | --- | --- | --- |
| ENG-001 | P0 | 项目配置自动覆盖同名全局 host，没有 trust/approve | 打开恶意仓库即可改变连接目标和 bootstrap 数据 | Phase 1 |
| ENG-002 | P0 | Host 重定义保留旧 sticky env 和 secret 状态 | 状态污染和跨主机凭据混用 | Phase 2 |
| ENG-003 | P0 | context cancel 只停止本机等待，没有协议级 cancel | 远端前台命令可能继续运行 | Phase 3（已完成） |
| ENG-004 | P0 | `job_start` 先启动 supervisor 再写 metadata | 写入失败会产生不可管理的孤儿任务 | Phase 4 |
| ENG-005 | P0 | job 日志无限增长且只有手动回收 | 远端磁盘耗尽 | Phase 4 |
| ENG-006 | P1 | job ID 没有 grammar/containment 检查 | job 操作可越出 `state/jobs` | Phase 4 |
| ENG-007 | P1 | `job_wait` 不占普通并发 slot，数量无上限 | goroutine 和文件轮询耗尽 | Phase 3（已完成） |
| ENG-008 | P1 | read、output、line、request frame 缺少统一硬上限 | 本地或远端内存压力 | Phase 3（已完成） |
| ENG-009 | P1 | `doList`、`jobList` 先读取完整目录再应用 limit | 超大目录延迟和内存不可控 | Phase 4 |
| ENG-010 | P1 | 文件写入直接 `O_TRUNC`，`chmod` 错误被忽略 | 中断留下半文件，模式承诺不可靠 | Phase 4 |
| ENG-011 | P1 | host 配置保存不是原子写，既有模式不修复 | 崩溃损坏配置或保留宽权限 | Phase 1 |
| ENG-012 | P1 | CLI flag parser 静默接受未知 flag，数字错误变 0 | 拼写错误可能变成无限超时或错误行为 | Phase 6 |
| ENG-013 | P1 | `readAllStdin` 吞掉非 EOF 错误 | 不完整输入被当作成功 | Phase 6 |
| ENG-014 | P1 | CLI exec 默认无限超时，MCP 默认 60 秒 | 两个入口行为不一致 | Phase 6 |
| ENG-015 | P1 | `sync --delete` 没有强制预览或删除 manifest | 路径配置错误时扩大删除范围 | Phase 4 |
| ENG-016 | P2 | IPv6 destination 被第一次 `:` 错误切分 | IPv6 连接不可用或误解析 | Phase 6 |
| ENG-017 | P2 | `readTail` 扫描约 1 MiB 后无截断标志 | 调用者误以为拿到完整尾部 | Phase 4 |
| ENG-018 | P1 | job 没有 client/project owner | 多 Agent 可观察或操作彼此 job | Phase 5 |
| ENG-019 | P1 | 单进程连接访问后长期保持，没有 idle eviction | 百机时 ssh 进程、TCP、远端 agent 长期堆积 | Phase 4 |
| ENG-020 | P1 | 多进程只共享 OpenSSH master，不共享 rdev 会话 | 同一远端运行多个 agent 和重复状态机 | Phase 5 |
| ENG-021 | P1 | 一个客户端可占满远端全部 handler | 多 Agent 互相饥饿 | Phase 5 |
| ENG-022 | P2 | bulk traffic 与控制请求共享 TCP 资源 | 大同步任务抬高 status/cancel 延迟 | Phase 5 |
| ENG-023 | P2 | 文档将协议称为 JSON-RPC，并夸大部分模式/脱敏保证 | 用户依据错误契约做出危险假设 | Phase 6 |
| ENG-024 | P1 | 连接没有 lease、draining 和断开原因模型 | 不能安全判断何时关闭，容易泄漏或误断在途任务 | Phase 4 |
| ENG-025 | P1 | 缺少统一结构化日志、指标、追踪和诊断快照 | 无法量化连接复用、稳定性和资源演进效果 | Phase 0 |
| ENG-026 | P0 | 本机和远端受管数据没有统一磁盘预算、水位和清理契约 | job 日志或诊断数据可填满磁盘并影响远端业务 | Phase 4 |
| ENG-027 | P1 | 缺少 host inventory、批量计划、波次和部分失败模型 | 百机操作只能由上层 Agent 手写循环，重试与范围不可控 | Phase 7 |
| ENG-028 | P0 | 共享 broker 缺少 capability、policy、approval 和 audit | 一个 Agent 的权限可能被所有 client 间接使用 | Phase 5 |
| ENG-029 | P1 | job 没有 CPU、内存、PID、FD 和数量 envelope | 多 Agent 可争抢或耗尽远端计算资源 | Phase 4 |
| ENG-030 | P0 | 持久 state 没有 schema、migration、quarantine 和 repair | 升级或损坏记录可能让 job 状态不可恢复 | Phase 4 |
| ENG-031 | P1 | 文件传输缺少分块、resume、digest、原子提交和冲突语义 | 大文件中断、并发覆盖和部分同步不可安全恢复 | Phase 4 |
| ENG-032 | P1 | 一请求一回复协议缺少 streaming、progress 和背压 | 大输出和慢 client 会造成延迟、轮询或内存压力 | Phase 3（已完成） |
| ENG-033 | P0 | 错误主要依赖字符串，没有稳定 retry/execution-state 语义 | Agent 可能错误重试或误判已执行状态 | Phase 3（已完成） |
| ENG-034 | P1 | `rdevd` 的安装、single-instance、reload、drain 和升级未定义 | broker 难以作为长期用户服务可靠运行 | Phase 5 |
| ENG-035 | P1 | 发布缺少签名、SBOM、provenance、升级回滚和兼容矩阵 | 供应链和远端 agent 升级不可审计 | Phase 8 |
| ENG-036 | P1 | 缺少正式支持矩阵和非目标 | 用户会把未测试的 PTY、Windows、ACL 等行为当作承诺 | Phase 0/6 |
| ENG-037 | P1 | PATH、locale、shell 和工具链没有 ExecutionProfile | 同一命令在不同入口、重试和主机上环境不可复现 | Phase 4 |
| ENG-038 | P0 | 缺少真实 SSH 集成、混沌、fuzz、规模和长期 soak 认证 | 连接池和 broker 可能只在单测环境成立 | Phase 0/8 |

## 6. 分阶段任务计划

### Phase 0：建立安全与性能基线

目标：在修改架构前固定可重复的测试、威胁模型和观测指标。

覆盖：ENG-025、ENG-036、ENG-038，并为后续所有安全、连接和 QoS 任务提供观测基线。

| Task | 内容 | 依赖 | 交付物 | 验收条件 |
| --- | --- | --- | --- | --- |
| P0-01 | 新增 `SECURITY.md`，定义项目配置、远端 host、同账号用户和本地 Agent 的信任边界 | 无 | 安全策略文档 | 明确哪些能力是设计授权，哪些是漏洞 |
| P0-02 | 建立连接管理 benchmark/simulator | 无 | 可模拟 100 host、失败、延迟和断线的测试工具 | CI 可重复运行，不依赖生产机器 |
| P0-03 | 增加低基数 metrics registry 和安全导出接口 | 无 | pool、连接、请求、流量、job、进程和安全指标 | `status/doctor --json` 可读取稳定快照；默认不开放 TCP listener |
| P0-04 | 固化现有行为测试 | 无 | retry、cancel、job ownership、config precedence 的 characterization tests | 后续改造不会无意破坏兼容行为 |
| P0-05 | 建立统一结构化日志 API 和事件字典 | P0-01 | 稳定事件名、关联 ID、level、sink redactor、轮转策略 | 日志测试证明 argv/env/secret/原始输出不会进入 sink |
| P0-06 | 定义 trace/span 和 `rdev doctor --json` schema | P0-03、P0-05 | 请求阶段耗时和可安全分享的诊断快照 | schema 有版本；高基数和敏感字段有负向测试 |
| P0-07 | 建立本机/远端 managed storage inventory 和压测夹具 | P0-02、P0-03 | 可生成大日志、磁盘压力、损坏 metadata 和并发 GC 的测试环境 | 不依赖填满真实系统盘即可重复验证水位策略 |
| P0-08 | 建立支持矩阵、最低版本和明确 non-goals | 无 | OS/OpenSSH/网络/命令/文件能力矩阵 | 未测试能力不会以默认支持形式暴露 |
| P0-09 | 建立隔离的真实 OpenSSH/sshd/ProxyJump 集成 harness | P0-02 | 专用测试 key、sshd、jump host、故障 proxy | 不读取开发者生产 SSH 配置即可跑完整 bootstrap/exec/sync |
| P0-10 | 建立 fuzz 和 fake-clock/fault-injection 基础设施 | P0-04 | config/path/frame/error/state fuzz target 和确定性竞态工具 | CI 有短 fuzz smoke，长 fuzz 可独立运行 |

完成标准：

- [x] `go test ./...`、`go test -race ./...`、`go vet ./...` 通过（Batch A 验证，2026-08-27）。
- [x] `make check-agents` 通过四个平台构建一致性检查（Batch A 验证，2026-08-27）。
- [ ] 百机 simulator 可报告进程、连接、内存和重连峰值。
- [ ] 日志、metrics 和 doctor 快照通过 secret/cmd/path 泄露测试。
- [ ] 指标 label cardinality 在百机和高请求量下保持有界。
- [ ] storage simulator 可验证 quota、high/low watermark、retention 和磁盘不足降级。
- [x] Tier 1 支持矩阵和 non-goals 已进入 README/机器可读 snapshot，并完成 Ubuntu amd64 手工真实 SSH 基线。
- [ ] 真 SSH 集成测试与 fuzz smoke 可在 CI 重复执行。

### Phase 1：封住项目配置和 SSH bootstrap 边界

覆盖：SEC-001、SEC-002、SEC-012、ENG-001、ENG-011。

| Task | 内容 | 依赖 | 验收条件 |
| --- | --- | --- | --- |
| P1-01 | 引入 project config trust 状态，首次使用项目配置时要求显式批准路径和内容摘要 | P0-01 | 未批准项目不能覆盖全局 host 或触发连接 |
| P1-02 | 集中实现 `ValidateDestination` | P0-04 | 拒绝前导 `-`、空白、控制字符和非法 port；覆盖全部 ssh sink |
| P1-03 | 集中实现 `ValidateRemoteDir` | P0-04 | 仅接受规范安全相对路径；引号、换行、`$()`、反引号、路径穿越全部失败 |
| P1-04 | bootstrap 固定脚本化，动态值只通过参数/标准输入传递 | P1-03 | 不再把配置值插入 shell 源码 |
| P1-05 | 配置保存改为 no-follow 原子写 | P0-04 | 拒绝目录/文件 symlink；临时文件、fsync、rename；最终模式 0600 |
| P1-06 | `rsync` operand 增加 `--` 并验证 local/remote path | P1-02 | 前导 `-` 路径按文件处理或明确拒绝 |
| P1-07 | connection pool 绑定 canonical fingerprint 与 Registry generation | P1-01 | alias 更新/批准后全部 sink 不复用旧 destination、port 或 `RemoteDir` |
| P1-08 | project approval 使用 staged snapshot 和 durable transaction | P1-01、P1-05 | 任意持久化失败不改变 host、scope、state、approval 或连接代际 |
| P1-09 | bootstrap staging 排他创建并验证对象身份 | P1-04 | link/既有对象/并发/失败不能重定向写或破坏已安装 agent |

Gate：在 SEC-001、SEC-002 修复前，不开始把项目配置传播给共享 `rdevd`。

#### Batch A 实施记录（2026-08-27）

Phase 1 实现清单（已独立验收）：

- [x] P1-01：项目配置以绝对路径和精确 SHA-256 绑定批准；未批准或内容变化时只保留 global host，不能触发连接。
- [x] P1-02：`ValidateDestination` 在所有 SSH 进程创建的共享边界执行，并覆盖配置加载、运行时注册和一次性 destination。
- [x] P1-03：`ValidateRemoteDir` 只接受规范的 home-relative 安全组件，保留 `~/...` 兼容输入。
- [x] P1-04：probe、目录创建、版本检查、上传、安装和 agent 启动均使用固定 shell 程序，动态值只走位置参数或 stdin。
- [x] P1-05：配置与 trust store 使用 no-follow 打开、同目录 0600 临时文件、file fsync、原子 rename 和 directory fsync；配置目录收紧为 0700。
- [x] P1-06：rsync 在 operand 前加入 `--`；本地路径拒绝控制字符，远端路径限制为无需 remote-shell 转义的安全字符集。
- [x] P1-07：连接池以 canonical fingerprint 和单调 generation 复用；Registry 发布主动淘汰旧连接，拨号发布前后重验代际。
- [x] P1-08：批准在 durable trust commit 后从当前 live state 构造 immutable staged snapshot，再一次发布 host/scope/state/approval/connection generation；commit 前 read/marshal/write/fsync/rename/首次 dir-fsync 故障保持旧快照，rollback 任一步不可验证则进入 fatal ambiguous 状态，commit 后 backup 清理故障以 committed warning 返回并同步发布内存状态。
- [x] P1-09：bootstrap 使用密码学随机 staging 目录、排他创建与写前后 owner/type/link/inode/digest 校验；显式执行 `STAGED → VERIFIED → INSTALLING → COMMITTED`，仅对仍匹配本次 publication inode 的 target 回滚并复核旧 inode/digest，ambiguous 保留证据，commit 后清理故障返回 typed warning，首次发布不删除并发 target。

Phase 0 本批必要基线：

- [x] P0-01：根 `SECURITY.md` 已建立并成为仓库安全策略。
- [~] P0-03：已建立 schema-versioned、固定 reason 集合的低基数安全 metrics snapshot；完整 pool/request/job/process 指标和 `doctor` 导出留待后续。
- [~] P0-04：已固化 config precedence、未批准 fallback、SSH sink、bootstrap 参数化、rsync operand、no-follow、原子写和并发 writer 行为；Phase 2+ 的 retry/cancel/job ownership characterization 随对应批次补齐。
- [~] P0-05：已建立固定安全事件名、level、reason 和 hash-only target sink；完整日志轮转与全链路 redactor 留待后续。
- [x] P0-08：README 与 `rdev support` 的 schema-versioned snapshot 区分 Tier 1、build-only、依赖能力和 non-goals；OpenSSH 最低版本在真实 harness 认证前不作虚假承诺。
- [ ] P0-02、P0-06、P0-07、P0-09、P0-10：连接 simulator、完整 doctor/trace、storage fixture、隔离 sshd/ProxyJump harness 和通用 fuzz/fake-clock 基础设施明确留到后续批次，未提交空壳。

Batch A 新增的负向与竞态覆盖包括：项目配置未批准/摘要变化/错误摘要、恶意 destination、`RemoteDir` shell 元字符与 traversal、项目配置和目录 symlink、owner/mode/fd-native ACL 策略、0600/0700 修复、并发原子 writer、批准 commit/rollback/cleanup 全阶段故障、alias generation 并发切换、per-alias exec/read/write/sync lease、安全 bootstrap staging/inode 绑定，以及 rsync 前导 `-`/remote-shell 字符。后续复验修复额外断言 host A 长操作不阻塞 host B 更新、同 alias 发布等待旧 sink、Darwin ACL 查询绑定已打开 fd、Linux/macOS 普通文件检查不依赖本地化或空文件类型字符串，以及 bootstrap 发布后校验失败、rollback mv/rm/inode 故障、首次安装并发 target、commit 后 backup/rmdir 清理故障；CLI/MCP 对 committed approval 均投影为 `Approved=true` 的成功结果并附 warning。

第一轮独立验证已在隔离的 Ubuntu amd64 目录中通过真实 SSH bootstrap、exec、read/write 与 rsync，并由本地 Claude Code 通过 rdev MCP 完成一次只读远端调用；精确测试目录已清理且二次确认无目录、symlink 或进程残留。第二轮发现 GNU `stat` 空文件类型文本回归后已改为 numeric metadata + `test -f` + fd/inode 绑定。最终代码 `c9a796cc8ea57aee2afbca13671d27b360baaee5` 已由独立 review 将两个 bootstrap 顺序窗口均判定为 fixed；独立测试通过全量门禁、两个边界测试各 100 轮，以及 Ubuntu 首次 bootstrap、最小 exec 和双重清理验证。rdev 当前没有原生 `IdentityFile`/`IdentitiesOnly` 字段，测试使用隔离 SSH wrapper 注入认证参数，此能力纳入 P4-01/P6 支持验收。

独立验证同时确认：底层/MCP 可安全同步裸 `-leading-local`，但 CLI parser 不能直接表达该 operand（`./-leading-local` 可用）；当时输出被截断时 CLI 不展示、本地 context cancel 不传播到远端进程。这三项分别进入 P6-09、P3-12 和既有 P3-04；后两项现已由 Phase 3 完成，前一项仍保留在 Phase 6。

Batch A 已验收：最终代码 SHA 为 `c9a796cc8ea57aee2afbca13671d27b360baaee5`；本地与独立验证的 `gofmt`、`make clean && make all`、`go test ./...`、`go test -race ./...`、`go vet ./...` 和 `make check-agents` 全部通过，独立针对性复验也通过两个 bootstrap 边界测试各 100 轮及 Ubuntu 实机链路。此结论只验收 Phase 0–1 当前批次，不把尚缺的 P0-02/P0-06/P0-07/P0-09/P0-10 伪装成已完成。维护者审查归档见 [`docs/security/phase0-1-codex-security-review.md`](security/phase0-1-codex-security-review.md)。

### Phase 2：密钥作用域与输出边界

覆盖：SEC-003、SEC-004、SEC-005、SEC-009、SEC-010、ENG-002。

| Task | 内容 | 依赖 | 验收条件 |
| --- | --- | --- | --- |
| P2-01 | Secret key 改为 `scope + host + name` | Phase 1 | 两台主机同名 secret 永不互相解析 |
| P2-02 | host 重定义时清理或显式迁移 sticky env/secret | P2-01 | 地址或身份变化不会继承旧凭据 |
| P2-03 | 连接安全初始化状态机 | P2-01 | host-scoped secret 读取持有正确 identity lease，secret 加载完成后才发布连接；失败状态可见且不能伪装为已保护 |
| P2-04 | 在序列化前递归脱敏结构化字符串 | P2-01 | quote、backslash、newline、Unicode 在 structured/text/error 三条路径均脱敏 |
| P2-05 | 拒绝被截断的远端 secret | P2-01 | `EOF=false` 时 Store 保持不变 |
| P2-06 | 明确短 secret 策略 | P2-01 | 要么入口拒绝 `<6`，要么实现可证明的上下文脱敏；状态不能误报 |

Gate：共享 broker 不得在 secret 尚未 host-scoped 时上线。

#### Phase 2 实施记录（2026-08-28）

实现状态：

- [x] P2-01：`internal/secrets.Key` 由 registry `scope`、immutable host identity（alias、canonical fingerprint、monotonic generation）和 `name` 组成。`ResolveEnv` 只接受完整精确键；两台 host 的同名值可并存。为兼容既有 hostless MCP/local-file 调用，保留 `scope=output`，但该值只参与输出脱敏，永远不能作为远端 env 的回退来源。
- [x] P2-02 / ENG-002：地址/用户（包含在 `Addr`）、端口、`RemoteDir` 或 registry scope 变化会推进 generation、清空旧 sticky state，并由 host-change hook 关闭连接及清除该 alias 的 scoped Store 条目。配置 entry 按权威 snapshot 替换而不是 merge，删除的 env/secret 不会在 reload 后残留。仅 `force_agent_upload` 变化会重建连接和重载 secret，但不会误清非身份 sticky 配置。
- [x] P2-03：连接使用显式 `cold → initializing → ready|failed` 安全初始化状态；状态、声明数、加载数和固定失败 reason 通过 `rdev_session.connection_security` 暴露。identity/generation read lease 覆盖 state snapshot、secret 展开、请求构造、远端 I/O、递归响应/错误脱敏和返回；声明 secret 带 `declarative` provenance，通过临时 batch 全部验证后原子写入 Store，随后连接才发布，重连会刷新声明值而不覆盖 `manual` 值。失败保持 fail-closed 且同一未变化 generation 不反复拨号伪装成 ready。secret rotation/delete 使用 identity write lease；输出另持有操作起点的不可变 redaction snapshot 并与返回时 Store 合并，旧值删除/轮换不会击穿在途响应，也不跨 alias 阻塞。
- [x] P2-04：Store 对 struct、pointer/interface、slice/array 和 map key/value 做 pre-serialization recursive redaction；map key 脱敏碰撞保留所有 entry。MCP SDK 已生成 `json.RawMessage`/JSON text fallback 时先 decode 为原始字符串、递归处理、再 serialize；CLI `printJSON`、stdout/stderr client result、最终 error 和安全事件使用同一 Store redactor。远端受控 probe/stderr/raw frame 不再拼入 transport error，未知 prospective secret 的初始化/读取错误只返回固定分类。测试覆盖引号、反斜杠、换行、Unicode、嵌套 JSON、纯文本和 error。`content_b64` 是不可信元数据：编码字符串若命中注册值会清除该标志并返回脱敏文本，选择 fail closed 而不是允许 agent 借标志绕过。
- [x] P2-05：远端 secret 请求 `64 KiB + 1 byte`，接受条件包含 `EOF=true`、`Size <= 64 KiB`、实际返回内容不超过上限且为 text；恰好 64 KiB 接受，观测到额外一字节或 `EOF=false` 均拒绝，失败前不修改 Store。
- [x] P2-06：所有 Store set/batch/file/remote/inline 入口统一拒绝 `<6 bytes`；验证失败保持旧值不变，不再存在“注入成功但不会脱敏”的状态。
- [x] Phase 2 观测：security snapshot schema 升为 v2，增加固定 reason 的 secret load/reject 计数、连接安全状态 transition 计数和无标签 redaction hit 计数；事件只带固定 vocabulary 和 host target hash，不携带 secret name/value、路径、argv、远端输出或错误原文。

独立复审验收补充：

- CLI `hosts add` 与 MCP `rdev_session` 现统一调用 Registry 原生 staged transaction；host、scope、sticky state 和完整 secret declarations 先验证，`persist` 先安全写入 staged snapshot，随后在一个临界区发布 host/scope/state/generation，并且每个 alias 最多触发一次失效。普通持久化失败不会改变 live snapshot；并发 status/list 通过原子 `Inspect` 只观察提交前或提交后的完整组合。
- Registry 的单文件安全写原语不能为 global/project 两个 `hosts.json` 提供带恢复日志的崩溃一致事务，因此 `persist=true` 默认拒绝仍存在旧 scope 持久化定义的跨 scope 迁移，并在任何文件写入前返回明确的显式分步提示。Registry 额外记录每个 alias 实际存在于哪些配置文件，因而先做 live-only scope 变化、再调用 `Save` 或第二次 `ApplyHostUpdate(persist=true)` 也不能绕过保护。调用方必须先显式保存/移除旧 scope 定义，再单独持久化新 scope；旧 scope 删除失败会保留迁移保护和原文件。`persist=false` 的 scope 变化被明确为仅当前进程有效：它仍推进 identity generation、清空旧 sticky/secret declarations 并失效连接，但不改变任一文件，重启恢复磁盘定义。
- 最终只读复核发现并修复了显式两步迁移后的反向旁路：每个 alias 的持久化位置与 live-only 源现在合并为 `stable` / `live-moved` 状态机，并在 Load、`ApplyHostUpdate`、`SetScope` 和 scope 权威重写时作为一个值转换、快照和发布。旧源重写一旦提交（包括 committed cleanup warning）即同时移除 durable source 并归一化 move source；目标写入失败保留“源已删除、目标未持久化”的准确状态供安全重试，完成后任何反向 `persist=true` 都重新进入 `live-moved` 并在写入前拒绝。
- 连接池 publication 绑定单调 token，所有 published connection 的 detach、`Close` 与收尾状态发布统一经过同一边界：detach 必须命中连接实例和 publication，阻塞 `Close` 返回后还必须仍持有最新 token 且 alias 没有替代连接，才能写 `cold`/`failed`。显式 disconnect、host invalidation、陈旧池连接、transport retry、初始化失败和 client-wide close 不再有直接状态回写旁路；新连接和 `ready` 在同一锁区发布，旧收尾不能再制造 `connected=true/security=cold`，且关闭过程不持有全局 client 锁。
- MCP JSON backstop 使用 `UseNumber` 保持大整数原文/语义，同时继续递归脱敏字符串及 map key；secret 文件分类先依据 `EOF/Size` 判定截断，再判 binary，最后只对 text 检查 `Content` 长度，因此恰好 64 KiB 的 base64 payload 不再被编码长度误分类。

兼容与迁移：

- 现有 hosts.json schema 不变，声明式 `secrets: {name:path}` 继续可用；行为从“读取失败只 warning、连接仍可用”收紧为 fail-closed。修复错误路径或用带 `host` 的显式注册提供同一 exact identity 的值后可重新初始化。
- 既有 `rdev_secrets action=set` / 本地 `set_from_file` 若省略 `host` 仍成功，但现在是明确的 redaction-only registration；需要 `secret:name` 注入时必须提供 host。`list` 保留去重后的 `names` 字段，并新增无值的完整 `secrets` descriptor 列表。
- Host identity/scope 重定义不迁移旧凭据。非持久化重定义只影响当前进程；持久化跨 scope 变更必须先显式移除旧 scope 的磁盘定义。调用方需要在新 identity 下重新注册；声明式路径会在新连接初始化时重新读取。

本阶段明确未覆盖：

- Phase 3 已在其独立完成批次中补齐操作分类、安全重试、协议级 cancel/deadline、frame/output 硬上限、结构化 error envelope 与流状态机；Phase 2 的 immutable identity/generation/operation lease 继续贯穿这些路径。
- 尚未引入 Phase 4 的完整连接池、LRU/TTL、`IdentityFile`/`IdentitiesOnly` execution profile 或历史 secret rotation archive；Phase 2 只实现每 alias 的安全初始化与 operation/mutation lease。
- 多 principal capability 和共享 broker secret authority 仍属于 Phase 5；当前 scope 是单进程 registry 的 global/project scope，并以 alias-local identity 隔离。
- 显式的两步跨 scope 文件操作本身不是跨文件原子事务；进程在两步之间崩溃时需要调用方继续第二步或恢复旧文件。本阶段刻意不伪装成拥有不存在的崩溃一致性，自动 `persist=true` 路径保持 fail-closed。若未来要求单命令原子迁移，需要引入可恢复 journal/transaction manifest，并在每次 Load/写入前完成恢复。

验证覆盖包括：跨 host 同名 secret、output-only 禁止注入、Host/Scope/config override 清理、组合更新持久化失败无 live mutation、global↔project 双向持久化迁移拒绝、两步迁移后反向拒绝与连续往返、账本状态归一化、目标优先/源优先顺序、源/目标未提交失败与 committed warning、重启重载旧定义与旧 secret declaration、live-only scope 语义、`SetScope + Save`/第二次 persist 旁路、源 scope 删除失败和目标 writer 不可达、并发不可见半状态、CLI/MCP 共享声明验证、并发首请求、初始化失败与状态可见、显式 disconnect 及 transport retry 的旧连接慢 `Close`/新 `ready` 确定性时序、跨 host 不阻塞、连接安全状态与 metrics 一致性、初始化期间 alias redefinition、请求构造/响应脱敏/secret rotation lease、特殊字符及大整数的 structured/text/error/CLI 路径、64 KiB text/binary 与超一字节边界、短值、Store 不变性、低基数 metrics/event 以及 race。完整门禁结果记录在本次提交说明中。

### Phase 3：请求语义、取消和资源硬限制

覆盖：SEC-006、SEC-007、ENG-003、ENG-007、ENG-008、ENG-032、ENG-033。

| Task | 内容 | 依赖 | 验收条件 |
| --- | --- | --- | --- |
| [x] P3-01 | 为每个 op 声明 `read-only/idempotent/mutating` | P0-04 | 自动重试仅适用于安全类别 |
| [x] P3-02 | 增加稳定 operation ID 和 agent 端有界去重 | P3-01 | “执行后响应丢失”故障注入不会重复 mutation |
| [x] P3-03 | 引入 `ambiguous_outcome` | P3-01 | 无法证明未执行时不谎报失败或重试成功 |
| [x] P3-04 | 增加协议级 cancel/deadline | P3-01 | 本地取消能终止对应远端前台进程组，不影响其他请求 |
| [x] P3-05 | 限制 NDJSON request/response frame | P0-03 | 超限在常量内存内失败并关闭坏连接 |
| [x] P3-06 | stderr 改固定 ring buffer | P0-03 | 长期连接内存不随 stderr 总量增长 |
| [x] P3-07 | 为 read/output/line/wait/queue 定义绝对上限 | P0-01 | 任意协议字段不能绕过上限造成大分配 |
| [x] P3-08 | 单独限制 job wait watchers | P3-07 | 同 job 多个 waiter 可 fan-out，goroutine 数有界 |
| [x] P3-09 | 建立共享 error code registry 和 envelope | P3-01 | CLI/MCP/broker/agent 对 retry 和 execution_state 使用同一语义 |
| [x] P3-10 | 定义 accepted/data/progress/final 流状态机和 feature negotiation | P3-05、P3-09 | 单 stream 只有一个 terminal event，N/N-1 能协商是否支持 streaming |
| [x] P3-11 | 实现 frame budget、stream window/credit 和慢 client 策略 | P3-10 | 慢 stream 不造成其他 stream 饥饿或无界缓冲 |
| [x] P3-12 | CLI/MCP 明确投影输出截断状态与原始字节计数 | P3-07 | 发生截断时人和 Agent 都不会误判为完整输出 |
| [x] P3-13 | 将 cancel、final、disconnect 和 detach 竞态纳入协议测试 | P3-04、P3-10 | 每种竞态得到确定且可恢复的 terminal state |

Phase 3 完成记录：

- `internal/proto` 是 operation descriptor、error code、execution state、feature、stream event 和 hard limit 的唯一注册来源。client、agent、CLI 和 MCP 都消费同一组类型；未知 operation 和未知安全语义 fail closed。
- client 为每个逻辑调用固定 caller/operation ID、适用操作的 deadline 和请求摘要。read-only/idempotent 只按注册策略受控重试；mutation 在 agent 内存去重仍能证明相同请求时复用 accepted/final，跨新 agent、重启或记录淘汰则返回 `ambiguous_outcome`。同 operation ID 的 op/digest 冲突明确拒绝。
- agent 的去重表同时受 TTL、条目容量和 64 MiB 结果字节预算约束，运行中 accepted 记录不被淘汰；超出结果预算的 mutation 保留 compact ambiguous tombstone，不能退化成重新执行。该表是进程内故障窗口保护，不宣称跨重启 exactly-once；durable mutation journal 不借本阶段提前实现。
- 前台命令拥有独立进程组。只有 registry 声明 `DisconnectCancel` 的前台操作接收 wire cancel/deadline，cancel target 和 early tombstone 均绑定 target op；已发送的独立 mutation 在 caller context 结束时返回 `possibly_executed`/`ambiguous_outcome`。host 的 terminal commit 与 context cancel 在同一连接锁和 pending 状态机内线性化：已 commit terminal 必须返回，cancel 先赢则先标记/移除 pending、再于锁外排队 cancel，迟到 success 作为协议错误而不伪装成 canceled。cancel/deadline/attached disconnect 精确终止目标组，detach job 保持独立；TERM 后的 leader 即使先退出也不会终止 escalation，agent 在保留 leader 未 reap、从而阻止 PID/PGID 复用的窗口内，对原 PGID 执行 grace 后检查/KILL。控制请求不进入普通队列。状态机执行 `accepted → progress/data* → final`，并在取消、终态、断连和 detach 竞态下保持唯一 terminal。
- request/response NDJSON frame 使用共同 8 MiB 绝对上限；无换行超帧只读取 `limit+1` 即关闭污染连接。stderr 使用固定 64 KiB ring，bootstrap 辅助 SSH 输出也有固定保留上限。read/output/line/wait/watcher/queue/window 参数经防负数和防溢出的绝对上限校验，调用方只能在硬上限内选择。
- `job_wait` 使用独立 worker/queue 和共享 watcher hub；相同 job fan-out 一个观察循环，waiter/watcher 都有上限，取消会解除订阅并在无人使用时回收。目录和 job list 改为增量有界选择，避免为列表预载全部 metadata。
- streaming 通过 N/N-1 版本区间与 feature 交集协商。两端每条连接使用一个固定 writer loop、有界 control/data 队列、control 优先级与 queued+in-flight 总 frame budget；底层 write 超时会唤醒全部等待者，不按 write 创建 goroutine。真实 Linux pipe 证明 `close` 同一 fd 不能可靠中断已阻塞的 `read`/`write`，因此 agent 在 watchdog 失败后先做 bounded attached cancellation/worker drain，再走明确进程退出路径，由进程退出关闭 SSH channel；detached supervisor 不受影响。exec data chunk 在进程仍运行时直接发出并受 stream credit/输出预算约束；慢 data consumer 只能造成带账本的丢弃或连接 teardown，不能无限阻塞 terminal/cancel。v3 final/error 必须匹配 request/operation ID、terminal 位、合法非空 execution state、OK/error 组合和嵌套结果元数据；重复 terminal 或 cancel 后 success 会作为协议错误关闭连接。legacy unary 只由协商版本选择，不再按 `Type==""` 猜测。
- rsync stdout/stderr 不再使用无界 builder；两路持续 drain、各自有 retention cap，并返回 original/retained/dropped/truncated 与二进制 base64 标记。exec/read/rsync 的 binary wire value 在 client 端先 decode、对 raw bytes 脱敏、再 lossless encode，CLI 和官方 Go MCP SDK 投影同一安全账本。普通业务错误经 agent 的单一 typed mapping 边界转换：参数/资源、object not found、process start/state 分别使用稳定 registry code/category/retry/execution state，只有未知错误映射为 `internal.failure`，对外消息不含路径、argv 或 raw errno。
- 新增的观测词汇是固定低基数枚举；secret、argv、stdout/stderr 和远端路径不进入 label。测试使用临时目录、合成数据、fake deadline、blocking writer、真实 OS pipe/child process 和 deterministic barrier，并覆盖响应丢失三种操作分类、restart/eviction/conflict、leader 早退且子进程忽略 TERM、cancel/deadline/disconnect、terminal-before-cancel-lock、cancel-before-terminal、terminal 通知与 ctx 同时 ready、cancel ack/final 迟到、初始写与 data 写阻塞、精确 cancel 优先级、慢读 teardown、rsync Unicode/NUL/非 UTF-8 与无限输出、typed error 全链路投影、malformed/重复/cancel-to-success terminal、watcher fan-out/queue、N/N-1 以及官方 MCP SDK 元数据。仲裁排列普通/race 各执行 1000 次；真实 blocked-pipe 进程 teardown 普通/race 各执行 100 次。

兼容与残余边界：协议 3 的最低兼容版本是 2；v2 共同操作保留一元 fallback，但只有双方协商到对应 feature 才启用 v3 cancel、streaming、dedupe 和结构化截断。writer 的固定预算保证等待者返回，并在 agent 端经 bounded cleanup 后退出污染的 serving process；它不保证向已经停止读取的 peer 成功交付 control frame，也不保证本机 ssh 在其本地 stdout 未被 drain 时同步退出。标准 MCP progress 仍依赖调用方提供 progress token，未提供时 MCP 返回最终聚合结果而不伪造实时通知。rsync 截断账本描述本机保留内容，不提供被丢弃字节的持久归档。Phase 4 的百机连接池、idle TTL、detached job 磁盘预算、持久去重/事务日志和 job durable ownership 未在本阶段扩大范围。

### Phase 4：单进程百机 Connection Manager 与 job 耐久性

覆盖：SEC-008、SEC-011、ENG-004 至 ENG-010、ENG-015、ENG-017、ENG-019、ENG-024、ENG-026、ENG-029 至 ENG-031、ENG-037。

| Task | 内容 | 依赖 | 验收条件 |
| --- | --- | --- | --- |
| P4-01 | 实现 canonical connection key | Phase 1 | 相同身份可合并；不同 user/key/proxy/state 不误复用 |
| P4-02 | 实现 COLD/DIALING/WARM/BACKOFF/EVICTING 状态机 | P0-03、P4-01 | 状态转换和错误原因可观测 |
| P4-03 | 实现 connection lease、inflight/queue 计数和 `DRAINING` 竞态控制 | P4-02、P3-04 | 有 lease/inflight/queue 时绝不关闭；新请求与 eviction 竞态可重复测试 |
| P4-03A | 增加 `max_warm_hosts`、idle TTL、last-client grace 和 LRU | P4-03 | 顺序访问 100 host 后 warm 数不超过配置值；无 client 后按 grace 回收 |
| P4-03B | 实现 graceful close 和 ControlMaster ownership | P4-03 | 只关闭自己拥有的 master；共享 master 不被误杀；disconnect reason 完整记录 |
| P4-04 | 全局 dial semaphore、singleflight、backoff+jitter | P4-02 | 100 host 同时恢复时并发拨号不超过上限，无重连风暴 |
| P4-05 | job 创建事务化 | Phase 3 | metadata 失败时 supervisor 被终止并回收，不留下孤儿 |
| P4-06 | job 使用不可复用的进程身份 | P4-05 | PID start identity 不匹配时拒绝 signal |
| P4-07 | job ID grammar 和目录 containment | P4-05 | 所有 job 路径必须位于 `state/jobs/<validated-id>` |
| P4-08 | job 文件权限收紧和 owner 验证 | P4-05 | 目录 0700，文件 0600，既有宽权限会修复或拒绝 |
| P4-09 | 定义 local、remote state 和 per-job storage policy schema | P0-07、P4-05 | 支持预算、保留期、高低水位、最小剩余空间和硬上限；项目配置不能放宽硬上限 |
| P4-09A | supervisor 改为有界 stdout/stderr sink | P4-09 | 达到上限继续 drain；按策略保留头部或尾部；状态明确报告 dropped/truncated bytes |
| P4-09B | 实现 owner-safe、lock-safe 的增量 GC | P4-09 | 只删除受管 state；运行中 job 控制记录和未知文件永不自动删除 |
| P4-09C | 实现 storage status/gc dry-run/doctor 接口 | P4-09B、P0-06 | dry-run 与实际候选一致；每次 GC 有删除和时间预算 |
| P4-09D | 接入 storage metrics、pressure event 和告警 | P4-09A、P4-09B、P0-03 | quota hit、丢弃字节、GC 结果、free/budget bytes 可观测且标签有界 |
| P4-10 | 目录 listing 改增量/有界策略 | Phase 3 | 大目录不会先完整载入全部 metadata |
| P4-11 | 文件写入改原子模式，append 另行定义 | Phase 3 | 覆盖写中断仍保留旧文件；chmod/fsync 错误返回调用者 |
| P4-12 | `sync --delete` 增加 manifest/dry-run gate | Phase 1 | 默认不能无预览执行大范围删除 |
| P4-13 | tail 返回截断状态 | Phase 3 | 超过扫描窗口时调用者明确看到 `truncated=true` |
| P4-14 | 为 state root 和所有 record 增加 schema/version/manifest | P0-04、P4-05 | 未知 future schema fail closed，损坏单项不拖垮全局 |
| P4-15 | 实现 migration lock、逐版本原子迁移、backup 和 quarantine | P4-14 | 迁移中崩溃可恢复，旧 agent 不能覆盖新 schema |
| P4-16 | 实现 state inspect/migrate/repair dry-run | P4-15、P0-06 | repair 不删除未知数据，所有变更前可预览 |
| P4-17 | 实现 remote resource capability probe 和 effective policy | P0-08、P4-01 | cgroup/rlimit 能力明确，无法保证时返回 unsupported |
| P4-18 | 为 job 应用 CPU/memory/PID/FD/wall/job-count envelope | P4-17、P4-05 | 项目不能放宽 hard cap；OOM/timeout/PID limit 有独立终态 |
| P4-19 | 定义并实现 ExecutionProfile 与 profile digest | P4-01、P0-08 | PATH/shell/locale/cwd 可检查，retry 使用相同 profile digest |
| P4-20 | 实现 `rdev env inspect` 和 capability cache 失效 | P4-19、P0-06 | 输出不含 secret，host/profile 变化会失效旧 probe |
| P4-21 | 单文件传输改为 chunk/resume/digest/staging/atomic commit | P3-10、P4-11、P4-09 | 中断可安全 resume，完成前目标不可见，digest 不符不提交 |
| P4-22 | 目录 sync 增加 immutable manifest、冲突和 symlink policy | P4-21、P4-12 | delete 只作用于计划 snapshot，并发覆盖返回 conflict |

P4-01–P4-03 首批实现记录见 [`docs/phase4-connection-manager.md`](phase4-connection-manager.md)。

百机 Gate：

- [ ] 配置 100 host、未使用时保持 0 个 SSH 连接。
- [ ] 顺序访问 100 host 后 warm host 不超过 16（或配置值）。
- [ ] 所有本地 client 退出后，非 pinned 连接在 grace + idle TTL 内回到 0。
- [ ] 后台 job 独立运行时不持有 transport lease，断开后仍可重新发现。
- [ ] 在途 exec、sync、bootstrap 和 transaction 不会被 idle sweep 中断。
- [ ] 并发拨号不超过 6（或配置值）。
- [ ] 单台失败不会阻塞其他 host。
- [ ] 网络整体恢复时无 thundering herd。
- [ ] 每次断开都有 reason、idle age、lifetime 和 drain duration 指标。
- [ ] managed storage 达到 high watermark 后能清理到 low watermark，不发生阈值抖动。
- [ ] 低于 `min_free_bytes` 时拒绝新持久 job，但现有 job 仍可查询、停止和回收。
- [ ] 单 job stdout/stderr 超限不会阻塞子进程，并明确报告截断与丢弃字节。
- [ ] 自动 GC 不删除运行中 job 的控制记录、未知文件或用户业务文件。
- [ ] state N→N+1 迁移、崩溃恢复、future schema 拒绝和 quarantine 测试通过。
- [ ] CPU/memory/PID/FD 限制在支持平台实际生效；不支持平台明确返回 unsupported。
- [ ] PATH、locale、shell 和 cwd 的 effective profile 可检查并写入 job metadata。
- [ ] 大文件传输中断可续传，digest 错误和并发覆盖不会替换目标。
- [ ] race、故障注入和 30 分钟 soak test 通过。

### Phase 5：多 Agent 共享 `rdevd` 与 QoS

覆盖：ENG-018、ENG-020、ENG-021、ENG-022、ENG-028、ENG-034。

| Task | 内容 | 依赖 | 验收条件 |
| --- | --- | --- | --- |
| P5-01 | 定义本地 broker protocol 和版本协商 | Phase 3 | client/broker 版本不兼容时明确失败 |
| P5-02 | 实现 0600 Unix socket、single-instance lock 和 peer identity | P5-01 | 其他 OS 用户不能调用 broker |
| P5-03 | 把 Connection Manager 和 secret Store 移入 `rdevd` | Phase 2、Phase 4 | 多 client 指向同一 host 时只有一个基础远端 agent 会话 |
| P5-04 | 引入 `client_id/project_id` | P5-03 | 请求、secret、job 都有明确 owner/scope |
| P5-05 | broker 断开和 client 取消语义 | P3-04、P5-04 | 一个 client 退出不关闭共享 transport，不取消其他 client 请求 |
| P5-06 | 每 host 和每 client 配额 | P5-04 | 单 client 不能占满全部 handler/queue |
| P5-07 | weighted fair queue | P5-06 | 持续高负载 client 不会饿死其他 client |
| P5-08 | control/exec/bulk lane | P5-03 | bulk 期间 control latency 保持在无 bulk 基线的 2 倍以内 |
| P5-09 | job wait 合并订阅 | P5-04 | 同 job 的 N 个 watcher 只产生一份远端观察工作 |
| P5-10 | broker 生命周期与升级 | P5-03 | broker 重启后后台 job 仍可重新发现和管理 |
| P5-11 | broker client lease 和 last-client grace | P4-03、P5-05 | client 异常退出会释放 lease；最后一个 client 退出后连接按策略回收 |
| P5-12 | 实现 principal/capability/policy decision point | P5-02、P5-04、P0-01 | 共享模式默认 deny；每个请求在排队前得到稳定 decision |
| P5-13 | 实现 destructive risk flag 和 digest-bound approval token | P5-12 | approval 不能换 target、operation、principal 或过期复用 |
| P5-14 | 实现低敏感度审计事件、轮转和查询 | P5-12、P0-05 | decision/approval/result 可追溯且不记录 secret/原始输出 |
| P5-15 | 实现 launchd/systemd user service、readiness 和 single-instance recovery | P5-03 | stale socket 可恢复，READY 前不接受请求，其他 OS 用户不能连接 |
| P5-16 | 实现 config parse/validate/swap reload 和 bounded drain shutdown | P5-15 | reload 失败保留旧配置；shutdown 不重放或丢失 mutation 状态 |

多 Agent Gate：

- [ ] 20 个本地 client 同时访问一台 host，只有一个基础 transport/agent 会话。
- [ ] 每 client 的配额和取消互不影响。
- [ ] 长 exec、job wait、status 和 sync 混合负载下没有饥饿。
- [ ] bulk lane 空闲后按 TTL 自动退出。
- [ ] broker 崩溃重启后不会重复执行 mutation。
- [ ] 连接状态、client 配额、queue wait、lane 流量和 eviction reason 可通过 `status/doctor` 观察。
- [ ] 未授权 client 无法使用 host、secret、job 或 Fleet capability。
- [ ] destructive approval 绑定精确目标 snapshot 和 operation digest。
- [ ] broker reload、升级和异常退出不影响已脱离 SSH 的后台 job。

### Phase 6：CLI、兼容性、文档和发布收口

覆盖：ENG-012、ENG-013、ENG-014、ENG-016、ENG-023、ENG-036。

| Task | 内容 | 依赖 | 验收条件 |
| --- | --- | --- | --- |
| P6-01 | 替换或严格化 CLI flag parser | Phase 5 | 未知 flag、非法数字、重复冲突参数明确失败 |
| P6-02 | 修复 stdin 错误传播 | 无 | 非 EOF 错误返回非零状态且不执行部分写入 |
| P6-03 | 统一 CLI/MCP timeout 契约 | Phase 3 | 默认值、0 和显式无限的含义一致并有文档 |
| P6-04 | 使用 `net.SplitHostPort` 等方式支持 IPv6 | P4-01 | IPv4、IPv6、alias、user@host 全部覆盖 |
| P6-05 | 修正文档协议名称和能力保证 | 全部 | README 与实际 wire protocol、权限、脱敏和连接语义一致 |
| P6-06 | 在线依赖审计和发布检查 | 全部 | `govulncheck`/依赖审计、SBOM、构建 provenance 纳入 release gate |
| P6-07 | 把支持矩阵与 runtime capability 投影到 CLI/MCP | P0-08、P4-17 | unsupported/experimental 能力在调用前可发现，不靠运行失败猜测 |
| P6-08 | 为错误 code、config、state 和 protocol 发布兼容文档 | P3-09、P4-14 | N/N-1 行为、迁移和 breaking change 有机器可读版本说明 |
| P6-09 | CLI parser 支持 `--` 后的裸 leading-dash operand | P6-01 | `sync ... -- -leading-local remote` 与 MCP/底层行为一致 |

### Phase 7：Fleet inventory 与安全批量编排

覆盖：ENG-027。只有 Phase 5 的 policy/ownership 和 Phase 3 的幂等/错误语义完成后才能开始。

| Task | 内容 | 依赖 | 验收条件 |
| --- | --- | --- | --- |
| P7-01 | 定义 immutable HostID、alias、labels 和 inventory schema | P4-01、P4-14 | alias 变化不改变 HostID；selector 可确定性解析 |
| P7-02 | 实现 selector preview 和 immutable target snapshot | P7-01、P5-12 | 执行中 inventory 变化不扩大计划目标 |
| P7-03 | 定义 FleetPlan/HostRun 持久状态机 | P7-02、P3-02、P4-14 | broker 重启可恢复，成功 host 不重复执行 mutation |
| P7-04 | 实现 max_parallel、waves、canary 和 pause/resume | P7-03、P5-07 | 并发和波次严格受限，暂停后不启动新 host |
| P7-05 | 实现 max_failures/ratio 和剩余目标取消策略 | P7-04、P3-09 | 达阈值后按策略 pause/cancel，ambiguous 独立统计 |
| P7-06 | 实现 batch result aggregation 和 retry subset | P7-05 | success/failed/skipped/canceled/ambiguous/unreachable 不丢失 |
| P7-07 | 将 Fleet capability、approval、audit 和 storage policy 接入 | P5-13、P5-14、P7-03 | `all`/大范围/destructive 计划必须显式批准并可审计 |

Fleet Gate：

- [ ] selector 预览与实际 snapshot 完全一致。
- [ ] canary 失败时不会进入下一 wave。
- [ ] 100 host 部分失败、broker 重启和 cancel 后状态可恢复。
- [ ] retry 只作用于选定失败子集，已成功 mutation 不重复执行。
- [ ] 空 selector 和超阈值目标不能静默执行。

### Phase 8：发布、供应链与生产认证

覆盖：ENG-035、ENG-038，并作为正式稳定版发布 Gate。

| Task | 内容 | 依赖 | 验收条件 |
| --- | --- | --- | --- |
| P8-01 | 生成签名 manifest、SBOM 和 build provenance | Phase 6 | 所有 CLI/broker/agent artifact 可验证来源和 digest |
| P8-02 | 实现 agent upgrade lock、staging、health check 和 rollback | P4-15、P5-16 | 新 agent handshake 失败自动回滚，旧 client 不反复覆盖 |
| P8-03 | 建立 stable/beta/dev channel policy | P8-01、P5-12 | project 配置不能静默使用未签名或更宽松 channel |
| P8-04 | 将真 SSH、ProxyJump、故障注入和 fuzz 纳入 CI/release | P0-09、P0-10 | release 前覆盖主要 transport 和协议失败模式 |
| P8-05 | 执行 100 host/20 client scale 和 24h soak | Phase 7 | RSS/FD/goroutine/ssh/storage 回归稳定，无饥饿和重连风暴 |
| P8-06 | 维护 N/N-1 client/broker/agent/migration/rollback matrix | P6-08、P8-02 | 所有支持组合自动验证，unsupported 组合明确拒绝 |

Production Gate：

- [ ] 安全 High/Medium finding 已关闭或有明确接受记录。
- [ ] Tier 1 支持矩阵全部通过真实环境测试。
- [ ] artifact 签名、SBOM、provenance 和漏洞审计齐全。
- [ ] 24h soak 后资源回归稳定，磁盘和日志均在预算内。
- [ ] upgrade、rollback、broker crash、network recovery 和 state migration 演练通过。

## 7. 跨阶段测试矩阵

### 7.1 安全边界

- 恶意项目配置：前导 `-`、shell 元字符、换行、symlink、同名 host 覆盖。
- secret：跨 host 同名、特殊字符、短值、超大文件、初始化并发、错误输出。
- retry：执行前断线、执行后响应前断线、响应部分写入、broker 重启。
- filesystem：symlink、权限过宽、磁盘满、rename/fsync 失败。

### 7.2 连接与故障

- 100 host 冷启动、顺序访问、并发访问、全网失败、分批恢复。
- DNS 失败、认证失败、host key 变化、ProxyJump 失败、ControlMaster 消失。
- 同 host 多 alias、不同 user、不同 identity、不同 state namespace。
- broker/client/ssh/remote agent 分别崩溃和重启。
- idle sweep 与新请求同时发生，验证 lease 和 `DRAINING` 不误断请求。
- 最后一个 client 正常退出、崩溃、网络隔离，验证 grace 和 lease 自动释放。
- 只有后台 job 存活时关闭 transport，再连接后重新发现和管理 job。
- shared/owned ControlMaster 两种模式下验证不会误杀其他进程的连接。

### 7.3 公平性和负载

- 一个 client 持续提交长 exec，另一个 client 执行 status/cancel。
- 多个 job wait 观察同一 job。
- 大文件 sync 与短控制请求并发。
- 大 stdout、stderr、无换行帧、超大目录和超长日志。

### 7.4 兼容性

- Linux amd64/arm64、Darwin amd64/arm64 agent 构建。
- OpenSSH alias、ProxyJump、IPv4、IPv6。
- 新旧 broker/client/remote agent 协议版本组合。

### 7.5 可观测性与隐私

- 每个连接状态转换都有唯一原因和成对计数，不出现负数 lease/inflight。
- dial、queue、remote execution、response 四段耗时可关联到同一 trace。
- 100 host、百万请求模拟下 metric series 数保持在预算内，不随 request/path/command 线性增长。
- argv、stdin、stdout、stderr、env、secret、认证材料和敏感路径不会出现在日志、metric label、trace attribute 或 doctor 快照。
- logger/exporter 故障不能阻塞请求关键路径；丢弃日志时有有界计数。
- 日志轮转和 retention 不会无限占用本机磁盘。

### 7.6 存储配额与清理

- 单 job stdout、stderr 分别达到软限和硬限，验证所有 `on_log_limit` 策略。
- running、exited、unknown、orphaned、损坏 metadata 和未知目录混合时，GC 只删除允许的对象。
- high watermark 触发后清理到 low watermark，持续写入时不会频繁启停 GC。
- 文件系统低于 `min_free_bytes` 时，新持久 job 被拒绝，但 status、stop、logs 和 gc 仍可执行。
- 多 agent 同时 GC、job exit 与 `job_rm` 并发，不重复计算 freed bytes，不复活半个 job。
- GC 过程中进程崩溃，重启后没有半删除 metadata、悬空 lock 或越出 state root 的删除。
- dry-run 候选、预计释放空间与紧接着的实际 GC 在允许的并发误差内一致。
- 本机日志、trace spool 和 diagnostics history 达到预算后轮转，不影响 broker 请求路径。
- 任何项目级配置都不能启用 unlimited 或把硬预算提高到 global policy 以上。

### 7.7 Policy、审批、审计与 Fleet

- 无 capability、过期 approval、错误 principal、错误 host snapshot 和 operation digest 不一致全部拒绝。
- path policy 在 symlink、`..`、大小写和平台分隔符规范化后仍不能越界。
- policy reload 与在途请求并发时，请求绑定开始时的 policy digest。
- selector 解析过程中 inventory 变化，实际目标仍与 preview snapshot 一致。
- canary、wave、failure threshold、pause/resume、cancel 和 retry subset 的状态机穷举测试。
- broker 在每个 wave 边界崩溃并恢复，已成功 mutation 不重复执行。
- 审计中不出现 secret、env value、完整 argv 或输出；decision、approval 和结果可关联。

### 7.8 State、资源、环境、传输与流式协议

- state schema N、N+1、future N+2、损坏记录、迁移中断、backup 恢复和 quarantine。
- 两个不同版本 writer 竞争 migration lock，旧 writer 不得覆盖新 schema。
- cgroup/rlimit 支持和不支持路径；CPU、memory、PID、FD、wall/job-count 达限终态准确。
- ExecutionProfile digest 在前台、job、retry 和恢复后保持一致；secret 不进入 profile inspect。
- 单文件 chunk resume、digest mismatch、staging 崩溃、并发 CAS conflict 和磁盘不足。
- 目录 manifest 在源变化、目标变化、symlink、delete 和并发 sync 下保持 snapshot 语义。
- streaming frame 分割、乱序、重复、丢失、慢 client、window exhaustion 和 cancel/final 竞态。
- error registry 未知 code、N/N-1 code 和 retry/execution_state 投影保持向后兼容。

### 7.9 Broker 生命周期、供应链与发布认证

- stale socket、重复启动、READY 前请求、reload 失败、bounded drain 超时和进程 SIGKILL。
- launchd/systemd 用户服务安装、升级、卸载和 state/log 保留策略。
- artifact digest/signature/SBOM/provenance 验证失败全部 fail closed。
- agent staging、health check、atomic switch、rollback 和多 client 版本竞争。
- Tier 1 每个 OS/OpenSSH 组合运行真 SSH 集成套件。
- stable/beta/dev channel policy 以及未签名 dev artifact 的显式 opt-in。
- 100 host/20 client/24h soak 保存资源、延迟、公平性和错误分布报告。

## 8. Definition of Done

任何阶段只有同时满足以下条件才算完成：

- 代码、单元测试、race test 和故障注入测试已合入。
- 新增安全不变量有负向测试，而不只有正常路径测试。
- 指标能够证明资源和延迟目标，而不是只依赖代码推断。
- 所有连接创建、复用、健康失败和断开都有稳定 reason code，并能由 `rdev doctor` 汇总。
- 可观测性 sink 通过敏感数据负向测试和 label cardinality 测试。
- 本机及远端受管存储具有显式预算、hard cap、high/low watermark、retention 和可审计 GC reason。
- 所有日志截断和自动删除对调用者可见，不静默丢数据。
- 协议错误使用稳定 code、retry 和 execution_state，不要求调用者解析字符串。
- 持久 state 有 schema、migration、future-version 拒绝、quarantine 和 dry-run repair。
- 支持的远端资源限制有真实生效测试；不支持能力明确标记 unsupported。
- 文件传输在完成 digest 验证和原子提交前不会暴露部分目标。
- 共享 broker 的 capability、approval 和 audit 通过负向权限测试。
- Fleet 执行绑定不可变 target snapshot，并能从 broker 崩溃中恢复。
- Tier 1 支持矩阵、non-goals 和 N/N-1 兼容矩阵已发布。
- README、协议说明和配置示例同步更新。
- 不引入明文 secret 日志、项目配置隐式信任或新的 shell 字符串拼接。
- 工作树通过 `go test ./...`、`go test -race ./...`、`go vet ./...` 和 `make check-agents`。

## 9. 建议的近期执行批次

建议按以下批次推进，不能为了尽快做 Fleet 而跨过安全和协议 Gate：

| 批次 | 阶段 | 目标 |
| --- | --- | --- |
| A | Phase 0–1 | 固定真实测试和支持边界，关闭恶意项目配置触发的本机/远端代码执行 |
| B | Phase 2–3 | 完成 host-scoped secret、结构化错误、幂等、cancel、streaming 和资源硬上限 |
| C | Phase 4 | 完成百机 Connection Manager、state migration、job/resource/storage 和可靠文件传输 |
| D | Phase 5 | 上线受 policy/ownership 保护的共享 `rdevd`、公平队列和服务生命周期 |
| E | Phase 6–7 | 收口 CLI/支持矩阵，并增加可恢复、安全审批的 Fleet 编排 |
| F | Phase 8 | 完成供应链、升级回滚、真实兼容矩阵和生产规模认证 |

`rdevd` 应在项目配置、secret 和请求语义稳定后开始；Fleet 必须在 broker policy、operation ID 和持久状态机完成后开始。
