# rdev — 远程开发环境代理工具

给本地 Claude Code 用的远程执行代理。用 MCP 结构化工具替代手写 SSH 命令。

## 为什么做这个

在一次真实的远程调试会话里（SWE-bench 链路排查，约 40 轮 SSH 交互），失败的原因几乎全部来自 SSH 交互本身，而不是远程环境：

| 类别 | 实际报错 | 根因 |
|---|---|---|
| 引号地狱 | `tr: extra operand '"'`<br>`cut: '"': No such file or directory`<br>`no such column: $.score`<br>中文变成 `?????` | 命令穿过 `ssh` → `bash -lc` → `sqlite3` 三层解析，每层都要转义 |
| PATH 不一致 | `uv: command not found` | 非登录 shell 不加载 `~/.bashrc`，而 uv 在 `~/.local/bin` |
| 长任务 | 工具 120s 超时 → `nohup` + 轮询；中途 kill 丢了整个结果库 | 没有作业模型 |
| 进程管理 | `pkill` 模式没匹配上主进程；`pgrep` 把自己也列出来 | 靠字符串匹配找进程 |
| 传文件 | 反复 heredoc 写 `/tmp/q.sql`、`/tmp/chk.py` | 没有文件原语 |
| 无状态 | 每次都要 `cd ~/myproject` | 连接不持久 |
| 输出爆炸 | 每条命令手动接 `tail`/`grep` | 没有输出预算 |
| 凭据泄露 | token 明文进了对话记录 | 无脱敏 |

**根本矛盾：把结构化意图压成一个 shell 字符串，再让远端重新解析它。**
只要还在拼字符串，包装得再漂亮也会继续踩。

## 快速开始

```bash
make all                                       # 需要 Go 1.25+

# 注册进 Claude Code（工具本身全局可用）
claude mcp add rdev --scope user -- $PWD/bin/rdev serve

# 在你的项目目录下注册开发机（默认 project scope）
cd ~/works/myproject
~/works/rdev/bin/rdev hosts add dev user@1.2.3.4 -port 36000 -cwd '~/myproject' -save

# 首次连接自动上传 agent，无需在远端做任何准备
~/works/rdev/bin/rdev ping dev
```

## 两层配置：工具全局，主机按项目

工具是通用的，但开发机往往属于某个具体项目。两者分开：

| | 位置 | 可见范围 |
|---|---|---|
| **rdev 工具** | `~/.claude.json`（user scope MCP） | 所有项目 |
| **project 主机** | `<项目>/.rdev/hosts.json` | 仅在该目录下工作时 |
| **global 主机** | `~/.rdev/hosts.json` | 所有项目 |

加载顺序是 global → project，**同名时 project 覆盖 global**，所以一个仓库可以把 `dev` 指向自己的机器。

`hosts add` 默认写 project scope（在某个 repo 里注册的机器，基本上就属于那个 repo）；跨项目复用的机器加 `-global`。

MCP server 继承 Claude Code 启动时的项目目录作为 cwd，这就是 project scope 生效的机制，不需要额外配置。

实测隔离效果：
```
在 ~/works/myproject  → rdev exec dev -- pwd    ✅ /home/youruser/myproject
在 ~/works/rdev       → rdev exec dev -- pwd    ❌ unknown host "dev"
在 ~                  → rdev hosts list         ❌ null
```

> `.rdev/` 建议加进你的**全局** gitignore（`git config --global core.excludesfile`），
> 而不是项目的 `.gitignore` —— 里面是你个人的用户名和跳板地址，同事的开发机不一样。

## MCP 工具（14 个）

| 工具 | 用途 |
|---|---|
| `rdev_exec` | 前台命令，`argv` 数组 |
| `rdev_job_start` / `_wait` / `_list` / `_status` / `_logs` / `_stop` | 长任务，断连存活 |
| `rdev_job_rm` | 回收 job 记录（按 id / `older_than_sec` / `keep_last`） |
| `rdev_read` / `rdev_write` | 远端文件读写（替代 heredoc） |
| `rdev_list` | 结构化列目录（替代 `ls` + 解析文本） |
| `rdev_sync` | rsync push/pull |
| `rdev_session` | 每 host 的 `cwd`/`env`/`remote_dir`/`secrets` 粘性状态 + 连接状态 |
| `rdev_secrets` | 凭据注册 + 全局脱敏（也可在 `rdev_session` 里声明路径,自动注册） |

## 核心设计决策

### 1. `argv` 数组是硬约束，不提供 shell 字符串入口

```jsonc
// ✅ 唯一的入口形态
{"argv": ["sqlite3", "db", "SELECT json_extract(x,'$.score') FROM runs"], "cwd": "~/myproject"}

// ❌ 故意不提供
{"command": "cd ~/myproject && sqlite3 db \"SELECT ...\""}
```

参数是 JSON 数组，agent 直接 `exec`，**没有任何 shell 解析它**。引号、词分割、glob 展开在结构上不可能发生。

不留 shell 逃生口是有意的：一旦有，所有人都会用它，引号问题原地复活。真需要管道时显式写 `["sh","-c","a | b"]`，代价可见。

### 2. 远端常驻 agent，不是纯 SSH 包装

```
Claude Code (本地)
      ↕ MCP over stdio
  rdev (本地 MCP server / CLI，共享 internal/client)
      ↕ ssh -o ControlMaster=auto   每台机一条复用连接
  rdev-agent (自动 bootstrap 到 ~/.cache/rdev/)
      ↕ JSON-RPC over stdin/stdout (NDJSON)
   远端 OS
```

Go 静态编译 → 远端**零运行时依赖**（实测目标机 tmux/screen/jq 全无、无 Go）。
四个平台的 agent 二进制 `go:embed` 进 rdev，按远端 `uname` 自动选择，SHA-256 比对决定是否重传。
支持的组合见[平台支持](#平台支持)。

### 3. 作业用 supervisor 模式托管

这是最关键的一处设计。detached 进程不能依赖 agent 记录退出码——agent 随 SSH 断开就死，reaper goroutine 根本轮不到运行。

所以 `job_start` 把 agent 自己作为中间父进程拉起：

```
rdev-agent -supervise <jobdir> -- <argv...>
```

supervisor 在 detached session 内，比 SSH 连接活得久，负责 `wait()` 并写 `status.json`。**退出码因此能跨越断连、agent 重启、host 重启。**

`job_stop` 按记录的 pgid 发信号（不是 grep 进程名），所以能连子进程一起清掉。

### 4. 完成通知用 job_wait，不靠轮询

长任务反复查 `job_status` 既费 context 又不及时。`job_wait` 一次调用阻塞到结束：

```jsonc
{"host":"dev", "id":"<job>", "timeout_sec":600, "tail_on_exit":20}
→ {"job":{"state":"exited","exit_code":3}, "waited_ms":12009, "logs":"..."}
```

三个约束：
- **有界**。默认 300s，上限 3600s。超时返回 `timed_out=true`，**job 不受影响**，再调一次即可。无界等待会让请求永远悬着。
- **不阻塞其他命令**。请求带 ID、两侧都并发（见决策 7），所以 wait 走**共享连接**即可，不再需要专用连接池。
- **退避轮询**。agent 侧 200ms → 3s 退避，跑一小时的 job 每分钟几次 stat，而不是每秒十次。

`tail_on_exit` 顺带回传尾部日志，省一次 `job_logs` 往返。

批量场景传 `ids` 而不是 `id`，N 个 job 共享一个 deadline，省掉 N 次串行等待。加 `wait_any` 则任一结束就返回——批量跑最想知道的往往是**第一个失败**，等全部结束才发现就浪费了。

### 5. login shell 用位置参数 trampoline

要解决 `uv: command not found` 就得加载 profile，但又不能把 argv 塞进 shell 脚本文本（会被二次解析）：

```
bash -lc 'exec "$@"' rdev <argv...>
```

profile 被 source，然后 `exec "$@"` 用**位置参数**替换进程。argv 作为真实参数传入，shell 不会重新解析它。默认开启。

### 6. 凭据在边界统一脱敏

注册过的值在**所有**返回值里被替换成 `<redacted:name>`（stdout/stderr/日志/错误消息/job argv/rsync 命令行/cwd）。
用 `env: {"TOKEN":"secret:name"}` 可以把明文注入远端环境，而值从不出现在调用或结果里。

**两道防线，因为逐字段脱敏被证明会漏。** `SyncResult.Command` 漏过一轮（见 bug 17），
而它和已脱敏的 `Stdout` 在同一个结构体字面量里——漏掉不会有任何报错，代价是明文凭据进对话记录。

1. **逐字段 `Redact`**（各 handler 内）。CLI 直接用 `internal/client`、不走 MCP，所以这层必须留着。
2. **MCP 边界兜底**（`AddReceivingMiddleware`）。跑在**已序列化**的 `structuredContent` 和文本 fallback 上，
   所以「新加一个字段」甚至「新加一个工具」都自动覆盖，不依赖谁记得。

第 2 层是实测出来的，不是推的：`rdev_session` 的 `cwd` **本来就没有**逐字段脱敏，
关掉中间件后测试立刻在真实 MCP 往返里抓到明文（`TestMiddlewareRedactsUnscrubbedResultField`）。
只拦 `tools/call`——`tools/list` 不含远端输出，扫它是白付字节。

**远端凭据直接注册**（推荐）：
```jsonc
{"action":"set_from_file", "name":"apptoken", "host":"dev", "path":"~/.config/myapp/token"}
```
值经 agent 连接读取，直接进 store —— 不落本地磁盘、不进对话记录。
不带 `host` 则读本地文件；**注意两侧凭据可能不同**，注册错的值会导致远端明文不被脱敏（假通过）。

**声明式注册**（免去每个会话手动重注册）：store 是内存态的（有意为之，明文不落盘），
但那意味着每开一个新 MCP 会话都得重新注册一遍——而**忘记注册的凭据就是会原样进对话记录的凭据**。
所以 hosts.json 里可以只存**路径**：

```jsonc
{"name":"dev", "addr":"user@h", "secrets":{"apptoken":"~/.config/myapp/token"}}
```

首次连接该主机时经 agent 读取并注册,**在第一条命令执行之前**完成——延迟注册会留下一个明文可外泄的窗口。
存的是路径不是值,所以本地磁盘上依然没有任何凭据。已显式注册过的同名 secret 不会被覆盖(手动调用优先)。
读取失败只在 stderr 警告、不阻断连接:一个凭据文件缺失不该让整台机器不可用。

按长度降序替换：当一个 secret 是另一个的子串时，先替换短的会让长的漏出片段。

**容忍换行**：值被 config dump / YAML 折行 / `fold` 拆到多行时仍然会被替换(空白字符可插在字符之间)。
这是格式化事故不是刻意隐藏,所以划在防守范围内。只对 ≥16 字符的值启用——太短的话"字符间夹空白"这个模式
可能在无关输出里自然出现。实测覆盖:换行、多次换行、空格、tab、CRLF、缩进续行。

代价可控:`Redact` 跑在每条命令的每个字节上,所以先用一次 `ContainsAny` 判断有无空白,
单行输出直接跳过扫描。实测单行 65KB **707 MB/s**(72 B/3 allocs),带空白的 64KB 输出 239 MB/s。

**已知限制**：匹配仍是全值的。这些**主动变形**后的片段拦不住(实测确认):

| 输出形态 | 是否拦住 |
|---|---|
| 原样 / JSON 引号 / URL query / 尾随换行 / 被标点包围 | ✅ |
| 跨行折断(含 CRLF、缩进续行) | ✅ 本轮新增 |
| `cut -c1-4`(前 4 字符)、后 8 字符 | ❌ |
| 长前缀/长后缀(39 字符的值取 30) | ❌ |
| 重新 base64、取 hash、大小写变换 | ❌ |

它防的是「凭据被原样 echo / dump / 折行 / 引用」这类常见事故,不是防刻意提取。

### 7. 请求按 ID 多路复用，两侧都并发

原先 `Conn.Do` 全程持锁、agent 也串行处理，所以对同一台机器的两个调用是排队的——一个 60s 的命令挡住所有其他请求。Claude 并行发多个 tool call 是常态，这个上限很容易撞到。

现在两侧都改了：

- **host 侧**：单独一个 reader goroutine 按 `resp.ID` 派发到等待者，`Do` 只在注册 pending 时短暂持锁。
- **agent 侧**：每个请求一个 goroutine，回复经加锁 writer 串行落到 stdout（回复是单行，交错写会毁掉分帧）；并发上限 16，`job_wait` 不占额度（它本来就是睡眠轮询，占额度会让几个 wait 饿死其他请求）。

两把锁是分开的，这点是必须的：**写锁不能和连接锁是同一把**。管道满时写会阻塞到 agent 读走，如果此时还持着连接锁，reader 就无法派发回复，而恰恰是这些回复才能让 agent 继续消费——直接死锁。这个 bug 在写测试时被 `TestConcurrentWritesStayFramed` 抓到了。

副产品：取消一个请求不再需要关掉整条连接。ID 还留在 pending 表里，迟到的回复被丢弃，连接和其他在途请求都不受影响。

### 8. 远端 job 记录需要回收

job 的 stdout/stderr 是无上界的文件，跑批量任务的机器会一直堆到磁盘满，`job_list` 也因为要遍历每个目录而变慢。`rdev_job_rm` 支持按 id、`older_than_sec`、`keep_last` 三种方式清理。

两个过滤器是**合取**的：`keep_last=5` + `older_than_sec=3600` 意味着保留窗口内的不删、没到期的也不删。这样组合起来是保守的，不会给人惊喜。

**运行中的 job 永不删除**，会出现在 `skipped` 里——删掉记录会让进程继续跑而无法观测和停止，比占磁盘更糟。

**并发删除是幂等的。** job 记录有多个写者：每个 rdev 进程各起一个 agent，共享同一个 `~/.cache/rdev/jobs/`；而单个 agent 内部每个请求也各跑在自己的 goroutine 里（`maxConcurrentRequests`）。原先「读 meta 判断是否 running，再删目录」两步之间没有锁，产生两种错误答案：在删除之后才读到的那个调用会冒出裸的 `meta.json: no such file or directory`；而所有在删除之前读到的调用**都报成功**——`os.RemoveAll` 对已不存在的路径返回 `nil`，于是 6 个并发删除报 6 次删除、6 倍的 `freed_bytes`。后者更糟，因为它看起来是对的。

现在每个 job 由 `<state>/.job-locks/<id>.lock` 上的 `flock` 保护，覆盖 `job_rm`（单个和 sweep）、`job_stop` 的状态写入、以及 supervisor 记录退出码的那一步。锁文件放在 `jobs/` **之外**：放在 job 目录里会被它自己序列化的那次 `RemoveAll` 删掉，而放在 `jobs/` 下又会被 `job_list` 的 `Total` 计数进去。

记录已经不存在的 job 现在回到 `missing` 字段，而不是报错——和 `skipped`（还活着，故意保留）是两种不同的原因，不该混在一个字段里。调用方要的是「这个 job 不存在」这个状态,而它确实已经达成了。

（锁**只**加在 agent 侧。锁的是文件，不是连接，所以 `internal/transport` 里没有任何相关代码。Windows 远端本来就不支持——见下面的平台一节，`GOOS=windows` 下 agent 连编译都过不了——所以这里不需要 `flock` 的回退实现。）

### 9. 协议兼容是区间，不是精确相等

`proto.Version` 现在是 2（`job_rm` / `list` / `-state` / 多 id `job_wait` / `job_list limit`），并且多了一个 `MinVersion`。

握手不再要求两侧版本号相等，而是检查 **host 自己的 `Version` 落在 agent 的 `[MinVersion, Version]` 区间内**。因为新 op 是加法：agent 比 host 新完全可用，host 只是不去调那些新 op 而已。而「agent 比 host 新」恰恰是最常见的情况——两个人共用一台开发机，谁最后连上就由谁的 rdev 覆盖了远端二进制。要是坚持精确相等，另一个人立刻就连不上了。

反过来 agent 比 host 旧才是真问题（新 op 会返回 `unknown op`），所以错误消息**分方向给结论**，而不是丢一个 `protocol N, want M` 让人猜该动哪边：

```
remote agent at ... speaks protocol 1 but this rdev needs 2;
it was installed by an older rdev -- run 'make agents && make build' and reconnect
```

`MinVersion` 只在真的放弃老格式支持时才抬——抬早了等于把还能用的 peer 挡在门外。

### 10. 协议兼容解决不了**二进制**抖动

上一节保证了「agent 比 host 新」时协议仍然可用，但没管**谁该覆盖谁**。`ensureAgent` 原先只比 hash 相不相等：

```go
if installedSHA != "" && installedSHA == want {
    return nil // already current
}
// 否则无条件上传
```

hash 能回答「一样不一样」，回答不了「谁更新」。于是**最后连上的那个永远赢**：共享开发机上两个同事的 rdev 无限互相覆盖对方的 agent；同一个人开着新旧两个窗口也一样。我们真踩过一整轮 —— 15:39 启动的旧 MCP server 把 16:00 构建推上去的新 agent 反复顶回旧版，持续一下午。

现在多一步**构建标识**比对，但只在 hash 已经不同时才做：

| 情况 | 行为 | 代价 |
|---|---|---|
| hash 相同 | 直接返回 | **0 次 ssh**（installedSHA 来自 connect 探测） |
| 首次安装 | 直接上传 | 没有已装 agent 可比 |
| hash 不同 | 跑一次 `rdev-agent -version` 再决定 | 1 次 round trip，只在首连和 rebuild 后付 |
| 已装的更新 | **拒绝并报错** | —— |

比对为什么不能放在 `ping`：ping 需要 agent 已经跑起来，而那时候二进制**已经被覆盖了**，要问的那个身份没了。所以直接问磁盘上那个文件，在写任何东西之前。这次调用带 10s 超时 —— 它 exec 的可能是个半截上传、错架构、或者根本不是 agent 的文件，没有 deadline 的话一个启动就挂住的二进制会把整个 connect 拖死。

**所有拿不准的情况都放行上传**：agent 太老没有 stamp、本地是裸 `go build` 没注入、任意一侧是脏树 —— 这些都不构成降级证据，而一个「拒绝修复损坏的远端 agent」的 bootstrap 比它要解决的问题更糟。

拒绝时的措辞刻意**不**断言远端「比你新」是关于分支的事实。commit 时间能排序两个构建，但共享机器上两个人在不同分支上是**分叉**而非先后，日期比对照样会挑出一个赢家。所以消息只陈述比了什么，让读的人自己判断是哪种情况：

```
refusing to replace the agent on u@h: the installed one was built later than this rdev
  installed: 0.1.0 bbbbbbb 2026-08-07T16:00:00Z
  this rdev: 0.1.0 aaaaaaa 2026-08-07T15:39:00Z
Overwriting it is what makes two rdev processes flip one agent back and forth.
If this rdev is simply stale, rebuild it:  make all
If the builds are from different branches, or you mean to roll back, force it:
  rdev hosts add dev u@h -force-agent-upload -save
```

逃生口是 per-host 的（`force_agent_upload`，`rdev hosts list` 会显示），因为需要它的场景 —— 一台 agent 总被别的分支盖的共享机 —— 是那台机器的属性，不是某一条命令的属性。

**默认不静默降级**：这个故障发生时是看不见的，事后又很难归因。

## 实测验证

全部在真实远端（Linux x86_64，SSH 跳板 36000 端口）跑过。

**本轮实际失败过的命令，逐条回归：**

| 验证项 | 之前 | 现在 |
|---|---|---|
| `sqlite3` + `$.score` JSON path | `no such column: $.score` | ✅ `exit=0` 零转义 |
| `uv` 定位 | `command not found` | ✅ `/home/youruser/.local/bin/uv` |
| 中文 + 引号 + `$()` | `?????` / 被求值 | ✅ `中文 "quoted" $(not-run) 'sq'` 原样 |
| `cwd` 继承 | 每次手写 `cd` | ✅ `/home/youruser/myproject` |
| 非零退出码 | 混在文本里 | ✅ `exit=1` + stderr 分离 |
| 写远端脚本 | heredoc 三层转义 | ✅ `rdev_write` 34 bytes |
| token 出现在输出 | 明文进对话 | ✅ `<redacted:apptoken>` |
| `secret:` 注入 | — | ✅ 远端拿到 `LEN=32`，本地看不到值 |

**作业模型：**

| 验证项 | 结果 |
|---|---|
| job 跨 3 次 SSH 断连 | ✅ 持续产出 |
| **退出码跨 agent 死亡** | ✅ `state=exited exit_code=42` |
| **MCP 进程退出后 CLI 查同一 job** | ✅ `state=exited exit_code=5 label=mcp-e2e` |
| 远端 grep + tail | ✅ `matched=10` 但只回传 2 行 |
| stop by pgid | ✅ 连子进程 `sleep` 一起清掉 |
| **supervisor 被 SIGKILL** | ✅ `state=running orphaned=true child_pid=N`，且 `job_stop` 能清掉 |
| **UTF-8 边界截断** | ✅ `cap=10` 切在「文」中间 → 返回 `中文测`（valid UTF-8，非乱码） |
| **远端凭据注册** | ✅ `source=dev:~/.config/myapp/...`，明文不落本地 |
| **job_wait 阻塞** | ✅ 5s 的 job 阻塞 6s 返回 `exited exit_code=3` |
| **wait 不阻塞其他命令** | ✅ 同 MCP 进程内 wait 阻塞 12s 期间 `exec` 0.2s 返回 |
| **wait 超时不影响 job** | ✅ `timed_out=true` + `state=running`，可再等 |
| **多 id `job_wait`** | ✅ `ids` 共享一个 deadline，逐 job 报结果；坏 id 只影响那一条 |
| **`wait_any`** | ✅ 快的 job 一结束就返回，不等慢的 |
| **握手接受更新的 agent** | ✅ host v2 连 `[1,3]` 的 agent 通过；agent 比 host 旧则报错并指明该重建哪边 |
| **首次连接 host key 未信任** | ✅ 附带取 key / 核对指纹的下一步，并明确劝阻 `StrictHostKeyChecking=no` |
| **自解释的 ssh 错误不加料** | ✅ 域名解析失败 / 连接被拒 / **host key 变更**原样返回 |
| **secrets 跨断连存活** | ✅ store 是进程级的，重连后脱敏照旧；重载路径幂等（已注册的名字不重取） |
| **MCP 边界兜底脱敏** | ✅ 真实 MCP 往返：未逐字段脱敏的 `cwd` 在 `structuredContent` 和文本 fallback 里都被替换；关掉中间件立刻抓到明文 |
| **`tools/list` 不受影响** | ✅ 中间件只拦 `tools/call`，工具名和 schema 原样通过 |

**本轮新增能力（agent 直连管道实测 + 单测）：**

| 验证项 | 结果 |
|---|---|
| **agent 侧并发**：3s 的 exec 与 echo 同时发 | ✅ echo **0.63s** 先返回，slow 3.64s 后到（乱序） |
| **host 侧按 ID 派发** | ✅ 后到的回复仍送达对应调用方 |
| **取消不再毁连接** | ✅ 迟到回复被丢弃，同连接下一次调用拿到 `fresh` |
| **并发写不串行化就死锁** | ✅ `TestConcurrentWritesStayFramed` 抓到并已修（写锁与连接锁分离） |
| **agent 死亡唤醒全部等待者** | ✅ 3 个在途调用都拿到错因，而非挂到 ctx 超时 |
| **`-state` 生效** | ✅ job 落在自定义目录，`~/.cache/rdev/jobs` 保持为空 |
| **`job_rm keep_last=1`** | ✅ 4 个 job → 删 3 个、释放 4699 bytes、保留最新 1 个 |
| **`job_rm` 拒绝无过滤器** | ✅ 报错而非清空整个目录 |
| **运行中 job 不被删** | ✅ 进 `skipped`，记录保留 |
| **`list` 结构化** | ✅ `od d na me.txt` 作为**单个** entry；`symlink=true`、`is_dir=true` 正确 |
| **陈旧 offset 不再 panic** | ✅ clamp 后返回空 + 可用的 `next_offset` |
| **Latin-1 文件** | ✅ `content_b64=true`（此前会被 JSON 改写成 U+FFFD） |
| **session 持久化往返** | ✅ `env` / `remote_dir` / `login_shell:false` 跨进程存活 |
| **`login_shell` 默认真** | ✅ 缺字段不会被读成 false（用指针 + 只写非默认值） |
| **MCP 工具全注册** | ✅ 14 个工具经真实 MCP 协议 `ListTools` 校验 |
| **断连后 agent 及时退出** | ✅ 在途 3600s `job_wait` 不再挡住关闭，2.1s 退出（此前 >15s 仍挂着） |
| **在途回复仍被冲刷** | ✅ 1s 的 exec 在 stdin 关闭后回复照样送出 |

**性能：**冷启动（含 agent 上传）2.26s → 热连接 0.55s。

**单元测试：**`go test ./... -race` 全绿，175 项
（agent 79、mcpsrv 25、transport 24、client 20、secrets 14、session 13）。此前 `transport` 与 `mcpsrv` 两个包零覆盖，现已补上。

开发过程中测试抓到 7 个真 bug：
1. job ID 用 `nanosecond/100000` 做后缀，同毫秒必碰撞 → 改 `crypto/rand`
2. macOS 的 openrsync 不认 `--info=stats1` → 只用可移植 flag
3. **MCP 并发调用导致多个 goroutine 同时 bootstrap，抢同一个 `.tmp` 文件** → 加 per-host dial 锁 + PID 后缀
4. **supervisor 被 SIGKILL 时子进程变孤儿继续跑，但状态报 `unknown`** → 三级状态判定（status.json → supervisor pid → child pid），`job_stop` 也能清孤儿
5. **`max_output_bytes` 按字节硬切会切断多字节 UTF-8** → 截断时丢弃不完整 rune
6. **`rdev_secrets` 只能读本地文件**，远端凭据要 sync pull 绕路 → 加 `host` 参数直读远端

7. **`job_wait` 在 `doJob` 里实现了但 dispatcher 没路由**，报 `unknown op` → dispatcher 改为查 `isJobOp()`，op 列表只有一份

第 4、5、6 三个是用户实测报告发现的；第 7 个是加 `job_wait` 时自己撞上的 —— 起因是 `main.go` 和 `jobs.go` 各维护一份 op 列表，现在只有一份了。

**本轮代码审查又抓到 8 个：**

8. **`job_logs` 的 `since_offset` 超过文件大小 → `makeslice: len out of range` panic**。`info.Size()-p.SinceOffset` 为负。增量轮询遇上日志轮转/截断就会触发，而当时**全项目没有一处 `recover()`**，一个请求能带走整个 agent 进程和该主机所有连接状态 → offset clamp 到 `[0, size]`，serve 循环加 per-request recover
9. **`isPrintableUTF8` 只检查 NUL，不检查 UTF-8 有效性**。Latin-1 文件（无 NUL）被当文本发出，`encoding/json` 静默改写成 U+FFFD —— 恰好是 `doRead` 注释声称要避免的事 → 改名 `isJSONSafeText` 并加 `utf8.Valid`
10. **`rdev_session` 的 `persist` 报 `saved=true` 但丢掉 `env` 和 `login_shell`**。`hostEntry` 只有 5 个字段，`State` 有 3 个。用户以为粘性 env 存下来了，重启后静默消失 → 补字段；`login_shell` 用**指针**，否则 `omitempty` 会让显式的 `false` 在往返中变回默认 `true`
11. **`Host.RemoteDir` 与 agent 硬编码的 `~/.cache/rdev` 分叉**。host 按 `RemoteDir` 装二进制和建 `jobs/`，agent 却总读默认路径，设了自定义值两边就对不上；而且该字段**根本没有设置入口**（只能手编 JSON）→ host 显式传 `-state`，并在 MCP/CLI 暴露；改 `RemoteDir` 时顺带 `Disconnect`，否则旧连接会继续用旧设置
12. **多路复用的写锁若与连接锁共用即死锁**。管道满时写会阻塞到 agent 读走，但持着连接锁 reader 就无法派发回复，而正是这些回复才能让 agent 继续消费 → 分成 `mu` / `writeMu` 两把

第 8、9 两个是纯读代码时用一次性探针测出来的；第 12 个是 `TestConcurrentWritesStayFramed` 挂住 30s 暴露的——它会在真实的并行 tool call 突发下命中。

**复查自己这轮改动时又抓到 3 个：**

13. **`wg.Wait()` 让 agent 在断连后挂着不退**。为「冲刷在途回复」加的排空是无界的，而 `job_wait` 可以有 3600s 预算，于是每一条掉线的 ssh 都会留下一个空转 agent。实测断连后 15s 仍未退出 → 排空改为 2s 上界。放弃这些 handler 是安全的：管道已关，回复无处可去，而 job 是 `setsid` detach 的，不依赖此进程
14. **`JobOut` 丢掉了 `orphaned` / `child_pid`**。agent 算得出、CLI 直接打印 `proto.JobInfo` 所以能看到，但 MCP 侧结构体没这两个字段——于是**同一个孤儿 job，CLI 说 orphaned、MCP 说健康**，正是 `internal/client` 想防的前端漂移
15. **`IsConnected` / `Disconnect` 经 `Hosts.Host` 解析会顺手注册主机**。该方法有意会把 ssh 形态的名字自动注册（为一次性机器提供便利），所以一次只读的状态查询就能在 `rdev_session` 列表里留下一个幽灵条目 → 这两处改为直接查连接池

第 13 个是我自己在做多路复用时引入的，只在「断连时恰好有长 `job_wait` 在途」才显现，例行测试跑不到。

另外修了两处不算 bug 但很脆的地方：`readFull` 用 `err.Error() == "EOF"` 做字符串比较（改 `errors.Is(err, io.EOF)`），以及 README 工具数写的是 11。

**本轮抓到 1 个假测试和 1 个真泄露：**

16. **`TestLoadHostSecretsKeepsExplicitValue` 是空的。** 起因是想确认「断连重连后声明式 secrets 还在不在」——如果丢了，脱敏会**静默失效**，输出照样返回，唯一的变化是 token 变成明文。结论是安全的（store 是进程级的、重载路径有 `Get` 幂等检查），但补测试时用变异测试验了一下：**把 `Get` 守卫整个删掉，测试照样通过**。原因是失败路径故意是静默的，于是「跳过了」和「重取失败但旧值还在」从外部完全无法区分。唯一的可观测差别是那条 stderr 警告 → 警告改走可注入的 hook，断言改成「不应该有警告」。删掉守卫时新测试会失败，旧的那个仍然不会（留着，但有牙的是新的那个）。
    唯一的线索其实是运行时间：3.08s vs 0.45s——因为没有守卫它会真的去 dial。

17. **`SyncResult.Command` 回传未脱敏的 rsync argv。** 就在 `Stdout` / `Stderr` 两行**下面**——同一个结构体字面量，上面两个字段过了 `Redact`，它没过。而 argv 是调用方给的：`--exclude` 模式和路径都可能带凭据。实测确认同一个 secret 在 `Stdout` 里是 `<redacted:tok>`、在 `Command` 里是明文。`ExecResult.Cwd` 是同一类（从请求回显），也一起补了。
    根因是**脱敏按字段做而不是在边界做**——所以除了修这两处，还加了 MCP 边界兜底（见决策 6）。
    加完之后又在两个地方发现同一个 bug：`rdev_session` 的 `cwd` 没脱敏（做兜底时撞到的），
    `JobInfo` 的 `Label` / `Cwd` 也没有（追 `redactJob` 的 0% 覆盖率时撞到的，`Argv` 脱了、这两个漏了）。
    也就是说我报告「一个泄露」的时候，实际至少有**四个实例**，我只看到了自己碰巧看的那个。
    这正是「逐字段」的问题：修掉看得见的那个，剩下的仍然静默存在——所以真正的修法是边界兜底 + 用反射遍历字段的测试。

## CLI

MCP 之外还有一套 CLI，共享同一 `internal/client`，给人肉调试和 CI 用：

```bash
rdev exec dev -- sqlite3 db "SELECT json_extract(x,'\$.score') FROM runs"
rdev job start dev -label batch -- ./run.sh
rdev job logs dev <id> -grep ERROR -tail 50
rdev job wait dev <id> -timeout 600 -tail 20              # 阻塞到结束，退出码透传给 shell
rdev job stop dev <id> -signal TERM -grace 5
rdev job rm dev -keep-last 5 -older-than 86400            # 回收旧 job 的日志
rdev read dev ~/app/config.yaml
rdev ls dev ~/app -limit 50                               # 结构化，不用解析 ls
echo "content" | rdev write dev /tmp/f.txt
rdev sync dev push ./src /remote/dst -exclude .git -dry-run
rdev hosts list                    # 含 scope / remote_dir / env
rdev hosts add dev user@h -port 36000 -save          # project scope
rdev hosts add prod user@h -global -save             # 全局可见
rdev hosts add dev user@h -env PROXY=http://p:1 -remote-dir '~/.cache/myrdev' -no-login -save
rdev hosts add dev user@h -secret apptoken='~/.config/myapp/token' -save   # 只存路径,不存值

# secrets 是 MCP 功能（store 在内存里，按进程隔离）。
# CLI 这三条只用来验证凭据路径解析正确、且脱敏真的覆盖了远端的值：
rdev secrets set-from-file apptoken ~/.config/myapp/token -host dev   # 只打印长度，不打印值
rdev secrets check dev apptoken -path ~/.config/myapp/token -- env    # → {"redacted": true}
rdev secrets list                                                     # 本进程已注册的名字
```

**CLI 故意没有 `secrets set <name> <value>`**，尽管 MCP 侧有 `action=set`。理由不是「避免密钥进 shell history」（那是个次要顾虑），而是它**做不到**：CLI 会注册、打印长度、然后进程退出，store 随之消失 —— 看起来提供了 MCP 的能力，实际什么也没做。MCP 侧有意义是因为 `rdev serve` 是长生命周期进程，注册完还会接着用那个值。

所以 `rdev secrets set` 现在返回一条解释这件事的错误,并指向真正能用的三条:

```
$ rdev secrets set -name x -value y
rdev: rdev has no `secrets set`: this store is in-memory and per-process, so a value
registered by one CLI command is gone when it exits.
To check that a credential file resolves and that redaction covers it:
  rdev secrets set-from-file <name> <path> [-host H]
  rdev secrets check <host> <name> [-path P] -- <argv...>
The MCP rdev_secrets tool does offer action=set, because `rdev serve` is a
long-lived process that goes on to use the value.
```

`--` 之后的一切都作为字面 argv 传递，本地 shell 也不会二次解析。

## 平台支持

本地和远端是**两套独立要求**，agent 只被交叉编译成四个 POSIX 组合。

| | 本地（跑 rdev） | 远端（跑 rdev-agent） |
|---|---|---|
| **Linux** amd64 / arm64 | ✅ | ✅ |
| **macOS** amd64 / arm64 | ✅ | ✅ |
| **Windows** | ❌ 未验证 | ❌ 不支持 |

所以 macOS ↔ Linux 四种方向任意组合都可以（本地 mac 连 Linux 开发机是主用法，实测在 Linux x86_64 上跑过；mac 连 mac、Linux 连 Linux 也在支持范围内）。

### Windows 远端：三层障碍，不是一层

最初这里只写了「job 模型依赖 POSIX 进程组」。那个说法不完整——真正要动的是三层，而且难度递增：

**① bootstrap 用的全是 POSIX 工具。** 「零远端准备、首次连接自动上传 agent」这个卖点是这么实现的：

| 步骤 | 实际命令 | Windows 上 |
|---|---|---|
| 探测 | `sh -c` 跑 `uname -s`、`uname -m`、`$HOME`、`sha256sum`\|`shasum` | 全都没有 |
| 上传 | `ssh … dd of=<path> status=none` + stdin 灌 9MB | 没有 `dd` |
| 安装 | `sh -c 'chmod 755 … && mv -f …'` | 没有 `chmod`/`mv` |

Windows OpenSSH 默认 shell 是 `cmd.exe`，**第一个 `uname` 就失败**——连「这是台什么机器」都问不出来，而要先知道是 Windows 才能换命令，鸡生蛋。PowerShell 侧有对应物（`Get-FileHash`、`$env:USERPROFILE`），但 9MB 二进制经 stdin 灌进 PowerShell 是出名的难搞（编码改写）。

**② job 模型建在 POSIX 进程语义上。** `Setsid` 把 job detach 出去（决策 3）、`syscall.Kill(-pgid)` 按进程组停子进程、`Kill(pid, 0)` 探活——`GOOS=windows` 下这些直接不编译（`-gcflags=-e` 数出 12 处，分布在 `jobs_run.go` / `jobs.go` / `main.go` / `supervise.go`；不加 `-e` 只会显示 10 条然后 `too many errors`）。都有对应物（Job Object、`OpenProcess`），是体力活。

**③ 信号语义无法等价，这层是永久降级。** Windows 上唯一保证生效的是 `TerminateProcess`，即 SIGKILL 等价物，**没有优雅版本**。`CTRL_BREAK_EVENT` 只能送到同组的控制台进程，且**可以被忽略**。于是 `job_stop` 的 `-signal TERM -grace 5` 会静默退化成「尽力发个 break，然后硬杀」——同一个 API，更弱的保证，而且恰好落在这个项目最核心的卖点上。

所以 `mapPlatform` 只认 `uname -s` 的 `linux` / `darwin`，其他值直接报 `unsupported remote OS`。**明确报错优于给一个看起来能用、实际保证更弱的实现。**

**已知可行但未实现的路（Tier 1）**：放弃「零远端准备」，成本立刻掉一个数量级——用户手动装一次 `rdev-agent.exe`，配置里声明 `platform: windows`，rdev 跳过探测和上传直接启动它。`exec` / 文件操作 / `list` 基本原样可用，而 `job_*` 显式返回 `unsupported on windows hosts`。**降级是显式的**，不违反上面那条原则。估一两天，等第一个真实用户出现就做。

（附带一个推测,没有 Windows 机器可验:login shell trampoline 大概可以直接跳过——Windows 的 PATH 来自注册表、由所有进程继承,决策 5 要解决的 `uv: command not found` 在那儿本来就不存在。真做的时候要先确认。）

### 本地 Windows：未验证

`rdev` 自身能 `GOOS=windows` 编过，但它调用外部 `ssh` 并依赖 `ControlMaster` 做连接复用——OpenSSH for Windows 至今不支持连接复用，而复用正是热连接 0.55s 的来源。`rdev_sync` 还依赖 `rsync`，Windows 不随系统提供。原生适配大概要引入 Go SSH 库自己管连接池，那是把「调用系统 ssh、复用它的 config 和 ProxyJump」这个简化假设整个推翻。没跑过，所以不宣称支持。

`make` 的任何目标都不产出 Windows 二进制（`PLATFORMS` 只有四个 POSIX 组合），所以没人会**意外**拿到一个跑不起来的版本。

> WSL2 里两侧都能当 Linux 用，零改动。但这**不是上面两个问题的答案**——想直接拿 Windows 当开发机的人，恰恰就是没装 WSL2 的那批人。列在这儿只是因为「如果你恰好有」。

## 首次连接：host key 必须先信任

`BatchMode=yes` 是必须的（交互式提示会挂死 MCP server），代价是**新主机的 host key 必须在首次连接前就被信任**，否则 ssh 直接失败。

这曾经只抛出一句 `Host key verification failed`。现在会附带下一步——怎么取 key、去哪核对指纹、以及**为什么不要用 `StrictHostKeyChecking=no`**（它会永久关掉这个检查，而 rdev 所有凭据脱敏的前提是「你连的是你以为的那台机器」）。

自解释的错误保持原样：域名解析失败、连接被拒，以及**host key 变更**——那条 OpenSSH 自己的警告比任何补充都更响亮、更具体，压在下面反而是帮倒忙。

## v1 明确不做

- ❌ **通用 shell 逃生口** — 见决策 1。这不是权衡，是整个项目的支点。
- ❌ **交互式 PTY / TUI 转发** — Claude 用不上；人肉需求直接用 `ssh`。
- ❌ **端口转发** — `ssh -L` 够用，而且它和 rdev 复用同一条 ControlMaster，用户已经有了。

## 还没做的（按优先级）

已完成的项留在这里作为记录：**首次连接的 host key 提示**、**secrets 跨断连存活**（结论是本来就安全，但补了测试）、**脱敏加 MCP 边界兜底**（见决策 6；bug 17 暴露了逐字段写法会漏，现在新字段自动覆盖）。那几轮还抓到一个假测试和一个真泄露，见 bug 台账 16、17。

**P1 — `exec` 的流式输出。** 命令跑完(或超时)才返回,期间拿不到增量。
实测确认:**超时会保留被 kill 之前已产生的 stdout/stderr**,`timed_out=true`、`truncated`/`stdout_bytes` 计数照旧准确,
且 kill 走进程组、能覆盖孙进程(测过 `sh -c 'sleep 45' &` 不留孤儿)。所以「跑一下看看输出到哪了」用 `exec` + 短 `timeout_sec` 就够。
真正的中间地带是**跑 30 秒的命令**:用 `exec` 得盲等,起 job 又要多两次往返——于是不确定要多久的命令倾向于起 job,简单任务也付了 job 的开销。

**排在 P1 而非 P0，是因为它卡在一个前提上。这轮把前提查清了，结论是「按原方案做不通」。** 三处硬证据：

1. **线协议是一请求一回复。** `readLoop` 收到回复就 `delete(c.pending, resp.ID)`（`internal/transport/conn.go:597`），增量推送要么改成多回复分帧，要么另开一路 notification——都是动 `proto` 的破坏性改动。
2. **MCP 的 `notifications/progress` 需要客户端先给 `progressToken`。** 服务端不能主动推：规范要求 token 来自请求方（SDK 里是 `CallToolParams.GetProgressToken`）。翻了本机 16 份 MCP 日志，**`progressToken` 出现 0 次**——Claude Code 调用工具时根本没带。没有 token，这条路在协议层就是关着的。
3. **Claude Code 实际用的是另一套机制。** 日志里每次连接都留下一行：
   ```
   Channel notifications skipped: server did not declare claude/channel capability
   ```
   也就是说增量推送走的是 `claude/channel` 这个厂商扩展，而不是标准的 progress notification。MCP SDK v1.7.0 里没有它的任何实现（`grep claude/channel` 无命中），但 `ServerCapabilities` 有 `Extensions`（`{vendor}/{name}` 格式）和 `Experimental` 两个槽，所以**技术上可以自己声明**——前提是先搞清 `claude/channel` 的实际报文格式，而那是未公开的。

所以现在的状态不是「没排上」，而是**原方案（progress notification）已被证伪，可行方案（`claude/channel`）依赖未公开的协议细节**。
在拿到格式之前不动手：一个建立在猜测报文上的破坏性改动，代价和收益完全不对称。

长任务的**持续**推送不算在这条里,那个场景本来就该用 job + `job_logs`。

**P3 — Windows 远端 Tier 1。** 见上。等第一个真实用户。

**不做 — `internal/client` 的传输层接缝。** 该包覆盖率 20.6%，明显低于其他包（54%–89%）。原因是 16 个 0% 的函数几乎都是同一个形状：拼请求 → `do()` → 脱敏 → 返回。要覆盖它们得把 `transport.Conn` 抽成接口再注入假实现。

**评估后认为不值得**：这些包装器里没有分支逻辑，测试会退化成「断言字段被复制进了结构体」——同义反复，改坏了照样通过。真正有内容的部分（`buildExecParams` 的分层、脱敏、参数校验）已经覆盖了，而这轮 `redactJob` 的漏洞是**直接测那个函数**抓到的，不需要接缝。

覆盖率数字会一直难看。**接受这一点，而不是用没有牙的测试把它刷上去**——后者更糟，因为它让下一个人以为这些路径有保护。真要做接缝，触发条件应该是「出现了带分支的包装器」，而不是「覆盖率低」。

**P4 — 多机并行扇出。** 原先列在「明确不做 — 等真需求」，但需求形态变了：Claude Code 现在会并行发 tool call，而 `job_wait` 的 `ids`/`wait_any` 已经是「一次调用管多个 job」的单机雏形。缺的是跨主机版本。仍然没人要，所以还在队尾——但它不再是「不做」。


## 开发

```bash
GO=~/sdk/go1.25.0/bin/go     # 本机 Go 装在这里，未改全局 PATH

make all           # = agents + build，日常就用这个
make agents        # 交叉编译 4 个平台的 agent
make build         # 编译 rdev（含 embed）
make check-agents  # 校验 embed 的 agent 确实由当前源码构出
make check         # = vet + test + check-agents
make test
make vet
```

改了 `cmd/rdev-agent/` 之后必须 `make agents`，否则 `bin/rdev` 里 embed 的还是旧 agent —— 远端跑的是那个副本，改动不会生效。

**用 `make all`，不要用 `go build ./cmd/rdev`。** 后者不重建 `cmd/rdev/agents/`，于是可以构出一个「内嵌 agent 比自身源码还旧」的 `bin/rdev`，而且从外面完全看不出来。我们真踩过：08-06 20:01 构建的 `bin/rdev` 里装着 20:18 的 agent。

两个工具让这件事可查、可拦:

```
$ rdev version
rdev 0.1.0 60503d1 2026-08-07T10:22:31Z
embedded agents:
  rdev-agent-darwin-amd64      3ef3e8588d1a  2548080 bytes
  rdev-agent-darwin-arm64      20407da522ed  2448658 bytes
  rdev-agent-linux-amd64       4e97ea14dabd  2580664 bytes
  rdev-agent-linux-arm64       9e55d8c503e9  2556088 bytes

$ make check-agents
STALE    rdev-agent-linux-amd64 embedded=66e69a9ea17b current=1b597da9bd83
...
The embedded agents were not built from this source tree.
Run `make all` (not `go build`) so bin/rdev and its agents agree.
```

版本标识由 `-ldflags -X` 注入 `internal/buildinfo`（git describe + **commit 时间**），`rdev-agent -version` 也会打印同一个 stamp,`ping` 结果里带 `build` 字段。

注意时间戳取的是 **commit 时间而非构建时间**：构建时间会让每次重编都改变 agent 的字节，那就废掉了「按 content hash 判断远端 agent 要不要换」这个快路径 —— 每次重连都要重传 9MB。commit 时间对排序这个用途一样够用，而且保持了构建可复现（实测两次 `-trimpath` 构建 SHA-256 完全一致，`check-agents` 正是靠这个性质才能用内容比对而不是比 mtime —— git 不保留 mtime，比时间戳会在新克隆和 CI 缓存上频繁误报）。

工作区脏时 `Commit` 带 `-dirty` 后缀，`buildinfo` 把它当作**不可排序**而不是「相等」：脏树继承父 commit 的时间，那个时间说明不了它的内容。

## 布局

```
cmd/rdev/            本地 CLI + MCP server 入口，embed agent 二进制
cmd/rdev-agent/      远端 agent：main / jobs / supervise
internal/proto/      线协议（唯一契约）
internal/transport/  ssh ControlMaster + agent bootstrap + NDJSON 框架
internal/client/     连接池 + 会话应用 + 脱敏（CLI 与 MCP 共享）
internal/mcpsrv/     MCP 工具定义
internal/secrets/    凭据脱敏
internal/session/    host 注册表 + 粘性状态
```

## 许可

MIT，见 [LICENSE](LICENSE)。

依赖的许可证都与 MIT 兼容（都是宽松型，无 copyleft）：

| 依赖 | 许可 |
|---|---|
| `modelcontextprotocol/go-sdk` | Apache-2.0 / MIT 混合（见下） |
| `google/jsonschema-go`、`segmentio/asm`、`segmentio/encoding` | MIT |
| `yosida95/uritemplate` | BSD-3-Clause |
| `golang.org/x/{oauth2,sync,sys,time}` | BSD-3-Clause（Go Authors） |

唯一的直接依赖 MCP SDK **不是纯 MIT**：该项目正在从 MIT 迁移到 Apache-2.0，新代码是
Apache-2.0，尚未取得转授权同意的原贡献仍是 MIT。两者都允许在 MIT 项目里使用。
只是如果将来要分发**含 SDK 代码**的产物（本项目不这么做——`go.mod` 引用而非 vendor），
Apache-2.0 的第 4 条要求保留其 NOTICE 和变更声明。

