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
| 无状态 | 每次都要 `cd ~/nexus` | 连接不持久 |
| 输出爆炸 | 每条命令手动接 `tail`/`grep` | 没有输出预算 |
| 凭据泄露 | token 明文进了对话记录 | 无脱敏 |

**根本矛盾：把结构化意图压成一个 shell 字符串，再让远端重新解析它。**
只要还在拼字符串，包装得再漂亮也会继续踩。

## 快速开始

```bash
make build                                     # 需要 Go 1.25+

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
在 ~/works/nexus  → rdev exec dev -- pwd    ✅ /home/tonynyyan/nexus
在 ~/works/rdev   → rdev exec dev -- pwd    ❌ unknown host "dev"
在 ~              → rdev hosts list         ❌ null
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
| `rdev_session` | 每 host 的 `cwd`/`env`/`remote_dir` 粘性状态 + 连接状态 |
| `rdev_secrets` | 凭据注册 + 全局脱敏 |

## 核心设计决策

### 1. `argv` 数组是硬约束，不提供 shell 字符串入口

```jsonc
// ✅ 唯一的入口形态
{"argv": ["sqlite3", "db", "SELECT json_extract(x,'$.score') FROM runs"], "cwd": "~/nexus"}

// ❌ 故意不提供
{"command": "cd ~/nexus && sqlite3 db \"SELECT ...\""}
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

### 5. login shell 用位置参数 trampoline

要解决 `uv: command not found` 就得加载 profile，但又不能把 argv 塞进 shell 脚本文本（会被二次解析）：

```
bash -lc 'exec "$@"' rdev <argv...>
```

profile 被 source，然后 `exec "$@"` 用**位置参数**替换进程。argv 作为真实参数传入，shell 不会重新解析它。默认开启。

### 6. 凭据在边界统一脱敏

注册过的值在**所有**返回值里被替换成 `<redacted:name>`（stdout/stderr/日志/错误消息/job argv）。
用 `env: {"TOKEN":"secret:name"}` 可以把明文注入远端环境，而值从不出现在调用或结果里。

**远端凭据直接注册**（推荐）：
```jsonc
{"action":"set_from_file", "name":"gftoken", "host":"dev", "path":"~/.nexus/auth/gongfeng/key"}
```
值经 agent 连接读取，直接进 store —— 不落本地磁盘、不进对话记录。
不带 `host` 则读本地文件；**注意两侧凭据可能不同**，注册错的值会导致远端明文不被脱敏（假通过）。

按长度降序替换：当一个 secret 是另一个的子串时，先替换短的会让长的漏出片段。

**已知限制**：匹配是全值的。`cut -c1-4`、重新 base64、取 hash 这类**主动变形**后输出的片段不会被拦住
（实测 `echo $MYTOK | cut -c1-4` → `82d9` 明文可见）。它防的是「凭据被原样 echo / dump / 引用」这类常见事故，
不是防刻意提取。

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

## 实测验证

全部在真实远端（TencentOS x86_64，SSH 跳板 36000 端口）跑过。

**本轮实际失败过的命令，逐条回归：**

| 验证项 | 之前 | 现在 |
|---|---|---|
| `sqlite3` + `$.score` JSON path | `no such column: $.score` | ✅ `exit=0` 零转义 |
| `uv` 定位 | `command not found` | ✅ `/home/tonynyyan/.local/bin/uv` |
| 中文 + 引号 + `$()` | `?????` / 被求值 | ✅ `中文 "quoted" $(not-run) 'sq'` 原样 |
| `cwd` 继承 | 每次手写 `cd` | ✅ `/home/tonynyyan/nexus` |
| 非零退出码 | 混在文本里 | ✅ `exit=1` + stderr 分离 |
| 写远端脚本 | heredoc 三层转义 | ✅ `rdev_write` 34 bytes |
| token 出现在输出 | 明文进对话 | ✅ `<redacted:gongfeng>` |
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
| **远端凭据注册** | ✅ `source=dev:~/.nexus/...`，明文不落本地 |
| **job_wait 阻塞** | ✅ 5s 的 job 阻塞 6s 返回 `exited exit_code=3` |
| **wait 不阻塞其他命令** | ✅ 同 MCP 进程内 wait 阻塞 12s 期间 `exec` 0.2s 返回 |
| **wait 超时不影响 job** | ✅ `timed_out=true` + `state=running`，可再等 |

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

**单元测试：**`go test ./... -race` 全绿，132 项
（agent 65、mcpsrv 20、session 13、transport 12、secrets 11、client 11）。此前 `transport` 与 `mcpsrv` 两个包零覆盖，现已补上。

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

# secrets 是 MCP 功能（store 在内存里，按进程隔离）。
# CLI 这两条只用来验证凭据路径解析正确、且脱敏真的覆盖了远端的值：
rdev secrets set-from-file gftoken ~/.nexus/auth/gongfeng/key -host dev   # 只打印长度，不打印值
rdev secrets check dev gftoken -path ~/.nexus/auth/gongfeng/key -- env    # → {"redacted": true}
```

`--` 之后的一切都作为字面 argv 传递，本地 shell 也不会二次解析。

## v1 明确不做

- ❌ 交互式 PTY / TUI 转发 — 复杂度高，Claude 用不上，人肉需求直接用 `ssh`
- ❌ 端口转发 — `ssh -L` 够用
- ❌ 多机并行扇出 — 等真需求
- ❌ 通用 shell 逃生口 — 见决策 1

## 还没做的

- **`exec` 没有真正的流式输出**。命令跑完(或超时)才返回,期间拿不到增量。
  不过实测确认:**超时会保留被 kill 之前已产生的 stdout/stderr**,`timed_out=true`、`truncated`/`stdout_bytes` 计数照旧准确,
  且 kill 走进程组、能覆盖孙进程(测过 `sh -c 'sleep 45' &` 不留孤儿)。
  所以「跑一下看看输出到哪了」用 `exec` + 短 `timeout_sec` 就够,不必为此起 job。
  真正缺的是长任务的**持续**推送——那个场景本来就该用 job + `job_logs`,不打算在 exec 上再造一套。
- **`job_wait` 只等单个 job**。批量跑 N 个要 N 次调用串行等，缺 `wait_any` / 多 id 版本。
- **secrets 不跨进程**。store 在内存里（有意为之，不落盘），但每个新 MCP 会话都要重新注册。
- **`proto.Version` 仍是 1**。本轮加了 `job_rm` / `list` / `-state`，旧 agent 遇到新 op 会报 `unknown op` 而不是被识别为版本不匹配。靠 SHA-256 比对会自动重传，所以实际不会踩到——但如果哪天要支持「host 比 agent 新」，这里得先动。

## 开发

```bash
GO=~/sdk/go1.25.0/bin/go     # 本机 Go 装在这里，未改全局 PATH

make agents      # 交叉编译 4 个平台的 agent
make build       # 编译 rdev（含 embed）
make test        # 单元测试
make vet
```

改了 `cmd/rdev-agent/` 之后必须 `make agents`，否则 `bin/rdev` 里 embed 的还是旧 agent —— 远端跑的是那个副本，改动不会生效。

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
