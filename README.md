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

## MCP 工具（11 个）

| 工具 | 用途 |
|---|---|
| `rdev_exec` | 前台命令，`argv` 数组 |
| `rdev_job_start` / `_wait` / `_list` / `_status` / `_logs` / `_stop` | 长任务，断连存活 |
| `rdev_read` / `rdev_write` | 远端文件读写（替代 heredoc） |
| `rdev_sync` | rsync push/pull |
| `rdev_session` | 每 host 的 `cwd`/`env` 粘性状态 |
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
- **独立连接**。`Conn.Do` 在单条 agent 管道上串行化请求，所以阻塞等待走**专用连接池**（`waitConns`），否则一个 5 分钟的 wait 会卡住到该主机的所有命令。
  实测：同一 MCP 进程内 wait 阻塞 12s 期间，`exec` **0.2s** 返回。
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

**性能：**冷启动（含 agent 上传）2.26s → 热连接 0.55s。

**单元测试：**`go test ./...` 全绿（agent 43 项、secrets 11 项、session 8 项、client 9 项）。

开发过程中测试抓到 7 个真 bug：
1. job ID 用 `nanosecond/100000` 做后缀，同毫秒必碰撞 → 改 `crypto/rand`
2. macOS 的 openrsync 不认 `--info=stats1` → 只用可移植 flag
3. **MCP 并发调用导致多个 goroutine 同时 bootstrap，抢同一个 `.tmp` 文件** → 加 per-host dial 锁 + PID 后缀
4. **supervisor 被 SIGKILL 时子进程变孤儿继续跑，但状态报 `unknown`** → 三级状态判定（status.json → supervisor pid → child pid），`job_stop` 也能清孤儿
5. **`max_output_bytes` 按字节硬切会切断多字节 UTF-8** → 截断时丢弃不完整 rune
6. **`rdev_secrets` 只能读本地文件**，远端凭据要 sync pull 绕路 → 加 `host` 参数直读远端

7. **`job_wait` 在 `doJob` 里实现了但 dispatcher 没路由**，报 `unknown op` → dispatcher 改为查 `isJobOp()`，op 列表只有一份

第 4、5、6 三个是用户实测报告发现的；第 7 个是加 `job_wait` 时自己撞上的 —— 起因是 `main.go` 和 `jobs.go` 各维护一份 op 列表，现在只有一份了。

## CLI

MCP 之外还有一套 CLI，共享同一 `internal/client`，给人肉调试和 CI 用：

```bash
rdev exec dev -- sqlite3 db "SELECT json_extract(x,'\$.score') FROM runs"
rdev job start dev -label batch -- ./run.sh
rdev job logs dev <id> -grep ERROR -tail 50
rdev job wait dev <id> -timeout 600 -tail 20              # 阻塞到结束，退出码透传给 shell
rdev job stop dev <id> -signal TERM -grace 5
rdev read dev ~/app/config.yaml
echo "content" | rdev write dev /tmp/f.txt
rdev sync dev push ./src /remote/dst -exclude .git -dry-run
rdev hosts list                    # 含 scope 列
rdev hosts add dev user@h -port 36000 -save          # project scope
rdev hosts add prod user@h -global -save             # 全局可见
```

`--` 之后的一切都作为字面 argv 传递，本地 shell 也不会二次解析。

## v1 明确不做

- ❌ 交互式 PTY / TUI 转发 — 复杂度高，Claude 用不上，人肉需求直接用 `ssh`
- ❌ 端口转发 — `ssh -L` 够用
- ❌ 多机并行扇出 — 等真需求
- ❌ 通用 shell 逃生口 — 见决策 1

## 开发

```bash
GO=~/sdk/go1.25.0/bin/go     # 本机 Go 装在这里，未改全局 PATH

make agents      # 交叉编译 4 个平台的 agent
make build       # 编译 rdev（含 embed）
make test        # 单元测试
make vet
```

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
