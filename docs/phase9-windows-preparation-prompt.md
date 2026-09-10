你是运行在 Windows 目标机器上的工程 agent。请实际执行配置、检查和必要的修复，把这台机器准备成 rdev Phase 9 的原生 Windows 远端执行机，并交付可复查的检查结果和 Linux 控制端连接信息。不要只给建议或安装教程。

目标是 Linux 控制端通过 Windows OpenSSH Server 启动 Windows 原生 rdev-agent.exe，执行命令、传输文件、同步目录、运行可跨 SSH 断线存活的后台任务，并完成 agent 安装与升级恢复测试。

所有业务进程以选定的普通 Windows 用户运行。原生 Windows 控制端、WSL 远端、交互式桌面、RDP、PTY、管理员提权弹窗不属于本任务。

一、输入与执行原则

先从现有环境和用户已提供的信息中确定以下输入；确实缺失时集中询问一次，同时继续不依赖这些输入的检查：

- 目标登录账号：优先使用用户指定的普通本地账号；没有指定时报告合适的现有账号，创建专用账号前确定用户名及凭据管理方式。
- Linux 控制端的 SSH 公钥。只需要公钥，不能索取、生成后传回或复制控制端私钥。如果已有明确属于控制端的公钥，核对指纹后复用。
- 控制端到目标机的网络路径，以及 Windows 实际看到的来源 IP/可信网段；可能经过 VPN、NAT 或跳板机，不能直接假定控制端本机 IP 就是来源地址。
- SSH 端口：优先保留已有有效端口；新安装默认使用 TCP 22。
- 当前 Phase 9 源码或测试产物所在位置，如果已经提供。没有产物时先完成主机准备，原生功能验证记为待执行。

以下行为已属于本任务的执行范围：只读检查；创建专用测试目录；安装官方 Windows OpenSSH Server 组件；配置并启动 sshd；为指定账号添加指定公钥；添加限定来源的 SSH 防火墙规则；在新建的专用目录中配置所需 ACL；运行隔离测试和清理本次创建的临时文件。

遵循已有机器管理约束。需要管理员权限而当前进程没有权限时，明确列出尚未执行的管理员动作，并继续完成其他检查。不要将权限不足报告成已完成。不要在报告中包含密码、私钥、访问令牌或全部 authorized_keys 内容。

修改 sshd 配置、注册表、服务启动方式、防火墙或现有文件 ACL 前，记录原值并保存必要备份；优先增量、可重复执行的变更。保留现有用户、公钥、会话和用途。不要覆盖整个 sshd_config、authorized_keys 或整棵用户目录 ACL，不要删除未知的 rdev 状态。

不要关闭 Defender、防火墙、UAC、WDAC/AppLocker、审计或组织策略，不要添加整盘/整目录安全软件排除项。受管理策略阻止时记录具体事件和规则，交给管理员处理。不要自行开放公网端口、配置路由器转发、强制重启系统或永久关闭休眠来获得通过结果。

二、确认系统、工具和存储基础

1. 记录 Windows 产品名称、版本、OS Build、补丁状态和 OS/进程架构。首个目标为 Windows 11 x64；Windows Server 可以作为单独的测试目标，必须按实际版本报告，不能写成 Windows 11 已验证。ARM64、32 位 Windows 或 WSL 不作为本次 amd64 原生验收通过。
2. 使用 64 位 Windows PowerShell 5.1。检查实际路径、版本和非交互 SSH 会话中的 PATH，确保可以直接启动 powershell.exe、cmd.exe 和 whoami.exe。PowerShell 7 可以存在，但不能代替验证 powershell.exe 的 Windows PowerShell 5.1 路径。
3. 检查 PowerShell LanguageMode 及实际功能：执行固定的 -NoLogo -NoProfile -NonInteractive -EncodedCommand 程序；使用 ConvertFrom-Json、Get-FileHash、Get-Acl、.NET GZipStream、FileStream、ProcessStartInfo，以及 UTF-8/UTF-16 编解码。编码内容使用 UTF-16LE 后 base64。不能通过全局设置 ExecutionPolicy Bypass/Unrestricted 来“修复”问题。
4. 确认系统时钟和时区正常；记录检查时间。时间异常会影响日志、进程身份和后续发行有效期判断。
5. 目标账号的真实 UserProfile、TEMP、rdev 状态目录和测试工作目录必须位于可写的本地 NTFS 卷。检查磁盘剩余空间及配额。建议留至少 2 GiB 用于隔离验收，源码构建需要更多空间；这是测试余量建议，不是 rdev 协议的硬性最低配置。
6. 状态和测试路径避免 OneDrive/网络目录重定向、UNC、设备路径、ADS、junction、symlink 和其他 reparse point，检查路径的每一层祖先。临时目录也要检查，因为原生测试会在那里创建状态。
7. 保证联调期间机器开机、联网且不会被计划关机/睡眠打断。报告可用时间段和电源状态；如需改变全局电源策略，先说明实际需要，不要擅自永久修改。

三、配置 Windows OpenSSH Server

1. 使用 Get-WindowsCapability 或现有安装信息确认 OpenSSH.Server 和 OpenSSH.Client，记录 sshd/ssh/ssh-keygen 的路径、版本和来源。缺少 Server 时通过 Windows 官方可选组件或已批准的官方安装包安装；离线安装不要求放通任意外网。
2. 检查服务名称 sshd，配置自动启动并启动服务。确认实际监听地址与端口，检查端口冲突和服务日志。服务正在运行、TCP 正在监听、认证成功是三个独立检查项。
3. 找到实际生效的 sshd_config，常见位置为 C:\ProgramData\ssh\sshd_config。检查 PubkeyAuthentication、AuthorizedKeysFile、AllowUsers/DenyUsers、AllowGroups/DenyGroups、Match 块，以及可能阻止远程命令的 ForceCommand/受限子系统配置。只修改与目标账号接入有关的配置。
4. 修改后用实际 sshd.exe 执行配置语法检查；可用时用有效配置输出检查目标用户和来源地址对应的 Match 结果。语法检查通过后再重启服务，并保留恢复路径，避免在唯一远程管理会话中无准备地断开自身连接。
5. 记录 HKLM\SOFTWARE\OpenSSH 下 DefaultShell 及相关参数的有效值。新装机器可使用 Windows 默认 cmd.exe；已有 PowerShell 默认 shell 时验证实际行为。已有其他用户依赖默认 shell 时不要直接全局切换。
6. 登录必须支持执行非交互命令、双向 stdin/stdout/stderr 和退出码返回，不能强制进入 TUI、交互式登录脚本、Conda 初始化提示、MFA/UAC 提示或仅限 SFTP 的会话。
7. 普通 rdev 运行不要求安装 Go、Git、Python、rsync、MSYS2、Cygwin、WSL、PowerShell 7 或任何额外 .NET SDK。也不要求 TCP 转发、RDP、PTY。不要把这些列为基础依赖或顺手安装。
8. 业务命令需要的编译器、SDK 或工具链只按明确的工作负载要求安装；它们与 rdev 连接条件分开报告。

四、配置账号、公钥和 SSH 主机身份

1. 确认目标账号启用、没有登录限制，能够实际加载本地用户配置文件。以该账号记录 whoami、SID、所属组及 UserProfile，不能用管理员配置会话的 HOME 代替目标用户目录。
2. 优先使用非 Administrators 成员账号。添加公钥时使用实际 AuthorizedKeysFile 规则，避免重复添加，也不能覆盖已有公钥。
3. 普通账号通常使用 <UserProfile>\.ssh\authorized_keys；如果目标账号属于 Administrators，Windows 默认 Match Group administrators 可能改用 C:\ProgramData\ssh\administrators_authorized_keys。按实际配置处理并验证 ACL，不要通过授予管理员身份来绕过普通用户权限问题。
4. 检查 .ssh 和 authorized_keys 的所有者与 ACL 符合 Windows OpenSSH 要求。使用 SID 或解析后的身份操作，避免依赖英文版的 Users/Administrators 组名。authorized_keys 文件编码不得破坏公钥行，例如引入 UTF-16 或多余 BOM；保留原有合法行。
5. 普通用户的 .ssh 权限、管理员公钥文件权限和 rdev 私有状态权限是不同的检查项，不能简单复用一份 ACL 模板。
6. 保留现有合法 SSH 主机密钥；缺失时由官方 OpenSSH 机制生成。输出实际主机公钥的类型和 SHA256 指纹，供控制端通过可信渠道核对。不要输出主机私钥。
7. 无人值守连接最终必须通过公钥认证和 BatchMode；不能依赖远端输入账号密码或弹窗。控制端私钥可以有口令并通过已有 ssh-agent 解锁，不要求创建无口令私钥。

五、打通并验证网络

1. 输出适合控制端访问的主机名/IP、接口、SSH 端口、路由/VPN/跳板关系。不要把 127.0.0.1、本机自测地址或不可达内网 IP 当作已交付的远程连接地址。
2. 检查 Windows 防火墙有效规则、活动网络配置文件及其他已知网络边界。为本任务新建的 SSH 入站规则限定到已确认来源 IP/网段及正确端口/配置文件。来源不明确时报告缺失项，不自行采用 Any 公网来源。
3. 从 Windows 本机检查服务和端口可达只能证明本机状态。要求 Linux 控制端实际连接后，才能将 controller_to_windows_network 和 batchmode_publickey_auth 标为 PASS。
4. 如果你没有控制端工具访问权限，生成控制端需要执行的命令和连接片段，不要擅自联系第三方或索取私钥。可以完成的 Windows 检查继续进行，跨机检查标记 PENDING。

六、准备 rdev 状态与测试目录

1. 查询目标账号的 [Environment]::GetFolderPath('UserProfile')。默认状态相对目录为 .cache/rdev，完整路径为 <UserProfile>\.cache\rdev。控制端实际 SSH 测试还会创建随机的 <UserProfile>\.cache\rdev-phase9-* 隔离命名空间。
2. 不需要预装 rdev-agent.exe；控制端会上传并选择正确架构。不要手工伪造 .rdev-release.json、版本目录、安装日志、迁移锁或任务记录，也不要把未知旧状态覆盖成空目录。
3. 可以预先创建本次专用目录，或让 rdev 创建。新建 rdev 私有目录使用受保护 DACL，仅允许目标用户 SID 与 SYSTEM，子目录/文件继承这两个身份的所需权限；记录实际所有者和 SDDL。目标用户须具备读写、创建子目录、执行程序、替换及删除测试文件的能力。
4. 已存在的 .cache 或 rdev 目录先检查所有者、ACL 和内容。rdev 当前实现会拒绝不符合要求的私有状态，不能靠反复上传解决。若目录包含其他应用数据或宽泛 ACL，不要递归改权限或删除，选择无冲突的专用账号/位置，或明确报告冲突。注意当前自动 SSH smoke 使用固定的 .cache/rdev-phase9-* 路径，仅配置另一个状态目录不足以证明该 smoke 可运行。
5. 私有路径的祖先不必全都采用 rdev 的私有 DACL，但必须是受信所有者且不能让其他普通身份改写、删除或重定向所选路径。当前实现信任用户本人、SYSTEM、Administrators 及 Windows TrustedInstaller 身份；具体检查以提供的 Phase 9 winutil 和 bootstrap 测试为准。不要递归收紧 C:\、C:\Users 或共享父目录权限。
6. 工作目录优先为 <UserProfile>\rdev-work，或用户指定且权限合适的本地 NTFS 目录；输出实际绝对路径及 C:/... 写法。创建本次专用子目录，检查中文名、空格名、普通文件读写、目录创建、SHA256、同目录文件替换、只读属性和时间戳行为，并清理本次测试文件。
7. 路径不得使用保留设备名、尾随点/空格或冒号数据流；同步测试不得依赖大小写不同但名称冲突的文件、junction/symlink、ACL/xattr/owner 完整保真或 POSIX 执行位。远端 rsync 不受支持，目录同步验证使用共享 broker 的 prepare/plan 流程。
8. 通过实际目标用户和 SSH 会话确认安全软件允许执行本次已确认来源及摘要的 agent/测试程序。发生拦截时保留文件摘要、事件编号和相关策略名称，不能关闭保护后宣布通过。

七、验证 shell、二进制通道和后台进程生命周期

1. 本机完成相应检查后，再通过真正的非交互 SSH 会话复查账号、架构、UserProfile、PATH、powershell.exe 路径和工作目录权限。不能用管理员终端里的成功结果代替 SSH 用户环境。
2. 通过当前默认 SSH shell 调用 powershell.exe -NoLogo -NoProfile -NonInteractive -EncodedCommand。固定脚本与动态参数分别编码，不能把用户提供的路径、公钥或命令值直接拼接到 shell 源码中。
3. 验证 Unicode/空格/引号/反斜杠参数、显式环境变量、cwd、stdin、非零退出码和二进制 stdin/stdout。不要用 PowerShell 文本管道、Out-File 或默认文本编码转发 .exe、测试二进制或原始协议流；使用文件流/原生字节流并比较 SHA256。
4. rdev 使用短的 EncodedCommand 加 stdin 上的一行有界 base64/gzip 安装脚本，再接原始 executable 字节；安装器与 loader 必须共享输入流以避免读走二进制内容。这一行为优先运行现有 transport bootstrap 测试验证，不要另造一个简化启动器就当作 rdev 已通过。
5. 前台任务要求 Job Object 在子进程开始运行前接管进程树；超时、取消和连接关闭应清理属于该请求的后代。后台监督进程要求能从 SSH 会话的外层 Job Object 成功 break away，并在 SSH 会话结束后继续运行。
6. 这不是检查一个注册表开关就能确认的条件。必须使用实际 rdev 后台任务：启动一个持续任务，关闭该 SSH 执行会话，重新建立连接，确认同一任务继续执行、日志存在、能查询状态并能停止；同步检查超时后没有遗留测试后代。任务只能影响本次隔离命名空间。
7. 如果 CREATE_BREAKAWAY_FROM_JOB 被当前 sshd/运行宿主策略拒绝，记录实际错误、sshd 版本和启动方式，标为 BLOCKED 或 FAIL。不能假装脱离已成功，也不能用计划任务、SYSTEM 服务、交互桌面或另一个手写守护程序代替 rdev 实现来获得通过结果。
8. 没有 agent 产物时，上述功能项记为 PENDING，仍可完成主机基础准备。不要把尚未执行的原生或真实 SSH 生命周期测试写成 PASS。

八、按可用产物运行现有原生验收

请优先使用提供的当前 Phase 9 测试产物，记录文件 SHA256 和来源；校验它们与控制端所给摘要一致。Linux 构建出的 rdev-agent-windows-amd64 在 Windows 上需要使用 .exe 文件名运行。

方式 A：已提供 Windows amd64 agent 和交叉编译的原生测试程序，不需要安装 Go。将它们保存在受控测试目录，以目标普通账号运行。根据实际文件名替换以下路径，并逐条保存退出码及输出：

```powershell
$env:RDEV_WINDOWS_AGENT = 'C:\实际测试目录\rdev-agent.exe'
& $env:RDEV_WINDOWS_AGENT -version
& 'C:\实际测试目录\rdev-phase9-files.test.exe' -test.v -test.count=1 -test.timeout=5m
& 'C:\实际测试目录\rdev-phase9-runtime.test.exe' -test.v -test.count=1 -test.timeout=10m
& 'C:\实际测试目录\rdev-phase9-install.test.exe' -test.v -test.count=1 -test.timeout=10m
& 'C:\实际测试目录\rdev-phase9-transport.test.exe' -test.v -test.count=1 -test.timeout=5m -test.run='^TestWindows'
```

这些是命令模板，不是可盲目粘贴的真实路径。每条原生命令后立即检查 $LASTEXITCODE；不能只读取最后一条命令的退出码。transport 测试只运行 Windows 对应测试，不能在 Windows 上直接运行全仓测试并把 Unix 依赖失败归为机器未配置好。

方式 B：已提供完整、正确版本的 Phase 9 源码，并允许安装构建工具，则使用仓库 go.mod 指定的 toolchain（当前为 Go 1.26.8），核对源码身份后执行：

```powershell
$env:CGO_ENABLED = '0'
$env:RDEV_WINDOWS_AGENT = Join-Path $env:TEMP 'rdev-phase9-agent.exe'
go build -trimpath -o $env:RDEV_WINDOWS_AGENT ./cmd/rdev-agent
go test -v -count=1 -timeout=10m ./internal/winutil ./internal/windowsruntime ./internal/agentinstall
go test -v -count=1 -timeout=5m ./internal/transport -run '^TestWindows'
```

同样逐条检查退出码，构建失败后不得继续运行旧产物。运行前检查实际使用的 Go 路径与版本。Go 仅用于这一源码验证方式，不是 Windows 远端运行依赖。

当前 Phase 9 可能尚未提交或推送到远程 main；不能自行 clone 一个旧 main 就宣布缺少 Windows 文件或验证通过。无法核对源码/产物身份时报告这一缺口。

原生测试验证 agent 与系统能力；完整 Linux → Windows SSH、共享 broker 同步和发行策略验收仍需控制端执行。Windows 本机测试通过不能自动把这些项目标为通过。

九、交付 Linux 控制端连接与联调信息

输出填写了已知值的 SSH config 片段，未知字段清楚标为待填，私钥路径仅由控制端持有人填写：

```sshconfig
Host rdev-win
    HostName <控制端可达的 Windows 地址>
    User <实际 Windows SSH 用户名>
    Port <实际端口>
    IdentityFile <控制端已有私钥路径，仅在控制端填写>
    IdentitiesOnly yes
    BatchMode yes
    StrictHostKeyChecking yes
    # 仅在已有网络路径确实要求时添加 ProxyJump。
```

提供主机公钥 SHA256 指纹与类型；控制端须先经可信渠道核对并登记 known_hosts。不要建议 StrictHostKeyChecking=no，也不能把未验证的 ssh-keyscan 结果直接当作可信身份。

给出以下控制端检查清单：

```sh
# 完成主机指纹核对、known_hosts 和 SSH alias 配置后：
ssh -T -o BatchMode=yes rdev-win whoami.exe

# 在包含当前 Phase 9 改动的 Linux rdev 工作区执行：
make GO=/实际Go路径 all daemon
RDEV_WINDOWS_SSH=rdev-win make GO=/实际Go路径 windows-remote-smoke
```

记录真实命令结果，尤其是 SSH 认证、自动安装、文件读写和后台任务重连。现有 windows-remote-smoke 不覆盖所有场景；还需控制端按 Phase 9 验收文档补充共享 broker 的同步计划/执行、取消和升级恢复验证。

控制端生产 release policy、broker principal、项目审批和签名信任不由 Windows 主机准备自动满足。现有 SSH smoke 使用隔离命名空间和明确指定的测试 artifact；其通过不代表生产签名发布认证通过，不要修改生产策略以绕过它。

十、输出结果与完成标准

在本次专用证据目录保存 Windows 准备报告、结构化检查结果、命令退出码、脱敏日志、配置变更清单和恢复说明。证据目录本身也应限制访问。控制台最后给出中文摘要和文件路径。

每个检查使用 PASS、FAIL、BLOCKED、PENDING 或 NOT_REQUIRED，并写明命令/证据、观察值和原因。PENDING 不等于 PASS。至少列出：

- os_architecture、powershell_51、encoded_command、clock、ntfs_and_capacity。
- sshd_installed、sshd_config_valid、sshd_running、ssh_listener、default_shell。
- target_account、public_key_installed、authorized_keys_acl、host_key_fingerprints。
- firewall_scope、controller_to_windows_network、batchmode_publickey_auth。
- user_profile、private_state_acl、trusted_ancestors、no_reparse_paths、temp_directory、work_directory。
- native_executable_allowed、binary_streams、native_test_suite。
- foreground_tree_cleanup、ssh_background_breakaway、background_reconnect_and_stop。
- shared_broker_sync、upgrade_recovery、artifact_identity。

最后按层分别给出状态：

1. HOST_PREPARED：本机系统、服务、账号、路径和必要配置检查完成。
2. SSH_READY：真实 Linux 控制端已完成网络、主机身份和 BatchMode 公钥登录验证。
3. RDEV_RUNTIME_VERIFIED：已经实际验证的 rdev 功能及证据；部分通过时必须列出未通过/未运行项目，不能给出笼统通过结论。

交接摘要必须包含 OS/Build、sshd 版本、地址/端口、用户名和 SID、主机公钥指纹、公钥是否已安装、默认 shell、UserProfile、状态路径、工作目录、测试产物摘要、检查结果、仍需控制端执行的动作，以及本次改动的恢复方式。

只清理本次创建且确认不再使用的临时文件和测试进程。保留接入所需账号、公钥、服务、目录和证据，不删除其他项目或历史任务。发现软件实现问题时，准确记录可复现步骤及日志，不要通过放宽机器安全配置来掩盖问题。
