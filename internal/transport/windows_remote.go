package transport

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"unicode/utf16"

	"github.com/CIPFZ/rdev/internal/artifact"
	"github.com/CIPFZ/rdev/internal/buildinfo"
)

// powershellCommand has only fixed ASCII tokens and an encoded program. JSON
// values are separately base64 encoded so neither SSH default shell reparses
// user-controlled paths, decision records or native command lines.
func powershellCommand(script string, data any) []string {
	program := `$m=[IO.MemoryStream]::new([Convert]::FromBase64String('` + compressedPowerShell(script, data) + `')); $z=[IO.Compression.GZipStream]::new($m,[IO.Compression.CompressionMode]::Decompress); $r=[IO.StreamReader]::new($z,[Text.Encoding]::UTF8); & ([ScriptBlock]::Create($r.ReadToEnd()))`
	return encodedPowerShell(program)
}

func compressedPowerShell(script string, data any) string {
	b, _ := json.Marshal(data)
	program := `$ErrorActionPreference='Stop'; $ProgressPreference='SilentlyContinue'; [Console]::OutputEncoding=New-Object System.Text.UTF8Encoding($false); $p=([Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('` + base64.StdEncoding.EncodeToString(b) + `'))|ConvertFrom-Json); ` + script
	var compressed bytes.Buffer
	z := gzip.NewWriter(&compressed)
	_, _ = z.Write([]byte(program))
	_ = z.Close()
	return base64.StdEncoding.EncodeToString(compressed.Bytes())
}

func encodedPowerShell(program string) []string {
	units := utf16.Encode([]rune(program))
	encoded := make([]byte, len(units)*2)
	for i, u := range units {
		binary.LittleEndian.PutUint16(encoded[i*2:], u)
	}
	return []string{"powershell.exe", "-NoLogo", "-NoProfile", "-NonInteractive", "-EncodedCommand", base64.StdEncoding.EncodeToString(encoded)}
}

// The default Windows SSH shell, cmd.exe, limits command lines to 8191
// characters. Installation sends one bounded base64/gzip line followed by the
// executable bytes on stdin. Read the prefix without a text reader: read-ahead
// would consume binary bytes that the installer must receive unchanged.
const windowsInstallLoader = `$ErrorActionPreference='Stop'; $rdevBootstrapInput=[Console]::OpenStandardInput(); $line=[IO.MemoryStream]::new(); while($true){$b=$rdevBootstrapInput.ReadByte(); if($b -lt 0){throw 'missing bootstrap frame'}; if($b -eq 10){break}; if($line.Length -ge 65536){throw 'bootstrap frame exceeds bound'}; $line.WriteByte([byte]$b)}; $m=[IO.MemoryStream]::new([Convert]::FromBase64String([Text.Encoding]::ASCII.GetString($line.ToArray()))); $z=[IO.Compression.GZipStream]::new($m,[IO.Compression.CompressionMode]::Decompress); $r=[IO.StreamReader]::new($z,[Text.Encoding]::UTF8); & ([ScriptBlock]::Create($r.ReadToEnd()))`

func windowsInstallCommand(data any, executable []byte) ([]string, io.Reader, error) {
	prefix := compressedPowerShell(windowsInstallScript, data)
	if len(prefix) > 65536 {
		return nil, nil, errors.New("Windows bootstrap frame exceeds bound")
	}
	return encodedPowerShell(windowsInstallLoader), io.MultiReader(strings.NewReader(prefix+"\n"), bytes.NewReader(executable)), nil
}

func quoteWindowsArg(s string) string {
	var out strings.Builder
	out.WriteByte('"')
	slashes := 0
	for _, r := range s {
		if r == '\\' {
			slashes++
			continue
		}
		if r == '"' {
			out.WriteString(strings.Repeat(`\`, slashes*2+1))
		} else {
			out.WriteString(strings.Repeat(`\`, slashes))
		}
		slashes = 0
		out.WriteRune(r)
	}
	out.WriteString(strings.Repeat(`\`, slashes*2))
	out.WriteByte('"')
	return out.String()
}
func windowsArgs(args []string) string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = quoteWindowsArg(a)
	}
	return strings.Join(out, " ")
}

const windowsLaunchScript = `$s=New-Object System.Diagnostics.ProcessStartInfo; $s.FileName=$p.exe; $s.Arguments=$p.args; $s.UseShellExecute=$false; $s.CreateNoWindow=$true; $c=[Diagnostics.Process]::Start($s); $c.WaitForExit(); exit $c.ExitCode`

func windowsLaunch(exe string, args ...string) []string {
	return powershellCommand(windowsLaunchScript, map[string]string{"exe": exe, "args": windowsArgs(args)})
}

// Native bootstrap is limited to local drive paths and owner/SYSTEM access.
// Existing objects are checked, never repaired to legitimize a foreign owner.
const windowsPrivateScript = `
$sid=[Security.Principal.WindowsIdentity]::GetCurrent().User
function Check-Path([string]$v) {
 if ($v -notmatch '^[A-Za-z]:[\\/]' -or $v.Substring(2).Contains(':')) { throw 'local drive path required' }
 $q=[IO.Path]::GetFullPath($v)
 while ($q) {
  if ([IO.File]::Exists($q) -or [IO.Directory]::Exists($q)) {
   if (([IO.File]::GetAttributes($q) -band [IO.FileAttributes]::ReparsePoint) -ne 0) { throw 'reparse point refused' }
   if ([IO.Directory]::Exists($q)) {
    $a=Get-Acl -LiteralPath $q
    $trusted=@($sid.Value,'S-1-5-18','S-1-5-32-544','S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464')
    if ($a.GetOwner([Security.Principal.SecurityIdentifier]).Value -notin $trusted) { throw 'untrusted ancestor owner' }
    foreach($rule in $a.GetAccessRules($true,$true,[Security.Principal.SecurityIdentifier])) {
     if ($rule.AccessControlType -eq 'Allow' -and ($rule.PropagationFlags -band [Security.AccessControl.PropagationFlags]::InheritOnly) -eq 0 -and $rule.IdentityReference.Value -notin $trusted -and ([long]$rule.FileSystemRights -band 0x500D0152) -ne 0) { throw 'writable ancestor refused' }
    }
   }
  }
  $parent=[IO.Path]::GetDirectoryName($q); if ($parent -eq $q) { break }; $q=$parent
 }
}
function Check-Private([string]$v) {
 Check-Path $v
 $acl=Get-Acl -LiteralPath $v
 $owner=$acl.GetOwner([Security.Principal.SecurityIdentifier]).Value
 if ($owner -ne $sid.Value -and $owner -ne 'S-1-5-32-544') { throw 'foreign owner refused' }
 foreach ($rule in $acl.GetAccessRules($true,$true,[Security.Principal.SecurityIdentifier])) {
  if ($rule.AccessControlType -eq 'Allow' -and $rule.IdentityReference.Value -ne $sid.Value -and $rule.IdentityReference.Value -ne 'S-1-5-18') { throw 'public ACL refused' }
 }
}
function New-PrivateDirectory([string]$v) {
 Check-Path $v
 if (![IO.Directory]::Exists($v)) {
  $parent=[IO.Path]::GetDirectoryName($v); if (![IO.Directory]::Exists($parent)) { New-PrivateDirectory $parent }
  $acl=New-Object Security.AccessControl.DirectorySecurity
  $acl.SetSecurityDescriptorSddlForm(('O:'+$sid.Value+'D:P(A;OICI;FA;;;'+$sid.Value+')(A;OICI;FA;;;SY)'))
  [void][IO.Directory]::CreateDirectory($v,$acl)
 }
 Check-Private $v
}
`

func (c *Conn) probeWindows(ctx context.Context) (*remoteProbe, error) {
	remoteDir, err := ValidateRemoteDir(c.host.RemoteDir)
	if err != nil {
		return nil, err
	}
	script := windowsPrivateScript + `
$homeDir=[Environment]::GetFolderPath('UserProfile'); Check-Path $homeDir
$root=[IO.Path]::Combine($homeDir,$p.dir)
Write-Output 'rdev-os Windows'
Write-Output ('rdev-arch '+[Environment]::GetEnvironmentVariable('PROCESSOR_ARCHITECTURE'))
Write-Output ('rdev-home '+$homeDir)
if ([IO.Directory]::Exists($root)) {
 Check-Private $root
 $record=[IO.Path]::Combine($root,'.rdev-release.json')
 if ([IO.File]::Exists($record)) {
  Check-Private $record
  if ((Get-Item -LiteralPath $record).Length -gt 32768) { throw 'release record exceeds bound' }
  $d=([IO.File]::ReadAllText($record)|ConvertFrom-Json)
  if ($d.digest -cnotmatch '^[0-9a-f]{64}$') { throw 'invalid release digest' }
  $exe=[IO.Path]::Combine($root,('versions\rdev-agent-'+$d.digest+'.exe'))
  Check-Private $exe
  if ((Get-Item -LiteralPath $exe).Length -gt 67108864) { throw 'agent exceeds bound' }
  if ((Get-FileHash -LiteralPath $exe -Algorithm SHA256).Hash.ToLowerInvariant() -ne $d.digest) { throw 'active agent digest mismatch' }
  Write-Output ('rdev-sha '+$d.digest)
 }
}`
	out, err := c.runSSH(ctx, powershellCommand(script, map[string]string{"dir": strings.ReplaceAll(remoteDir, "/", `\`)})...)
	if err != nil {
		return nil, fmt.Errorf("Windows platform probe: %w", err)
	}
	return parseProbe(out)
}

func windowsStatePath(home, dir string) (string, error) {
	if len(home) < 3 || home[1] != ':' || (home[2] != '\\' && home[2] != '/') || strings.ContainsAny(home, "\x00\r\n") || strings.Contains(home[2:], ":") {
		return "", errors.New("invalid Windows home directory")
	}
	return strings.TrimRight(home, `\/`) + `\` + strings.ReplaceAll(dir, "/", `\`), nil
}
func windowsAgentPath(state, digest string) string {
	return state + `\versions\rdev-agent-` + digest + ".exe"
}

func (c *Conn) ensureWindowsAgent(ctx context.Context, bin *AgentBinary, want, installed string, decision artifact.Decision) error {
	if decision.Digest == "" {
		decision = artifact.Decision{Digest: want, Unsigned: true, Channel: "dev", Version: buildinfo.Current().Version}
	}
	if installed != "" {
		c.agentPath = windowsAgentPath(c.stateDir, installed)
		if !c.host.ForceAgentUpload && decision.Unsigned && installed != want {
			probeCtx, cancel := context.WithTimeout(ctx, agentVersionTimeout)
			out, err := c.runSSH(probeCtx, windowsLaunch(c.agentPath, "-version")...)
			cancel()
			if err == nil {
				if err = downgradeError(buildinfo.ParseVersionOutput(out), buildinfo.Current(), c.host); err != nil {
					return err
				}
			}
		}
		if installed == want {
			if bin.Authorize == nil {
				return nil
			}
			raw, _ := json.Marshal(decision)
			_, err := c.runSSH(ctx, windowsLaunch(c.agentPath, "-install-candidate", c.stateDir, string(raw), installed)...)
			if err != nil {
				return agentInstallError(err, err.Error())
			}
			return nil
		}
	}
	raw, _ := json.Marshal(decision)

	command, input, err := windowsInstallCommand(map[string]string{"root": c.stateDir, "digest": want, "args": windowsArgs([]string{"-install-candidate", c.stateDir, string(raw), installed})}, bin.Data)
	if err != nil {
		return err
	}
	args, err := c.sshArgs(command...)
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, "ssh", args...)
	cmd.Stdin = input
	var out boundedHeadBuilder
	errbuf := &lockedBuilder{}
	cmd.Stdout = &out
	cmd.Stderr = errbuf
	if err = cmd.Run(); err != nil {
		return agentInstallError(err, errbuf.String())
	}
	c.agentPath = windowsAgentPath(c.stateDir, want)
	return nil
}

type sshCommandError struct {
	cause   error
	message string
}

func (e *sshCommandError) Error() string { return e.message }
func (e *sshCommandError) Unwrap() error { return e.cause }
func remoteShellFailed(err error) bool {
	var e *exec.ExitError
	return errors.As(err, &e) && e.ExitCode() != 255
}

const windowsInstallScript = windowsPrivateScript + `
New-PrivateDirectory $p.root
New-PrivateDirectory ([IO.Path]::Combine($p.root,'jobs'))
$reservation=$null; $stage=$null
for ($i=0; $i -lt 4; $i++) {
 $candidate=[IO.Path]::Combine($p.root,('.rdev-upload-slot-'+$i))
 New-PrivateDirectory $candidate
 $marker=[IO.Path]::Combine($candidate,'reservation')
 if ([IO.File]::Exists($marker)) {
  $held=$null
  try {
   Check-Private $marker
   $held=[IO.FileStream]::new($marker,[IO.FileMode]::Open,[IO.FileAccess]::Read,[IO.FileShare]::None)
   if ($held.Length -gt 4096) { throw 'unknown reservation' }
   $reader=[IO.StreamReader]::new($held,[Text.Encoding]::UTF8,$true,4096,$true)
   try { $owner=($reader.ReadToEnd()|ConvertFrom-Json) } finally { $reader.Dispose() }
   if ($owner.pid -le 0 -or $owner.identity -cnotmatch '^windows:[0-9a-f]{16}$' -or ([DateTime]::UtcNow-[DateTime]::Parse($owner.started_at).ToUniversalTime()).TotalMinutes -lt 10) { throw 'reservation retained' }
   $alive=$null
   try { $alive=[Diagnostics.Process]::GetProcessById([int]$owner.pid) } catch [ArgumentException] {}
   if ($alive) { try { if (('windows:'+('{0:x16}' -f $alive.StartTime.ToFileTimeUtc())) -eq $owner.identity) { throw 'live reservation' } } finally { $alive.Dispose() } }
   foreach($entry in [IO.Directory]::GetFileSystemEntries($candidate)) { if ([IO.Path]::GetFileName($entry) -notin @('reservation','agent.exe') -or [IO.Directory]::Exists($entry)) { throw 'unknown upload object' }; Check-Private $entry }
   $old=[IO.Path]::Combine($candidate,'agent.exe'); if ([IO.File]::Exists($old)) { [IO.File]::Delete($old) }
   $held.Dispose(); $held=$null; [IO.File]::Delete($marker)
  } catch { } finally { if($held){$held.Dispose()} }
 }
 try {
  $reservation=[IO.FileStream]::new($marker,[IO.FileMode]::CreateNew,[IO.FileAccess]::Write,[IO.FileShare]::None)
  $self=[Diagnostics.Process]::GetCurrentProcess()
  try { $owner=@{pid=$PID;identity=('windows:'+('{0:x16}' -f $self.StartTime.ToFileTimeUtc()));started_at=[DateTime]::UtcNow.ToString('o')} } finally { $self.Dispose() }
  $ownerBytes=[Text.Encoding]::UTF8.GetBytes(($owner|ConvertTo-Json -Compress)); $reservation.Write($ownerBytes,0,$ownerBytes.Length); $reservation.Flush($true)
  $stage=$candidate; break
 } catch [IO.IOException] { if($reservation){$reservation.Dispose();$reservation=$null} }
}
if (!$stage) { throw 'RDEV_AGENT_INSTALL_NOT_SENT:upload_slots_full' }
$file=[IO.Path]::Combine($stage,'agent.exe'); $code=1
try {
 $f=New-Object IO.FileStream($file,[IO.FileMode]::CreateNew,[IO.FileAccess]::Write,[IO.FileShare]::None)
 try {
  $source=$rdevBootstrapInput; $buf=New-Object byte[] 65536; $total=0
  while (($n=$source.Read($buf,0,$buf.Length)) -gt 0) { $total+=$n; if ($total -gt 67108864) { throw 'agent upload exceeds bound' }; $f.Write($buf,0,$n) }
  $f.Flush($true)
 } finally { $f.Dispose() }
 Check-Private $file
 if ((Get-FileHash -LiteralPath $file -Algorithm SHA256).Hash.ToLowerInvariant() -ne $p.digest) { throw 'agent upload digest mismatch' }
 $s=New-Object Diagnostics.ProcessStartInfo; $s.FileName=$file; $s.Arguments=$p.args; $s.UseShellExecute=$false; $s.CreateNoWindow=$true
 $helper=[Diagnostics.Process]::Start($s); $helper.WaitForExit(); $code=$helper.ExitCode
} finally {
 $reservation.Dispose()
 if ([IO.File]::Exists($file)) { [IO.File]::Delete($file) }
 [IO.File]::Delete([IO.Path]::Combine($stage,'reservation'))
 [IO.Directory]::Delete($stage)
}
exit $code`
