package transport

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/binary"
	"io"
	"strings"
	"testing"
	"unicode/utf16"
)

func TestWindowsNativeArgumentQuoting(t *testing.T) {
	for _, tc := range []struct{ in, want string }{{"", `""`}, {"plain", `"plain"`}, {`C:\with space\`, `"C:\with space\\"`}, {`say "hello"`, `"say \"hello\""`}, {`a\"b`, `"a\\\"b"`}, {`$(not-run)&%PATH%`, `"$(not-run)&%PATH%"`}} {
		if got := quoteWindowsArg(tc.in); got != tc.want {
			t.Errorf("quote %q: got %q want %q", tc.in, got, tc.want)
		}
	}
}

func TestWindowsBootstrapEncodingAndShellLimit(t *testing.T) {
	value := `C:\Users\中文 space\';&$(not-run)\`
	args := powershellCommand(windowsPrivateScript+windowsLaunchScript, map[string]string{"exe": value, "args": windowsArgs([]string{value, "a\"b"})})
	binaryPayload := []byte{0, 10, 13, 255, 254, 128, 'M', 'Z'}
	installArgs, input, err := windowsInstallCommand(map[string]string{"root": value, "digest": strings.Repeat("a", 64), "args": windowsArgs([]string{"-install-candidate", value, `{"digest":"` + strings.Repeat("a", 64) + `","version":"0.1.0-dev.0","unsigned":true}`, strings.Repeat("b", 64)})}, binaryPayload)
	if err != nil {
		t.Fatal(err)
	}
	if len(strings.Join(installArgs, " ")) >= 8000 {
		t.Fatalf("Windows installer exceeds cmd.exe limit: %d", len(strings.Join(installArgs, " ")))
	}
	if len(strings.Join(args, " ")) >= 8000 {
		t.Fatal("encoded bootstrap exceeds cmd.exe command limit")
	}
	framed, err := io.ReadAll(input)
	if err != nil {
		t.Fatal(err)
	}
	prefix, binarySuffix, ok := bytes.Cut(framed, []byte{'\n'})
	if !ok || len(prefix) > 65536 || !bytes.Equal(binarySuffix, binaryPayload) {
		t.Fatal("installer framing changed the binary payload")
	}
	installProgram := decompressPowerShell(t, string(prefix))
	if !strings.Contains(installProgram, windowsInstallScript) || strings.Contains(installProgram, value) {
		t.Fatal("installer script/data boundary changed")
	}
	for _, a := range args {
		if strings.Contains(a, value) {
			t.Fatal("dynamic value escaped encoded payload")
		}
	}
	b, err := base64.StdEncoding.DecodeString(args[len(args)-1])
	if err != nil {
		t.Fatal(err)
	}
	u := make([]uint16, len(b)/2)
	for i := range u {
		u[i] = binary.LittleEndian.Uint16(b[2*i:])
	}
	loader := string(utf16.Decode(u))
	_, rest, ok := strings.Cut(loader, "FromBase64String('")
	if !ok {
		t.Fatal(loader)
	}
	encoded, _, _ := strings.Cut(rest, "')")
	script := decompressPowerShell(t, encoded)
	if !strings.Contains(script, windowsPrivateScript) || strings.Contains(script, value) {
		t.Fatal("bootstrap/data boundary changed")
	}
}

func decompressPowerShell(t *testing.T, encoded string) string {
	t.Helper()
	compressed, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatal(err)
	}
	z, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close()
	script, err := io.ReadAll(z)
	if err != nil {
		t.Fatal(err)
	}
	return string(script)
}

func TestWindowsProbeAndPaths(t *testing.T) {
	p, err := parseProbe("profile chatter\r\nrdev-os Windows\r\nrdev-arch AMD64\r\nrdev-home C:\\Users\\测试 user\r\nrdev-sha " + strings.Repeat("a", 64) + "\r\n")
	if err != nil || p.goos != "windows" || p.goarch != "amd64" {
		t.Fatalf("probe: %+v %v", p, err)
	}
	state, err := windowsStatePath(p.home, ".cache/rdev")
	if err != nil || state != `C:\Users\测试 user\.cache\rdev` {
		t.Fatalf("state: %q %v", state, err)
	}
	for _, home := range []string{"relative", `\\server\share`, "C:\\Users\\x\nextra", `C:\Users\x:stream`} {
		if _, err := windowsStatePath(home, ".cache/rdev"); err == nil {
			t.Errorf("accepted %q", home)
		}
	}
}
