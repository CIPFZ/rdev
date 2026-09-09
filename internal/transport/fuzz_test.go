package transport

import (
	"bufio"
	"bytes"
	"encoding/json"
	"path"
	"strings"
	"testing"

	"github.com/CIPFZ/rdev/internal/proto"
)

func FuzzNDJSONAndFrame(f *testing.F) {
	f.Add([]byte("{\"id\":\"1\",\"ok\":true}\n"), uint16(128))
	f.Add([]byte("\r\n{}\n"), uint16(1))
	f.Add([]byte("unterminated"), uint16(16))
	f.Fuzz(func(t *testing.T, data []byte, rawLimit uint16) {
		if len(data) > 128<<10 {
			t.Skip()
		}
		limit := int(rawLimit) + 1
		r := bufio.NewReaderSize(bytes.NewReader(data), 4096)
		line, err := readLineLimit(r, limit)
		if err != nil {
			return
		}
		if len(line) > limit || bytes.ContainsRune(line, '\n') {
			t.Fatal("frame escaped its byte/record boundary")
		}
		var response proto.Response
		if json.Unmarshal(line, &response) != nil {
			return
		}
		progress := streamProgress{typed: true, operationID: "0123456789abcdef0123456789abcdef", streaming: true}
		terminal, err := validateResponseFrame(&response, progress)
		if err == nil && (response.OperationID != progress.operationID || response.ID == "" || terminal != response.Terminal) {
			t.Fatal("accepted unbound response")
		}
	})
}

func FuzzRemotePath(f *testing.F) {
	for _, s := range []string{"", "~/.cache/rdev", "../escape", "a//b", "/tmp/agent", "a;id", "a/雪"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, input string) {
		got, err := ValidateRemoteDir(input)
		if err != nil {
			return
		}
		if path.IsAbs(got) || path.Clean(got) != got || got == "." {
			t.Fatal("unsafe accepted remote path")
		}
		for _, component := range strings.Split(got, "/") {
			if component == ".." || component == "." || component == "" {
				t.Fatal("unsafe accepted remote component")
			}
		}
		again, err := ValidateRemoteDir(got)
		if err != nil || again != got {
			t.Fatal("path validation is not canonical")
		}
	})
}
