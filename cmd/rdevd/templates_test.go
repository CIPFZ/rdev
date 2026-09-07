package main

import (
	"encoding/xml"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestServiceTemplatesDeclareReadinessAndRestartPolicy(t *testing.T) {
	root := filepath.Join("..", "..")
	systemd, err := os.ReadFile(filepath.Join(root, "deploy", "systemd", "rdevd.service"))
	if err != nil {
		t.Fatal(err)
	}
	s := string(systemd)
	for _, required := range []string{
		"[Service]", "Type=simple", "ExecStart=", "-ready-file", "Restart=on-failure",
		"Environment=RDEV_AGENT_DIR=",
	} {
		if !strings.Contains(s, required) {
			t.Fatalf("systemd template missing %q", required)
		}
	}

	plist, err := os.ReadFile(filepath.Join(root, "deploy", "launchd", "com.cipfz.rdevd.plist"))
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		XMLName xml.Name
		Dict    struct {
			Raw string `xml:",innerxml"`
		} `xml:"dict"`
	}
	if err := xml.Unmarshal(plist, &document); err != nil {
		t.Fatalf("launchd plist is not XML: %v", err)
	}
	for _, required := range []string{"com.cipfz.rdevd", "ProgramArguments", "RunAtLoad", "KeepAlive", "RDEV_AGENT_DIR"} {
		if !strings.Contains(document.Dict.Raw, required) {
			t.Fatalf("launchd template missing %q", required)
		}
	}
}

func TestServiceTemplatesPassManagerSyntaxChecks(t *testing.T) {
	root := filepath.Join("..", "..")
	if tool, err := exec.LookPath("systemd-analyze"); err == nil {
		cmd := exec.Command(tool, "verify", filepath.Join(root, "deploy", "systemd", "rdevd.service"))
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("systemd-analyze verify failed: %v\n%s", err, out)
		}
	} else {
		t.Log("systemd-analyze unavailable; manager syntax check skipped")
	}
	if tool, err := exec.LookPath("plutil"); err == nil {
		cmd := exec.Command(tool, "-lint", filepath.Join(root, "deploy", "launchd", "com.cipfz.rdevd.plist"))
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("plutil -lint failed: %v\n%s", err, out)
		}
	} else {
		t.Log("plutil unavailable; launchd syntax check skipped")
	}
}
