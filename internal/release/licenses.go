package release

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
)

// Licenses collects unmodified distribution notices from the actual linked
// modules (including stdlib/toolchain). Nested notices cover bundled third-party
// code; the index includes source module/version and original relative path.
func Licenses(dir, goCommand string) error {
	mods := map[string]string{}
	for _, name := range append([]string{"rdev", "rdevd"}, AgentNames...) {
		a, err := artifact(dir, name, goToolVersion(goCommand))
		if err != nil {
			return err
		}
		mods["stdlib@"+a.Build.GoVersion] = ""
		for _, d := range a.Build.Deps {
			if d.Replace != nil {
				return errors.New("license collection refuses replacement modules")
			}
			mods[d.Path+"@"+d.Version] = ""
		}
	}
	keys := make([]string, 0, len(mods))
	for k := range mods {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out bytes.Buffer
	out.WriteString("rdev binary distribution — third-party licenses and notices\nGenerated from actual linked Go build information. Original texts follow.\n\n")
	own, err := os.ReadFile("LICENSE")
	if err != nil {
		return err
	}
	fmt.Fprintf(&out, "===== rdev / LICENSE =====\n%s\n", own)
	for _, key := range keys {
		var root string
		if strings.HasPrefix(key, "stdlib@") {
			b, err := exec.Command(goCommand, "env", "GOROOT").Output()
			if err != nil {
				return err
			}
			root = strings.TrimSpace(string(b))
		} else {
			b, err := exec.Command(goCommand, "mod", "download", "-json", key).Output()
			if err != nil {
				return err
			}
			var m struct {
				Dir   string
				Error string
			}
			if err = json.Unmarshal(b, &m); err != nil || m.Error != "" || m.Dir == "" {
				return errors.New("license module cannot be resolved")
			}
			root = m.Dir
		}
		var names []string
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if d.Name() == ".git" {
					return filepath.SkipDir
				}
				return nil
			}
			n := strings.ToUpper(d.Name())
			if n == "LICENSE" || n == "NOTICE" || n == "COPYING" || n == "COPYRIGHT" || n == "PATENTS" || n == "AUTHORS" || strings.HasPrefix(n, "LICENSE.") || strings.HasPrefix(n, "LICENSE-") || strings.HasPrefix(n, "NOTICE.") {
				if d.Type()&os.ModeSymlink != 0 {
					return errors.New("license symlink rejected")
				}
				names = append(names, path)
			}
			return nil
		})
		if err != nil {
			return err
		}
		if len(names) == 0 {
			return fmt.Errorf("no license material for %s", key)
		}
		sort.Strings(names)
		for _, p := range names {
			b, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			if len(b) > 1<<20 {
				return errors.New("license file exceeds limit")
			}
			rel, _ := filepath.Rel(root, p)
			fmt.Fprintf(&out, "\n===== %s / %s =====\n%s\n", key, filepath.ToSlash(rel), b)
			if out.Len() > 4<<20 {
				return errors.New("license bundle exceeds limit")
			}
		}
	}
	return os.WriteFile(filepath.Join(dir, "THIRD_PARTY_NOTICES.txt"), out.Bytes(), 0600)
}
func goToolVersion(goCommand string) string {
	b, e := exec.Command(goCommand, "env", "GOVERSION").Output()
	if e != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}
