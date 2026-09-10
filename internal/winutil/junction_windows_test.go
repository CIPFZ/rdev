package winutil

import "os/exec"

func execCommandJunction(link, target string) *exec.Cmd {
	return exec.Command("cmd.exe", "/d", "/c", "mklink", "/J", link, target)
}
