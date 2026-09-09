package main

import (
	"context"
	"fmt"
	"os"

	"github.com/CIPFZ/rdev/internal/agentinstall"
)

func installCandidateCommand(args []string) int {
	if len(args) != 3 {
		fmt.Fprintln(os.Stderr, "RDEV_AGENT_INSTALL_NOT_SENT:arguments")
		return 1
	}
	decision, err := agentinstall.DecodeDecision(args[1])
	if err == nil {
		var self string
		self, err = os.Executable()
		if err == nil {
			err = agentinstall.Install(context.Background(), args[0], self, decision, args[2])
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, agentinstall.Marker(err))
		return 1
	}
	return 0
}

func recoverUpgradeCommand(args []string) int {
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "RDEV_AGENT_INSTALL_NOT_SENT:arguments")
		return 1
	}
	if err := agentinstall.Recover(context.Background(), args[0]); err != nil {
		fmt.Fprintln(os.Stderr, agentinstall.Marker(err))
		return 1
	}
	return 0
}
