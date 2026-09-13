package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"time"

	"github.com/CIPFZ/rdev/internal/artifact"
	"github.com/CIPFZ/rdev/internal/client"
)

type doctorItem struct {
	Status string `json:"status"`
	Detail string `json:"detail,omitempty"`
	Action string `json:"action,omitempty"`
}
type doctorReport struct {
	Version      string      `json:"version"`
	Mode         string      `json:"mode"`
	Policy       doctorItem  `json:"policy"`
	Host         string      `json:"host,omitempty"`
	Connectivity doctorItem  `json:"connectivity,omitempty"`
	Capability   *doctorItem `json:"capability,omitempty"`
}

func cmdDoctor(ctx context.Context, c *client.Client, args []string) error {
	fs, err := parseFlags(args, "doctor")
	if err != nil {
		return err
	}
	if len(fs.pos) > 1 {
		return errors.New("usage: rdev doctor [host]")
	}
	p := artifact.DiagnosePolicy(time.Now())
	item := doctorItem{Status: "FAIL", Detail: p.Error, Action: p.Action}
	if p.Valid {
		item = doctorItem{Status: "PASS", Detail: "policy is valid"}
	}
	r := doctorReport{Version: "local", Mode: "standalone", Policy: item}
	if len(fs.pos) == 1 {
		r.Host = fs.pos[0]
		probe, e := c.CapabilityProbe(ctx, r.Host, false)
		if e != nil {
			r.Connectivity = doctorItem{Status: "FAIL", Detail: e.Error(), Action: "verify host trust and public-key authentication"}
		} else {
			r.Connectivity = doctorItem{Status: "PASS", Detail: "agent responded"}
			x := doctorItem{Status: "PASS", Detail: probe.ProbeVersion}
			r.Capability = &x
		}
	} else {
		r.Connectivity = doctorItem{Status: "SKIP", Detail: "no host supplied"}
	}
	return json.NewEncoder(os.Stdout).Encode(r)
}
