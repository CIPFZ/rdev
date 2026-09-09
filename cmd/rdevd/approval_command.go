package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/CIPFZ/rdev/internal/broker"
)

// approval-create submits the exact reviewed request with the administrator's
// principal credential. Neither the signing key nor approval token goes to logs.
func approvalCreateCommand(args []string, output io.Writer) error {
	flags := flag.NewFlagSet("approval-create", flag.ContinueOnError)
	socket := flags.String("socket", os.Getenv("RDEV_BROKER_SOCKET"), "broker socket")
	requestFile := flags.String("request-file", "", "0600 JSON ApprovalSpec containing the reviewed request")
	if err := parseSingleFlags(flags, args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *socket == "" || *requestFile == "" {
		return fmt.Errorf("approval-create requires -socket and -request-file")
	}
	data, err := broker.ReadPrivateFile(*requestFile, 4<<20)
	if err != nil {
		return err
	}
	if trimmed := bytes.TrimSpace(data); len(trimmed) == 0 || trimmed[0] != '{' {
		return fmt.Errorf("approval request requires an object")
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var spec broker.ApprovalSpec
	if err := dec.Decode(&spec); err != nil {
		return err
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("approval request requires exactly one object")
	}
	admin := broker.Owner{ClientID: os.Getenv("RDEV_CLIENT_ID"), ProjectID: os.Getenv("RDEV_PROJECT_ID")}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c, err := broker.DialClient(ctx, *socket, admin)
	if err != nil {
		return err
	}
	defer c.Close()
	response, err := c.DoContext(ctx, broker.Request{Operation: "approval.create", ApprovalSpec: &spec})
	if err != nil {
		return err
	}
	if !response.OK || response.Approval == nil {
		return fmt.Errorf("approval rejected: %s", response.Error)
	}
	return json.NewEncoder(output).Encode(response.Approval)
}
