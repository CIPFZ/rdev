package client

import (
	"context"
	"errors"

	"github.com/CIPFZ/rdev/internal/observe"
	"github.com/CIPFZ/rdev/internal/proto"
)

// ReadPrincipalSecret returns prospective plaintext only to the broker's secret
// admission path. It cannot redact an already registered value before validating
// a repeated import. Fixed errors prevent prospective credentials in diagnostics.
// The caller must authorize both import and file-read before invoking it.
func (c *Client) ReadPrincipalSecret(ctx context.Context, host, target, clientID, projectID, path string) (string, error) {
	unavailable := errors.New("remote secret source unavailable or invalid")
	if target == "" || clientID == "" || projectID == "" || path == "" {
		return "", unavailable
	}
	pooled, _, release, err := c.leasedBulkConn(ctx, host, target)
	if err != nil {
		return "", unavailable
	}
	defer release()
	negotiated, ok := pooled.conn.(negotiatedConnection)
	if !ok || negotiated.NegotiatedVersion() < 3 {
		return "", unavailable
	}
	wire := &proto.Request{Op: proto.OpReadFile, ProjectID: projectID, Read: &proto.ReadParams{Path: path, Limit: maxSecretFileBytes + 1}}
	response, err := c.doRawOnConnectionAs(ctx, pooled.conn, wire, proto.PrincipalID(clientID, projectID))
	if err != nil || response == nil || !response.OK {
		return "", unavailable
	}
	if response.Read != nil {
		observe.RecordBulkPayload(ctx, uint64(len(response.Read.Content)))
	}
	value, _, err := validateSecretRead(response.Read)
	if err != nil {
		return "", unavailable
	}
	return value, nil
}
