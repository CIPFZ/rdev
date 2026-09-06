package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sync"

	"github.com/CIPFZ/rdev/internal/proto"
)

// Client is the local frontend for the rdevd broker. One instance owns one
// authenticated Unix connection and serializes request/response framing.
type Client struct {
	conn  net.Conn
	owner Owner
	mu    sync.Mutex
	seq   uint64
}

func DialClient(ctx context.Context, socket string, owner Owner) (*Client, error) {
	if err := owner.Validate(); err != nil {
		return nil, err
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", socket)
	if err != nil {
		return nil, err
	}
	hello := proto.BrokerHello{Version: proto.BrokerProtocolVersion, MinVersion: proto.BrokerMinVersion, ClientID: owner.ClientID, ProjectID: owner.ProjectID}
	if err := json.NewEncoder(conn).Encode(hello); err != nil {
		_ = conn.Close()
		return nil, err
	}
	var response proto.BrokerHelloResponse
	if err := json.NewDecoder(conn).Decode(&response); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if !response.OK {
		_ = conn.Close()
		return nil, fmt.Errorf("broker handshake rejected: %s", response.Error)
	}
	return &Client{conn: conn, owner: owner}, nil
}

func (c *Client) Do(req Request) (Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return Response{}, fmt.Errorf("broker client closed")
	}
	c.seq++
	if req.ID == "" {
		req.ID = fmt.Sprintf("broker-%d", c.seq)
	}
	if req.Owner == (Owner{}) {
		req.Owner = c.owner
	}
	if err := req.Owner.Validate(); err != nil {
		return Response{}, err
	}
	if err := json.NewEncoder(c.conn).Encode(req); err != nil {
		return Response{}, err
	}
	var response Response
	if err := json.NewDecoder(c.conn).Decode(&response); err != nil {
		return Response{}, err
	}
	return response, nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}
