package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
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

func DialClient(ctx context.Context, socket string, owner Owner) (client *Client, callErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := owner.Validate(); err != nil {
		return nil, err
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", socket)
	if err != nil {
		return nil, err
	}
	// DialContext stops governing the connection after connect. Keep cancellation
	// active through both hello writes and reads, including a broker that has
	// opened its socket but has not finished startup recovery.
	stopWatch, watchDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(watchDone)
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-stopWatch:
		}
	}()
	defer func() {
		close(stopWatch)
		<-watchDone
		// Never let a late handshake watcher close a successfully returned
		// connection after ownership has passed to the caller.
		if err := ctx.Err(); err != nil {
			_ = conn.Close()
			client, callErr = nil, err
		}
	}()
	hello := proto.BrokerHello{Version: proto.BrokerProtocolVersion, MinVersion: proto.BrokerMinVersion, ClientID: owner.ClientID, ProjectID: owner.ProjectID}
	hello.PrincipalToken = os.Getenv("RDEV_PRINCIPAL_TOKEN")
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
	if err := proto.ValidateBrokerHello(hello, proto.BrokerHello{Version: response.Version, MinVersion: response.MinVersion}); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("broker handshake rejected: %w", err)
	}
	return &Client{conn: conn, owner: owner}, nil
}

func (c *Client) Do(req Request) (Response, error) {
	return c.DoContext(context.Background(), req)
}

// DoContext sends one request and tears down this local broker connection when
// ctx is canceled. The remote transport remains owned by rdevd and is not
// affected by cancellation of this frontend connection.
func (c *Client) DoContext(ctx context.Context, req Request) (result Response, callErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
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
	if req.Approval == "" && (!isFleetOperation(req.Operation) || req.Operation == "fleet.execute") {
		req.Approval = os.Getenv("RDEV_APPROVAL_TOKEN")
	}
	if isSecretMutation(req.Operation) || isSyncOperation(req.Operation) && req.Sync != nil && !req.Sync.DryRun {
		if req.OperationID == "" {
			req.OperationID = os.Getenv("RDEV_OPERATION_ID")
		}
		if req.OperationID == "" {
			var err error
			req.OperationID, err = proto.NewOperationID()
			if err != nil {
				return Response{}, err
			}
		}
		if proto.ValidateOperationID(req.OperationID) != nil {
			return Response{}, fmt.Errorf("invalid mutation operation ID")
		}
		defer func() {
			if callErr != nil {
				callErr = fmt.Errorf("mutation %s: %w", req.OperationID, callErr)
			}
		}()
	}
	if IsWireMutation(req) {
		wire := *req.Wire
		if wire.OperationID == "" {
			wire.OperationID = os.Getenv("RDEV_OPERATION_ID")
		}
		if wire.OperationID == "" {
			id, err := proto.NewOperationID()
			if err != nil {
				return Response{}, err
			}
			wire.OperationID = id
		}
		if proto.ValidateOperationID(wire.OperationID) != nil {
			return Response{}, fmt.Errorf("invalid mutation operation ID")
		}
		req.Wire = &wire
		defer func() {
			if callErr != nil {
				callErr = fmt.Errorf("mutation %s: %w", wire.OperationID, callErr)
			}
		}()
	}
	if err := req.Owner.Validate(); err != nil {
		return Response{}, err
	}
	stopWatch := make(chan struct{})
	go func(conn net.Conn) {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-stopWatch:
		}
	}(c.conn)
	defer close(stopWatch)
	if err := json.NewEncoder(c.conn).Encode(req); err != nil {
		if ctx.Err() != nil {
			c.conn = nil
			return Response{}, ctx.Err()
		}
		return Response{}, err
	}
	var response Response
	if err := json.NewDecoder(c.conn).Decode(&response); err != nil {
		if ctx.Err() != nil {
			c.conn = nil
			return Response{}, ctx.Err()
		}
		_ = c.conn.Close()
		c.conn = nil
		var syntax *json.SyntaxError
		var shape *json.UnmarshalTypeError
		if errors.Is(err, errInvalidBrokerResponse) || errors.As(err, &syntax) || errors.As(err, &shape) {
			return Response{}, proto.NewError(proto.CodeInvalidFrame, requestOperationID(req), proto.StatePossiblyExecuted)
		}
		// EOF/deadline/network failures are transport errors. They cannot
		// establish that an operation was not sent or completed.
		return Response{}, err
	}
	if err := response.validateErrorBinding(req); err != nil {
		_ = c.conn.Close()
		c.conn = nil
		return Response{}, proto.NewError(proto.CodeInvalidFrame, requestOperationID(req), proto.StatePossiblyExecuted)
	}
	if !response.OK && response.Mutation != nil && response.ErrorEnvelope == nil {
		response.Error = fmt.Sprintf("mutation %s [%s]: %s", response.Mutation.OperationID, response.Mutation.State, response.Error)
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
