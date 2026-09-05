package broker

import "github.com/CIPFZ/rdev/internal/proto"

type Request struct {
	ID        string         `json:"id"`
	Owner     Owner          `json:"owner"`
	Operation string         `json:"operation"`
	Host      string         `json:"host,omitempty"`
	Wire      *proto.Request `json:"wire,omitempty"`
}
type Response struct {
	ID    string          `json:"id"`
	OK    bool            `json:"ok"`
	Error string          `json:"error,omitempty"`
	Wire  *proto.Response `json:"wire,omitempty"`
}
