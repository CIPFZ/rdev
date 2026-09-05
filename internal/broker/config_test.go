package broker

import (
	"context"
	"testing"
	"time"
)

func TestConfigReloadKeepsOldOnFailure(t *testing.T) {
	s, _ := NewConfigStore(Config{MaxHosts: 1, IdleTTL: time.Second})
	if err := s.Reload(Config{}); err == nil {
		t.Fatal("invalid reload accepted")
	}
	if s.Get().MaxHosts != 1 {
		t.Fatal("old config lost")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_ = s.Drain(ctx)
}

func TestConfigDrainWaitsForAdmittedRequests(t *testing.T) {
	s, err := NewConfigStore(Config{MaxHosts: 1, IdleTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if !s.BeginRequest() {
		t.Fatal("request was not admitted")
	}
	done := make(chan error, 1)
	go func() { done <- s.Drain(context.Background()) }()
	select {
	case err := <-done:
		t.Fatalf("drain completed before request: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	s.EndRequest()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if s.BeginRequest() {
		t.Fatal("request admitted after drain")
	}
}
