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
