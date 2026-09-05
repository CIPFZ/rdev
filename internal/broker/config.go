package broker

import (
	"context"
	"errors"
	"sync"
	"time"
)

type Config struct {
	MaxHosts int
	IdleTTL  time.Duration
}

func (c Config) Validate() error {
	if c.MaxHosts < 1 {
		return errors.New("max_hosts must be positive")
	}
	if c.IdleTTL <= 0 {
		return errors.New("idle_ttl must be positive")
	}
	return nil
}

type ConfigStore struct {
	mu       sync.RWMutex
	cfg      Config
	draining bool
}

func NewConfigStore(c Config) (*ConfigStore, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &ConfigStore{cfg: c}, nil
}
func (s *ConfigStore) Get() Config { s.mu.RLock(); defer s.mu.RUnlock(); return s.cfg }
func (s *ConfigStore) Reload(c Config) error {
	if err := c.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.draining {
		s.mu.Unlock()
		return errors.New("broker draining")
	}
	s.cfg = c
	s.mu.Unlock()
	return nil
}
func (s *ConfigStore) Drain(ctx context.Context) error {
	s.mu.Lock()
	s.draining = true
	s.mu.Unlock()
	<-ctx.Done()
	return ctx.Err()
}
