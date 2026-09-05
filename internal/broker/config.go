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
	active   int
	done     chan struct{}
}

func NewConfigStore(c Config) (*ConfigStore, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &ConfigStore{cfg: c, done: make(chan struct{})}, nil
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

// BeginRequest admits one request while the broker is accepting work.
func (s *ConfigStore) BeginRequest() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.draining {
		return false
	}
	s.active++
	return true
}

// EndRequest marks a previously admitted request complete.
func (s *ConfigStore) EndRequest() {
	s.mu.Lock()
	if s.active > 0 {
		s.active--
		if s.active == 0 && s.draining {
			close(s.done)
		}
	}
	s.mu.Unlock()
}

func (s *ConfigStore) Drain(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	if s.draining {
		done := s.done
		s.mu.Unlock()
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.draining = true
	if s.active == 0 {
		close(s.done)
	}
	done := s.done
	s.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
