package broker

import (
	"testing"
	"time"
)

func TestLeaseGraceAndInflight(t *testing.T) {
	l := NewLease(time.Second)
	l.Attach()
	l.Begin()
	l.Detach()
	if l.Reapable(time.Now().Add(2 * time.Second)) {
		t.Fatal("inflight lease reapable")
	}
	l.End()
	if !l.Reapable(time.Now().Add(2 * time.Second)) {
		t.Fatal("lease should be reapable")
	}
}

func TestLeaseDetachesAtomicallyButDoesNotBlockAdmissionDuringCleanup(t *testing.T) {
	l := NewLease(time.Millisecond)
	detached, releaseDetach, cleanup, releaseCleanup := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	done := make(chan bool, 1)
	go func() {
		done <- l.Reap(time.Now().Add(time.Second), func() func() {
			close(detached)
			<-releaseDetach
			return func() { close(cleanup); <-releaseCleanup }
		})
	}()
	<-detached
	attached := make(chan struct{})
	go func() { l.Attach(); l.Begin(); close(attached) }()
	select {
	case <-attached:
		t.Error("new admission raced pool detachment")
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseDetach)
	<-cleanup
	select {
	case <-attached:
	case <-time.After(time.Second):
		t.Error("old transport cleanup blocked new admission")
	}
	close(releaseCleanup)
	if !<-done {
		t.Fatal("idle pool was not detached")
	}
	if l.Reap(time.Now().Add(time.Hour), func() func() { t.Error("active pool reaped"); return func() {} }) {
		t.Fatal("active lease reaped")
	}
	l.End()
	l.Detach()
	if !l.Reap(time.Now().Add(time.Hour), func() func() { return func() {} }) {
		t.Fatal("new idle generation not reaped")
	}
	if l.Reap(time.Now().Add(time.Hour), func() func() { t.Error("idle generation reaped twice"); return func() {} }) {
		t.Fatal("duplicate reaping")
	}
}
