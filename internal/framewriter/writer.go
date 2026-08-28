// Package framewriter provides a bounded, priority-aware, single-owner writer.
// It is used on both sides of the rdev protocol so a blocked pipe cannot retain
// a mutex indefinitely or grow one goroutine per attempted write.
package framewriter

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrClosed       = errors.New("frame writer closed")
	ErrQueueFull    = errors.New("frame writer queue is full")
	ErrDropped      = errors.New("data frame dropped")
	ErrWriteTimeout = errors.New("frame write timed out")
)

// Priority controls dequeue order. A frame already inside the underlying
// Write cannot be preempted; the watchdog closes that writer if it stalls.
type Priority uint8

const (
	Data Priority = iota
	Control
	Critical
)

type Config struct {
	MaxFrames    int
	MaxBytes     int64
	WriteTimeout time.Duration
}

type queuedFrame struct {
	data []byte
	done chan error
}

type watchdogCommand struct {
	active bool
}

// Writer owns exactly one write-loop goroutine and one watchdog goroutine.
// Queues are never closed, which removes send-on-closed races; closure is
// represented by state guarded by mu and the broadcast-only done channel.
type Writer struct {
	out       io.Writer
	closeOut  func() error
	onFailure func(error)
	config    Config

	mu          sync.Mutex
	wake        chan struct{}
	done        chan struct{}
	workerDone  chan struct{}
	watchDone   chan struct{}
	watch       chan watchdogCommand
	queues      [3][]*queuedFrame
	queuedBytes int64
	inFlight    int64
	closed      bool
	err         error
	dropped     atomic.Int64
}

func New(out io.Writer, closeOut func() error, config Config, onFailure func(error)) *Writer {
	if config.MaxFrames <= 0 {
		config.MaxFrames = 128
	}
	if config.MaxBytes <= 0 {
		config.MaxBytes = 16 << 20
	}
	if config.WriteTimeout <= 0 {
		config.WriteTimeout = 2 * time.Second
	}
	w := &Writer{
		out: out, closeOut: closeOut, onFailure: onFailure, config: config,
		wake: make(chan struct{}, 1), done: make(chan struct{}),
		workerDone: make(chan struct{}), watchDone: make(chan struct{}),
		watch: make(chan watchdogCommand),
	}
	go w.watchdogLoop()
	go w.writeLoop()
	return w
}

// Write queues one frame and waits until the single writer has completed it.
// Queue admission itself never blocks. Data is dropped under pressure; losing a
// control frame instead tears down the polluted connection.
func (w *Writer) Write(ctx context.Context, data []byte, priority Priority) error {
	done := make(chan error, 1)
	frame, err := w.enqueue(data, priority, done)
	if err != nil {
		return err
	}
	select {
	case err := <-frame.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-w.done:
		return w.Err()
	}
}

// Enqueue queues a frame without waiting for the underlying write. It is used
// for best-effort cancel frames after the caller's context has already ended.
func (w *Writer) Enqueue(data []byte, priority Priority) error {
	_, err := w.enqueue(data, priority, nil)
	return err
}

func (w *Writer) enqueue(data []byte, priority Priority, done chan error) (*queuedFrame, error) {
	if priority > Critical {
		priority = Critical
	}
	size := int64(len(data))
	w.mu.Lock()
	if w.closed {
		err := w.err
		w.mu.Unlock()
		if err == nil {
			return nil, ErrClosed
		}
		return nil, err
	}
	frames := 0
	for i := range w.queues {
		frames += len(w.queues[i])
	}
	overBudget := size < 0 || size > w.config.MaxBytes ||
		w.queuedBytes > w.config.MaxBytes-size ||
		w.queuedBytes+w.inFlight+size > w.config.MaxBytes || frames >= w.config.MaxFrames
	if overBudget {
		w.mu.Unlock()
		if priority == Data {
			w.addDropped(size)
			return nil, ErrDropped
		}
		w.Fail(ErrQueueFull)
		return nil, ErrQueueFull
	}
	// Copy only after budget admission and while the reservation is protected by
	// the lock. Rejected callers therefore cannot transiently allocate one full
	// frame each outside the connection's total memory budget.
	frame := &queuedFrame{data: append([]byte(nil), data...), done: done}
	w.queues[priority] = append(w.queues[priority], frame)
	w.queuedBytes += size
	w.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
	return frame, nil
}

func (w *Writer) writeLoop() {
	defer close(w.workerDone)
	for {
		frame := w.next()
		if frame == nil {
			select {
			case <-w.wake:
				continue
			case <-w.done:
				return
			}
		}

		select {
		case w.watch <- watchdogCommand{active: true}:
		case <-w.done:
			w.finish(frame, w.Err())
			return
		}
		err := writeAll(w.out, frame.data)
		select {
		case w.watch <- watchdogCommand{}:
		case <-w.done:
		}
		w.finish(frame, err)
		if err != nil {
			w.Fail(err)
			return
		}
	}
}

func (w *Writer) next() *queuedFrame {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	for priority := int(Critical); priority >= int(Data); priority-- {
		queue := w.queues[priority]
		if len(queue) == 0 {
			continue
		}
		frame := queue[0]
		w.queues[priority] = queue[1:]
		size := int64(len(frame.data))
		w.queuedBytes -= size
		w.inFlight = size
		return frame
	}
	return nil
}

func (w *Writer) finish(frame *queuedFrame, err error) {
	w.mu.Lock()
	w.inFlight = 0
	w.mu.Unlock()
	if frame.done != nil {
		frame.done <- err
	}
}

func (w *Writer) watchdogLoop() {
	defer close(w.watchDone)
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	active := false
	for {
		var timeout <-chan time.Time
		if active {
			timeout = timer.C
		}
		select {
		case command := <-w.watch:
			if !timer.Stop() && active {
				select {
				case <-timer.C:
				default:
				}
			}
			active = command.active
			if active {
				timer.Reset(w.config.WriteTimeout)
			}
		case <-timeout:
			w.Fail(ErrWriteTimeout)
			return
		case <-w.done:
			return
		}
	}
}

// Fail closes the writer once, wakes every waiter, and asks the owner to tear
// down the rest of the connection. Callbacks run after state publication so
// even a slow Close cannot keep request goroutines asleep.
func (w *Writer) Fail(err error) {
	if err == nil {
		err = ErrClosed
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	w.err = err
	var waiting []*queuedFrame
	for i := range w.queues {
		waiting = append(waiting, w.queues[i]...)
		w.queues[i] = nil
	}
	w.queuedBytes = 0
	close(w.done)
	w.mu.Unlock()
	for _, frame := range waiting {
		if frame.done != nil {
			frame.done <- err
		}
	}
	if w.onFailure != nil {
		w.onFailure(err)
	}
	if w.closeOut != nil {
		_ = w.closeOut()
	}
}

func (w *Writer) Close() { w.Fail(ErrClosed) }

func (w *Writer) Done() <-chan struct{} { return w.done }

func (w *Writer) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err == nil {
		return ErrClosed
	}
	return w.err
}

func (w *Writer) DroppedBytes() int64 { return w.dropped.Load() }

func (w *Writer) addDropped(size int64) {
	const maxInt64 = int64(^uint64(0) >> 1)
	for {
		current := w.dropped.Load()
		next := maxInt64
		if size <= maxInt64-current {
			next = current + size
		}
		if w.dropped.CompareAndSwap(current, next) {
			return
		}
	}
}

func writeAll(w io.Writer, p []byte) error {
	for len(p) > 0 {
		n, err := w.Write(p)
		if err != nil {
			return err
		}
		if n <= 0 || n > len(p) {
			return io.ErrShortWrite
		}
		p = p[n:]
	}
	return nil
}
