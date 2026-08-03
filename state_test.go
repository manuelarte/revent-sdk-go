package revent_sdk_go

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"

	reventv1 "github.com/manuelarte/revent-sdk-go/internal/api/gRPC/revent/v1"
)

type fakeBidiStream struct {
	recvFn func() (*reventv1.ServerToClientMessage, error)
	sendFn func(*reventv1.ClientToServerMessage) error
}

func newFakeBidiStream(ctx context.Context) *fakeBidiStream {
	f := &fakeBidiStream{}
	f.recvFn = func() (*reventv1.ServerToClientMessage, error) {
		<-ctx.Done()

		return nil, ctx.Err()
	}
	f.sendFn = func(*reventv1.ClientToServerMessage) error {
		return nil
	}

	return f
}

func (f *fakeBidiStream) Send(m *reventv1.ClientToServerMessage) error {
	return f.sendFn(m)
}

func (f *fakeBidiStream) Recv() (*reventv1.ServerToClientMessage, error) {
	return f.recvFn()
}

func (f *fakeBidiStream) Header() (metadata.MD, error) {
	return metadata.MD{}, nil
}

func (f *fakeBidiStream) Trailer() metadata.MD {
	return metadata.MD{}
}

func (f *fakeBidiStream) CloseSend() error {
	return nil
}

func (f *fakeBidiStream) Context() context.Context {
	return context.Background()
}

func (f *fakeBidiStream) SendMsg(any) error {
	return nil
}

func (f *fakeBidiStream) RecvMsg(any) error {
	return nil
}

func TestStateStartStopsOnContextCancelBeforeStart(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	s, errState := NewState(DefaultConfig())
	if errState != nil {
		t.Fatalf("NewState() error = %v, want nil", errState)
	}

	err := s.start(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("start() error = %v, want %v", err, context.Canceled)
	}
}

func TestStateStartStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	s, errState := NewState(DefaultConfig())
	if errState != nil {
		t.Fatalf("NewState() error = %v, want nil", errState)
	}

	done := make(chan error, 1)

	go func() {
		done <- s.start(ctx)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("start() error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("start() did not stop after context cancellation")
	}
}

func TestStateStartReturnsCantConnectWhenRetriesExhausted(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()

	cfg := DefaultConfig()
	cfg.NumberOfRetries = 1

	s, err := NewState(cfg)
	if err != nil {
		t.Fatalf("NewState() error = %v, want nil", err)
	}

	err = s.start(ctx)
	if err == nil {
		t.Fatal("start() error = nil, want CantConnectToServerError")
	}

	var connectErr CantConnectToServerError
	if !errors.As(err, &connectErr) {
		t.Fatalf("start() error = %v, want CantConnectToServerError", err)
	}
}

func TestListenToStreamMarksDisconnectedOnRecvError(t *testing.T) {
	s, err := NewState(DefaultConfig())
	if err != nil {
		t.Fatalf("NewState() error = %v, want nil", err)
	}

	stream := newFakeBidiStream(t.Context())
	stream.recvFn = func() (*reventv1.ServerToClientMessage, error) {
		return nil, io.EOF
	}
	s.setStream(stream)
	s.setState(connectedState)

	s.listenToStream(t.Context())

	if got := s.getState(); got != disconnectedState {
		t.Fatalf("state after recv error = %q, want %q", got, disconnectedState)
	}
}

func TestListenToStreamStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	s, err := NewState(DefaultConfig())
	if err != nil {
		t.Fatalf("NewState() error = %v, want nil", err)
	}

	stream := newFakeBidiStream(ctx)
	s.stream = stream

	s.listenToStream(ctx)
}

func TestOpenSessionCanOnlyBeCalledOncePerState(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	s, err := NewState(DefaultConfig())
	if err != nil {
		t.Fatalf("NewState() error = %v, want nil", err)
	}

	err = OpenSession(ctx, s)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("OpenSession() first call error = %v, want %v", err, context.Canceled)
	}

	err = OpenSession(ctx, s)
	if err != nil {
		t.Fatalf("OpenSession() second call error = %v, want nil (idempotent)", err)
	}
}

func TestOpenSessionSecondCallDoesNotStartAgain(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	s, err := NewState(DefaultConfig())
	if err != nil {
		t.Fatalf("NewState() error = %v, want nil", err)
	}

	firstCtx, firstCancel := context.WithCancel(ctx)
	firstCancel()

	err = OpenSession(firstCtx, s)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("OpenSession() first call error = %v, want %v", err, context.Canceled)
	}

	err = OpenSession(ctx, s)
	if err != nil {
		t.Fatalf("OpenSession() second call error = %v, want nil", err)
	}
}
