package revent_sdk_go

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

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

func TestStateRunStopsOnContextCancelBeforeRun(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	s, errState := NewState(DefaultConfig())
	if errState != nil {
		t.Fatalf("NewState() error = %v, want nil", errState)
	}

	done := make(chan error, 1)

	go func() {
		s.stream = newFakeBidiStream(ctx)
		done <- s.start(ctx)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run() error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("run() did not stop after context cancellation")
	}
}

func TestStateRunStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	s, errState := NewState(DefaultConfig())
	if errState != nil {
		t.Fatalf("NewState() error = %v, want nil", errState)
	}

	done := make(chan error, 1)

	go func() {
		s.stream = newFakeBidiStream(ctx)
		done <- s.run(ctx)
	}()

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run() error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("run() did not stop after context cancellation")
	}
}

func TestStateRunReturnsRecvError(t *testing.T) {
	ctx := t.Context()
	expectedErr := errors.New("recv failure")
	stream := newFakeBidiStream(ctx)
	stream.recvFn = func() (*reventv1.ServerToClientMessage, error) {
		return nil, expectedErr
	}

	cfg := DefaultConfig()
	cfg.NumberOfRetries = 1

	s, err := NewState(cfg)
	if err != nil {
		t.Fatalf("NewState() error = %v, want nil", err)
	}

	s.stream = stream

	err = s.start(ctx)
	if err == nil {
		t.Fatal("run() error = nil, want recv error")
	}

	if !errors.Is(err, expectedErr) {
		t.Fatalf("run() error = %v, want wrapped %v", err, expectedErr)
	}
}

func TestStateRunStopsOnRecvEOF(t *testing.T) {
	ctx := t.Context()
	stream := newFakeBidiStream(ctx)
	stream.recvFn = func() (*reventv1.ServerToClientMessage, error) {
		return nil, io.EOF
	}

	cfg := DefaultConfig()
	cfg.NumberOfRetries = 1

	s, err := NewState(cfg)
	if err != nil {
		t.Fatalf("NewState() error = %v, want nil", err)
	}

	s.stream = stream

	err = s.start(ctx)

	var actualErr CantConnectToServerError
	if ok := errors.As(err, &actualErr); !ok {
		t.Fatalf("run() error = %v, want CantConnectToServerError", actualErr)
	}
}

func TestStateRunStopsOnRecvContextCanceledError(t *testing.T) {
	ctx := t.Context()
	stream := newFakeBidiStream(ctx)
	stream.recvFn = func() (*reventv1.ServerToClientMessage, error) {
		return nil, context.Canceled
	}

	s, err := NewState(DefaultConfig())
	if err != nil {
		t.Fatalf("NewState() error = %v, want nil", err)
	}

	s.stream = stream

	err = s.start(ctx)
	if err != nil {
		t.Fatalf("run() error = %v, want nil", err)
	}
}

func TestStateRunStopsOnRecvGRPCCanceledStatus(t *testing.T) {
	ctx := t.Context()
	stream := newFakeBidiStream(ctx)
	stream.recvFn = func() (*reventv1.ServerToClientMessage, error) {
		return nil, status.Error(codes.Canceled, "client canceled")
	}

	s, err := NewState(DefaultConfig())
	if err != nil {
		t.Fatalf("NewState() error = %v, want nil", err)
	}

	s.stream = stream

	err = s.start(ctx)
	if err != nil {
		t.Fatalf("start() error = %v, want nil", err)
	}
}

func TestStateRunSendsRegisterClientMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	stream := newFakeBidiStream(ctx)
	sentCh := make(chan *reventv1.ClientToServerMessage, 1)
	sentSignal := make(chan struct{})

	var sentOnce sync.Once

	stream.sendFn = func(msg *reventv1.ClientToServerMessage) error {
		sentCh <- msg

		sentOnce.Do(func() {
			close(sentSignal)
		})

		return nil
	}
	stream.recvFn = func() (*reventv1.ServerToClientMessage, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case _, ok := <-sentSignal:
			if ok {
				return nil, io.EOF
			}
		}

		//nolint:nilnil // false positive
		return nil, nil
	}

	cfg := DefaultConfig()

	s, err := NewState(cfg)
	if err != nil {
		t.Fatalf("NewState() error = %v, want nil", err)
	}

	s.stream = stream

	err = s.start(ctx)
	if err != nil {
		t.Fatalf("start() error = %v, want nil", err)
	}

	select {
	case msg := <-sentCh:
		registerPayload, ok := msg.Payload.(*reventv1.ClientToServerMessage_RegisterClient)
		if !ok {
			t.Fatalf("run() first send payload = %T, want RegisterClient", msg.Payload)
		}

		if registerPayload.RegisterClient.GetClientId() != cfg.ClientID.String() {
			t.Fatalf("RegisterClient.ClientId = %q, want %q",
				registerPayload.RegisterClient.GetClientId(),
				cfg.ClientID.String(),
			)
		}
	case <-time.After(time.Second):
		t.Fatal("run() did not send RegisterClient message")
	}
}

func TestOpenSessionCanOnlyBeCalledOncePerState(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	s, err := NewState(DefaultConfig())
	if err != nil {
		t.Fatalf("NewState() error = %v, want nil", err)
	}

	_ = OpenSession(ctx, s)

	err = OpenSession(ctx, s)
	if !errors.Is(err, ErrOpenSessionAlreadyCalled) {
		t.Fatalf("OpenSession() second call error = %v, want %v", err, ErrOpenSessionAlreadyCalled)
	}
}

func TestStateRunReconnectBypassesOpenSessionGuard(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	stream := newFakeBidiStream(ctx)
	stream.recvFn = func() (*reventv1.ServerToClientMessage, error) {
		cancel()

		return nil, io.EOF
	}

	s, err := NewState(DefaultConfig())
	if err != nil {
		t.Fatalf("NewState() error = %v, want nil", err)
	}

	// Simulate that public OpenSession was already called.
	s.openSessionCalled.Store(true)
	s.stream = stream

	err = s.run(ctx)
	if err == nil {
		t.Fatal("run() error = nil, want reconnect failure")
	}

	if errors.Is(err, ErrOpenSessionAlreadyCalled) {
		t.Fatalf("run() reconnect error = %v, should bypass OpenSession guard", err)
	}
}
