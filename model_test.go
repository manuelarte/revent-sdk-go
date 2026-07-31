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

func TestStateInitStopsOnContextCancelBeforeInit(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	stream := newFakeBidiStream(ctx)
	s := NewState(DefaultConfig())

	done := make(chan error, 1)
	go func() {
		done <- s.init(ctx, stream)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Init() error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Init() did not stop after context cancellation")
	}
}

func TestStateInitStopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	stream := newFakeBidiStream(ctx)
	s := NewState(DefaultConfig())

	done := make(chan error, 1)
	go func() {
		done <- s.init(ctx, stream)
	}()

	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Init() error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Init() did not stop after context cancellation")
	}
}

func TestStateInitReturnsRecvError(t *testing.T) {
	ctx := t.Context()
	expectedErr := errors.New("recv failure")
	stream := newFakeBidiStream(ctx)
	stream.recvFn = func() (*reventv1.ServerToClientMessage, error) {
		return nil, expectedErr
	}

	s := NewState(DefaultConfig())

	err := s.init(ctx, stream)
	if err == nil {
		t.Fatal("Init() error = nil, want recv error")
	}

	if !errors.Is(err, expectedErr) {
		t.Fatalf("Init() error = %v, want wrapped %v", err, expectedErr)
	}
}

func TestStateInitStopsOnRecvEOF(t *testing.T) {
	ctx := t.Context()
	stream := newFakeBidiStream(ctx)
	stream.recvFn = func() (*reventv1.ServerToClientMessage, error) {
		return nil, io.EOF
	}

	s := NewState(DefaultConfig())

	err := s.init(ctx, stream)
	if err != nil {
		t.Fatalf("Init() error = %v, want nil", err)
	}
}

func TestStateInitStopsOnRecvContextCanceledError(t *testing.T) {
	ctx := t.Context()
	stream := newFakeBidiStream(ctx)
	stream.recvFn = func() (*reventv1.ServerToClientMessage, error) {
		return nil, context.Canceled
	}

	s := NewState(DefaultConfig())

	err := s.init(ctx, stream)
	if err != nil {
		t.Fatalf("Init() error = %v, want nil", err)
	}
}

func TestStateInitStopsOnRecvGRPCCanceledStatus(t *testing.T) {
	ctx := t.Context()
	stream := newFakeBidiStream(ctx)
	stream.recvFn = func() (*reventv1.ServerToClientMessage, error) {
		return nil, status.Error(codes.Canceled, "client canceled")
	}

	s := NewState(DefaultConfig())

	err := s.init(ctx, stream)
	if err != nil {
		t.Fatalf("Init() error = %v, want nil", err)
	}
}

func TestStateInitSendsRegisterClientMessage(t *testing.T) {
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
		case <-sentSignal:
			return nil, io.EOF
		}
	}

	cfg := DefaultConfig()
	s := NewState(cfg)

	err := s.init(ctx, stream)
	if err != nil {
		t.Fatalf("Init() error = %v, want nil", err)
	}

	select {
	case msg := <-sentCh:
		registerPayload, ok := msg.Payload.(*reventv1.ClientToServerMessage_RegisterClient)
		if !ok {
			t.Fatalf("Init() first send payload = %T, want RegisterClient", msg.Payload)
		}

		if registerPayload.RegisterClient.GetClientId() != cfg.ClientID.String() {
			t.Fatalf("RegisterClient.ClientId = %q, want %q",
				registerPayload.RegisterClient.GetClientId(),
				cfg.ClientID.String(),
			)
		}
	case <-time.After(time.Second):
		t.Fatal("Init() did not send RegisterClient message")
	}
}
