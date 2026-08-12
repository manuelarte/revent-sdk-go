package flow

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	reventv1 "github.com/manuelarte/revent-sdk-go/internal/api/gRPC/revent/v1"
	"github.com/manuelarte/revent-sdk-go/revent"
)

type fakeRegistrationManager struct {
	registerErr    error
	subscribeErr   error
	unsubscribeErr error
	predicate      func(msg *reventv1.ServerToClientMessage) bool
	ch             chan<- *reventv1.ServerToClientMessage
	response       *reventv1.ServerToClientMessage
}

func (f *fakeRegistrationManager) Subscribe(
	_ uuid.UUID,
	pred func(msg *reventv1.ServerToClientMessage) bool,
	ch chan<- *reventv1.ServerToClientMessage,
) error {
	if f.subscribeErr != nil {
		return f.subscribeErr
	}

	f.predicate = pred
	f.ch = ch

	return nil
}

func (f *fakeRegistrationManager) Unsubscribe(uuid.UUID) error {
	return f.unsubscribeErr
}

func (f *fakeRegistrationManager) RegisterClient() error {
	if f.response != nil && (f.predicate == nil || f.predicate(f.response)) {
		select {
		case f.ch <- f.response:
		default:
		}
	}

	return f.registerErr
}

func (f *fakeRegistrationManager) QueryRequest(requestID revent.RequestID, queryID revent.QueryID) error {
	return nil
}

func (f *fakeRegistrationManager) Recv(msg *reventv1.ServerToClientMessage) {
	// No-op for testing
}

func TestClientRegistrationDoSuccess(t *testing.T) {
	m := &fakeRegistrationManager{
		response: &reventv1.ServerToClientMessage{
			Payload: &reventv1.ServerToClientMessage_ClientRegistered{
				ClientRegistered: &reventv1.ClientRegistered{ClientId: "my-client"},
			},
		},
	}
	registration := NewClientRegistration(slog.Default(), "my-client")

	err := registration.do(t.Context(), m)
	if err != nil {
		t.Fatalf("Do() error = %v, want nil", err)
	}
}

func TestClientRegistrationDoServerError(t *testing.T) {
	m := &fakeRegistrationManager{
		response: &reventv1.ServerToClientMessage{
			Payload: &reventv1.ServerToClientMessage_ClientRegistrationError{
				ClientRegistrationError: &reventv1.ClientRegistrationError{
					ClientId: "my-client",
					Reason:   "duplicate client id",
				},
			},
		},
	}
	registration := NewClientRegistration(slog.Default(), "my-client")

	err := registration.do(t.Context(), m)
	if err == nil {
		t.Fatal("Do() error = nil, want server error")
	}
}

func TestClientRegistrationDoTimeout(t *testing.T) {
	m := &fakeRegistrationManager{
		response: nil,
	}
	registration := NewClientRegistration(slog.Default(), "my-client")
	registration.timeout = 10 * time.Millisecond

	err := registration.do(t.Context(), m)
	if err == nil {
		t.Fatal("Do() error = nil, want timeout")
	}

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Do() error = %v, want wrapped deadline exceeded", err)
	}
}

func TestClientRegistrationDoTimeoutWhenMessageDoesNotMatchPredicate(t *testing.T) {
	m := &fakeRegistrationManager{
		response: &reventv1.ServerToClientMessage{
			Payload: &reventv1.ServerToClientMessage_ClientRegistered{
				ClientRegistered: &reventv1.ClientRegistered{ClientId: "other-client"},
			},
		},
	}
	registration := NewClientRegistration(slog.Default(), "my-client")
	registration.timeout = 10 * time.Millisecond

	err := registration.do(t.Context(), m)
	if err == nil {
		t.Fatal("Do() error = nil, want timeout")
	}

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Do() error = %v, want wrapped deadline exceeded", err)
	}
}
