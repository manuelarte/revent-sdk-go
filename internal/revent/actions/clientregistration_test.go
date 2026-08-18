package actions

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/manuelarte/revent-sdk-go/internal/revent/messages"
	"github.com/manuelarte/revent-sdk-go/revent"
)

type fakeRegistrationManager struct {
	unsubscribeErr error
	predicate      func(msg messages.ServerMsg) bool
	ch             chan<- messages.ServerMsg
	response       messages.ServerMsg
}

func (f *fakeRegistrationManager) Send(msg messages.ClientMsg) error {
	// When a client registration message is sent, respond immediately if a response is configured
	if f.response != nil && (f.predicate == nil || f.predicate(f.response)) {
		select {
		case f.ch <- f.response:
		default:
		}
	}

	return nil
}

func (f *fakeRegistrationManager) Subscribe(
	_ uuid.UUID,
	pred func(msg messages.ServerMsg) bool,
	ch chan<- messages.ServerMsg,
) {
	f.predicate = pred
	f.ch = ch
}

func (f *fakeRegistrationManager) Unsubscribe(uuid.UUID) error {
	return f.unsubscribeErr
}

func TestClientRegistrationDoSuccess(t *testing.T) {
	m := &fakeRegistrationManager{
		response: &messages.ClientRegisteredMsg{
			ClientID: "my-client",
		},
	}
	registration := NewClientRegistration(m)

	output, err := registration.Do(t.Context(), ClientRegistrationParams{
		ClientID:      "my-client",
		QueryHandlers: make([]revent.QueryID, 0),
	})
	if err != nil {
		t.Fatalf("Do() error = %v, want nil", err)
	}

	if output == nil {
		t.Fatal("Do() output = nil, want successful response")
	}

	if output.Err != nil {
		t.Fatalf("Do() output.Err = %v, want nil", output.Err)
	}
}

func TestClientRegistrationDoServerError(t *testing.T) {
	m := &fakeRegistrationManager{
		response: &messages.ClientRegistrationErrorMsg{
			ClientID: "my-client",
			Reason:   "duplicate client id",
		},
	}
	registration := NewClientRegistration(m)

	output, err := registration.Do(t.Context(), ClientRegistrationParams{
		ClientID:      "my-client",
		QueryHandlers: make([]revent.QueryID, 0),
	})
	if err != nil {
		t.Fatalf("Do() error = %v, want nil", err)
	}

	if output == nil {
		t.Fatal("Do() output = nil, want response with server error")
	}

	if output.Err == nil {
		t.Fatal("Do() output.Err = nil, want server error")
	}

	if output.Err.Reason != "duplicate client id" {
		t.Fatalf("Do() output.Err.Reason = %q, want %q", output.Err.Reason, "duplicate client id")
	}
}

func TestClientRegistrationDoTimeout(t *testing.T) {
	m := &fakeRegistrationManager{
		response: nil,
	}
	registration := NewClientRegistration(m)

	newCtx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()

	_, err := registration.Do(newCtx, ClientRegistrationParams{
		ClientID:      "my-client",
		QueryHandlers: make([]revent.QueryID, 0),
	})
	if err == nil {
		t.Fatal("Do() error = nil, want timeout")
	}

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Do() error = %v, want wrapped deadline exceeded", err)
	}
}

func TestClientRegistrationDoTimeoutWhenMessageDoesNotMatchPredicate(t *testing.T) {
	m := &fakeRegistrationManager{
		response: &messages.ClientRegisteredMsg{
			ClientID: "other-client",
		},
	}
	registration := NewClientRegistration(m)

	newCtx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()

	_, err := registration.Do(newCtx, ClientRegistrationParams{
		ClientID:      "my-client",
		QueryHandlers: make([]revent.QueryID, 0),
	})
	if err == nil {
		t.Fatal("Do() error = nil, want timeout")
	}

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Do() error = %v, want wrapped deadline exceeded", err)
	}
}
