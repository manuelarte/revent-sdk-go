package actions

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/manuelarte/revent-sdk-go/revent"
	"github.com/manuelarte/revent-sdk-go/revent/messages"
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
		response: &messages.ClientRegistrationResponseMsg{
			Msg: &messages.ClientRegisteredMsg{
				ClientID: "my-client",
			},
		},
	}
	registration := NewClientRegistration(slog.Default(), m)

	output, err := registration.Do(t.Context(), ClientRegistrationParams{
		ClientID:      "my-client",
		QueryHandlers: []revent.QueryID{},
	})
	if err != nil {
		t.Fatalf("Do() error = %v, want nil", err)
	}

	if output.Err != nil {
		t.Fatalf("Do() output.Err = %v, want nil", output.Err)
	}
}

func TestClientRegistrationDoServerError(t *testing.T) {
	m := &fakeRegistrationManager{
		response: &messages.ClientRegistrationResponseMsg{
			Err: &messages.ClientRegistrationErrorMsg{
				ClientID: "my-client",
				Reason:   "duplicate client id",
			},
		},
	}
	registration := NewClientRegistration(slog.Default(), m)

	_, err := registration.Do(t.Context(), ClientRegistrationParams{
		ClientID:      "my-client",
		QueryHandlers: []revent.QueryID{},
	})
	if err == nil {
		t.Fatal("Do() error = nil, want server error")
	}
}

func TestClientRegistrationDoTimeout(t *testing.T) {
	m := &fakeRegistrationManager{
		response: nil,
	}
	registration := NewClientRegistration(slog.Default(), m)

	newCtx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()

	_, err := registration.Do(newCtx, ClientRegistrationParams{
		ClientID:      "my-client",
		QueryHandlers: []revent.QueryID{},
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
		response: &messages.ClientRegistrationResponseMsg{
			Msg: &messages.ClientRegisteredMsg{
				ClientID: "other-client",
			},
		},
	}
	registration := NewClientRegistration(slog.Default(), m)

	newCtx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()

	_, err := registration.Do(newCtx, ClientRegistrationParams{
		ClientID:      "my-client",
		QueryHandlers: []revent.QueryID{},
	})
	if err == nil {
		t.Fatal("Do() error = nil, want timeout")
	}

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Do() error = %v, want wrapped deadline exceeded", err)
	}
}
