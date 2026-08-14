package actions

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/manuelarte/revent-sdk-go/revent"
)

type fakeRegistrationManager struct {
	subscribeErr   error
	unsubscribeErr error
	predicate      func(msg revent.ServerMsg) bool
	ch             chan<- revent.ServerMsg
	response       revent.ServerMsg
}

func (f *fakeRegistrationManager) Send(msg revent.ClientMsg) error {
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
	pred func(msg revent.ServerMsg) bool,
	ch chan<- revent.ServerMsg,
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

func TestClientRegistrationDoSuccess(t *testing.T) {
	m := &fakeRegistrationManager{
		response: &revent.ClientRegisteredMsg{
			ClientID: "my-client",
		},
	}
	registration := NewClientRegistration(slog.Default(), m)

	err := registration.Do(t.Context(), ClientRegistrationParams{
		ClientID:      "my-client",
		QueryHandlers: []revent.QueryID{},
	})
	if err != nil {
		t.Fatalf("Do() error = %v, want nil", err)
	}
}

func TestClientRegistrationDoServerError(t *testing.T) {
	m := &fakeRegistrationManager{
		response: &revent.ClientRegistrationErrorMsg{
			ClientID: "my-client",
			Reason:   "duplicate client id",
		},
	}
	registration := NewClientRegistration(slog.Default(), m)

	err := registration.Do(t.Context(), ClientRegistrationParams{
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

	err := registration.Do(newCtx, ClientRegistrationParams{
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
		response: &revent.ClientRegisteredMsg{
			ClientID: "other-client",
		},
	}
	registration := NewClientRegistration(slog.Default(), m)

	newCtx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()

	err := registration.Do(newCtx, ClientRegistrationParams{
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
