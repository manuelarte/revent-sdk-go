package flow

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"
)

type fakeRegistrationManager struct {
	registerErr  error
	waitResponse ClientRegistrationResponse
	waitErr      error
	registeredID string
}

func (f *fakeRegistrationManager) RegisterClient(clientID string) error {
	f.registeredID = clientID

	return f.registerErr
}

func (f *fakeRegistrationManager) WaitForClientRegistration(context.Context) (ClientRegistrationResponse, error) {
	if f.waitErr != nil {
		return ClientRegistrationResponse{}, f.waitErr
	}

	return f.waitResponse, nil
}

func TestClientRegistrationDoSuccess(t *testing.T) {
	m := &fakeRegistrationManager{
		waitResponse: ClientRegistrationResponse{ClientID: "my-client"},
	}
	registration := NewClientRegistration(slog.Default(), "my-client")

	err := registration.Do(t.Context(), m)
	if err != nil {
		t.Fatalf("Do() error = %v, want nil", err)
	}

	if m.registeredID == "" {
		t.Fatal("RegisterClient() was not called")
	}

	if m.registeredID != "my-client" {
		t.Fatalf("RegisterClient() clientID = %q, want %q", m.registeredID, "my-client")
	}
}

func TestClientRegistrationDoServerError(t *testing.T) {
	m := &fakeRegistrationManager{
		waitResponse: ClientRegistrationResponse{
			ClientID: "my-client",
			Err: ClientRegistrationRejectedError{
				ClientID: "my-client",
				Reason:   "duplicate client id",
			},
		},
	}
	registration := NewClientRegistration(slog.Default(), "my-client")

	err := registration.Do(t.Context(), m)
	if err == nil {
		t.Fatal("Do() error = nil, want server error")
	}
}

func TestClientRegistrationDoTimeout(t *testing.T) {
	m := &fakeRegistrationManager{
		waitErr: context.DeadlineExceeded,
	}
	registration := NewClientRegistration(slog.Default(), "my-client")
	registration.timeout = 10 * time.Millisecond

	err := registration.Do(t.Context(), m)
	if err == nil {
		t.Fatal("Do() error = nil, want timeout")
	}

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Do() error = %v, want wrapped deadline exceeded", err)
	}
}

func TestClientRegistrationDoUnexpectedClientID(t *testing.T) {
	m := &fakeRegistrationManager{
		waitResponse: ClientRegistrationResponse{ClientID: "other-client"},
	}
	registration := NewClientRegistration(slog.Default(), "my-client")

	err := registration.Do(t.Context(), m)
	if err == nil {
		t.Fatal("Do() error = nil, want unexpected client id error")
	}
}
