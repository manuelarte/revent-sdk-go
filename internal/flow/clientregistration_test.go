package flow

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	reventv1 "github.com/manuelarte/revent-sdk-go/internal/api/gRPC/revent/v1"
)

type fakeRegistrationManager struct {
	sendErr      error
	waitResponse ClientRegistrationResponse
	waitErr      error
	sent         *reventv1.ClientToServerMessage
}

func (f *fakeRegistrationManager) Send(msg *reventv1.ClientToServerMessage) error {
	f.sent = msg

	return f.sendErr
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

	if m.sent == nil {
		t.Fatal("Send() was not called")
	}

	payload := m.sent.GetRegisterClient()
	if payload == nil {
		t.Fatal("sent payload is not RegisterClient")
	}

	if got := payload.GetClientId(); got != "my-client" {
		t.Fatalf("RegisterClient.ClientId = %q, want %q", got, "my-client")
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
