package revent_sdk_go

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/manuelarte/revent-sdk-go/logger"
	"github.com/manuelarte/revent-sdk-go/revent"
)

// testQueryInput implements json.Unmarshaler.
type testQueryInput struct {
	Value string `json:"value"`
}

func (t *testQueryInput) UnmarshalJSON(_ []byte) error {
	return nil
}

func (t *testQueryInput) MarshalJSON() ([]byte, error) {
	return json.Marshal(t)
}

// testQueryOutput implements json.Marshaler.
type testQueryOutput struct {
	Result string `json:"result"`
}

func (t testQueryOutput) MarshalJSON() ([]byte, error) {
	return json.Marshal(t)
}

func (t testQueryOutput) UnmarshalJSON(_ []byte) error {
	return nil
}

// Test NewState.
func TestNewState(t *testing.T) {
	cfg := DefaultConfig()

	s, err := NewState(cfg)
	if err != nil {
		t.Errorf("NewState() error = %v, want nil", err)
	}

	if s == nil {
		t.Error("NewState() returned nil State")
	}
}

func TestNewStateWithInvalidConfig(t *testing.T) {
	cfg := Config{
		ClientID: "",
		Logger:   &logger.EmptyLogger{},
	}

	s, err := NewState(cfg)
	if err == nil {
		t.Error("NewState() error = nil, want error for invalid config")
	}

	if s != nil {
		t.Error("NewState() should return nil State for invalid config")
	}
}

// Test RegisterQueryHandler.
func TestRegisterQueryHandler(t *testing.T) {
	s, err := NewState(DefaultConfig())
	if err != nil {
		t.Fatalf("NewState() error = %v", err)
	}

	queryID := revent.Query[*testQueryInput, testQueryOutput]("test-query-id")
	handler := func(ctx context.Context, params *testQueryInput) testQueryOutput {
		return testQueryOutput{}
	}

	err = RegisterQueryHandler(s, queryID, handler)
	if err != nil {
		t.Errorf("RegisterQueryHandler() first call error = %v, want nil", err)
	}

	// Attempting to register the same handler twice should error
	err = RegisterQueryHandler(s, queryID, handler)
	if err == nil {
		t.Error("RegisterQueryHandler() second call error = nil, want error for duplicate handler")
	}

	var queryErr revent.QueryHandlerAlreadyRegisteredError
	if !errors.As(err, &queryErr) {
		t.Errorf("RegisterQueryHandler() error type = %T, want QueryHandlerAlreadyRegisteredError", err)
	}
}

// Test OpenSession - basic smoke test.
func TestOpenSessionCanOnlyBeCalledOncePerState(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	cfg := DefaultConfig()

	s, err := NewState(cfg)
	if err != nil {
		t.Fatalf("NewState() error = %v", err)
	}

	// First call with canceled context
	err = OpenSession(ctx, s, cfg.GRPCCfg)
	if err == nil {
		t.Fatal("OpenSession() first call error = nil, want error for canceled context")
	}

	// Second call should return nil because sync.Once ensures it's only called once
	err = OpenSession(ctx, s, cfg.GRPCCfg)
	if err != nil {
		t.Errorf("OpenSession() second call error = %v, want nil (idempotent due to sync.Once)", err)
	}
}
