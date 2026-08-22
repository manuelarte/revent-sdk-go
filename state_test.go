package revent_sdk_go

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/manuelarte/revent-sdk-go/internal/revent/messages"
	"github.com/manuelarte/revent-sdk-go/internal/txrx"
	"github.com/manuelarte/revent-sdk-go/logger"
	"github.com/manuelarte/revent-sdk-go/revent"
)

// testQueryInput implements json.Unmarshaler.
type testQueryInput struct {
	Value string `json:"value"`
}

func (t *testQueryInput) UnmarshalJSON(b []byte) error {
	type alias testQueryInput

	return json.Unmarshal(b, (*alias)(t))
}

func (t *testQueryInput) MarshalJSON() ([]byte, error) {
	type alias testQueryInput

	return json.Marshal((*alias)(t))
}

// testQueryOutput implements json.Marshaler.
type testQueryOutput struct {
	Result string `json:"result"`
}

func (t *testQueryOutput) MarshalJSON() ([]byte, error) {
	type alias testQueryOutput

	return json.Marshal((*alias)(t))
}

func (t *testQueryOutput) UnmarshalJSON(b []byte) error {
	type alias testQueryOutput

	return json.Unmarshal(b, (*alias)(t))
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

	queryID := revent.Query[*testQueryInput, *testQueryOutput]("test-query-id")
	handler := func(ctx context.Context, params *testQueryInput) *testQueryOutput {
		return &testQueryOutput{}
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

type mockTxRx struct {
	incoming     chan messages.ServerMsg
	sessionEvent chan txrx.SessionEvent
	sentMsgs     chan messages.ClientMsg
}

func newMockTxRx() *mockTxRx {
	return &mockTxRx{
		incoming:     make(chan messages.ServerMsg, 10),
		sessionEvent: make(chan txrx.SessionEvent, 10),
		sentMsgs:     make(chan messages.ClientMsg, 10),
	}
}

func (m *mockTxRx) Send(msg messages.ClientMsg) error {
	m.sentMsgs <- msg

	return nil
}

func (m *mockTxRx) Incoming() <-chan messages.ServerMsg {
	return m.incoming
}

func (m *mockTxRx) SessionEvent() <-chan txrx.SessionEvent {
	return m.sessionEvent
}

func TestServerManagerSend(t *testing.T) {
	s, err := NewState(DefaultConfig())
	if err != nil {
		t.Fatalf("NewState() error = %v", err)
	}

	// Send when not started should return ErrStreamClosed
	err = s.Send(&messages.ClientRegistrationMsg{ClientID: "test"})
	if !errors.Is(err, txrx.ErrStreamClosed) {
		t.Errorf("Send() error = %v, want %v", err, txrx.ErrStreamClosed)
	}

	mock := newMockTxRx()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startErrCh := make(chan error, 1)
	go func() {
		startErrCh <- s.start(ctx, func() (txrx.TxRx, error) {
			return mock, nil
		})
	}()

	// Allow start goroutine to initialize
	time.Sleep(50 * time.Millisecond)

	msgToSend := &messages.ClientRegistrationMsg{ClientID: "test-client"}
	err = s.Send(msgToSend)
	if err != nil {
		t.Errorf("Send() error = %v, want nil", err)
	}

	select {
	case sent := <-mock.sentMsgs:
		if sent != msgToSend {
			t.Errorf("Send() sent = %+v, want %+v", sent, msgToSend)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message to be sent")
	}

	cancel()
	<-startErrCh
}

func TestQueryHandlingAndResponding(t *testing.T) {
	s, err := NewState(DefaultConfig())
	if err != nil {
		t.Fatalf("NewState() error = %v", err)
	}

	queryID := revent.Query[*testQueryInput, *testQueryOutput]("getUser")
	handlerCalled := make(chan *testQueryInput, 1)

	err = RegisterQueryHandler(s, queryID, func(ctx context.Context, params *testQueryInput) *testQueryOutput {
		handlerCalled <- params

		return &testQueryOutput{Result: "hello-" + params.Value}
	})
	if err != nil {
		t.Fatalf("RegisterQueryHandler() error = %v", err)
	}

	mock := newMockTxRx()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startErrCh := make(chan error, 1)

	go func() {
		startErrCh <- s.start(ctx, func() (txrx.TxRx, error) {
			return mock, nil
		})
	}()

	requestUUID := uuid.New()
	reqMsg := &messages.QueryRequestedMsg{
		RequestID:  revent.RequestID(requestUUID),
		QueryID:    "getUser",
		Parameters: map[string]string{"value": "alice"},
	}

	mock.incoming <- reqMsg

	select {
	case receivedInput := <-handlerCalled:
		if receivedInput == nil || receivedInput.Value != "alice" {
			t.Errorf("expected receivedInput value 'alice', got: %+v", receivedInput)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for query handler to be called")
	}

	select {
	case sent := <-mock.sentMsgs:
		rawResp, ok := sent.(*messages.QueryResponseRawMsg)
		if !ok {
			t.Fatalf("expected *messages.QueryResponseRawMsg, got: %T", sent)
		}

		if rawResp.RequestID != revent.RequestID(requestUUID) {
			t.Errorf("expected RequestID %s, got %s", requestUUID, rawResp.RequestID)
		}

		var output testQueryOutput

		if errUnmarshal := json.Unmarshal(rawResp.Response, &output); errUnmarshal != nil {
			t.Fatalf("failed to unmarshal sent response: %v", errUnmarshal)
		}

		if output.Result != "hello-alice" {
			t.Errorf("expected result 'hello-alice', got %q", output.Result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for query response to be sent")
	}

	cancel()
	<-startErrCh
}
