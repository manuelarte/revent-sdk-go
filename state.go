package revent_sdk_go

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"

	"github.com/manuelarte/revent-sdk-go/internal/flow"
	"github.com/manuelarte/revent-sdk-go/logger"
	"github.com/manuelarte/revent-sdk-go/revent"
)

// State manages a persistent gRPC connection with automatic reconnection
//
//go:structinit
type (
	State struct {
		logger logger.ILogger
		cfg    Config

		once sync.Once
		txRx TxRx

		muSubscribers   sync.RWMutex
		subscribers     map[uuid.UUID]stateSubscription
		muQueryHandlers sync.RWMutex
		queryHandlers   map[revent.QueryID]any
	}

	stateSubscription struct {
		predicate func(msg revent.ServerMessage) bool
		ch        chan<- revent.ServerMessage
	}
)

func NewState(cfg Config) (*State, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &State{
		logger:        cfg.Logger,
		cfg:           cfg,
		subscribers:   make(map[uuid.UUID]stateSubscription),
		queryHandlers: make(map[revent.QueryID]any),
	}, nil
}

func (s *State) RegisterClient(ctx context.Context) error {
	type sendAndSubscribe struct {
		*State
		TxRx
	}

	x := sendAndSubscribe{s, s.txRx}

	errReg := flow.NewClientRegistration(
		s.logger,
		s.cfg.ClientID,
		s.getQueryHandlerIDs(),
	).Do(ctx, x)
	if errReg != nil {
		return fmt.Errorf("failed to register client: %w", errReg)
	}

	return nil
}

func (s *State) QueryRequest(requestID revent.RequestID, queryID revent.QueryID) error {
	panic("not implemented")
}

func (s *State) Subscribe(
	id uuid.UUID,
	pred func(msg revent.ServerMessage) bool,
	ch chan<- revent.ServerMessage,
) error {
	s.muSubscribers.Lock()
	defer s.muSubscribers.Unlock()

	s.subscribers[id] = stateSubscription{predicate: pred, ch: ch}

	return nil
}

func (s *State) Unsubscribe(id uuid.UUID) error {
	s.muSubscribers.Lock()
	defer s.muSubscribers.Unlock()

	delete(s.subscribers, id)

	return nil
}

func (s *State) Handle(msg revent.ServerMessage) {
	if msg == nil {
		return
	}

	s.muSubscribers.RLock()

	subs := make([]stateSubscription, 0, len(s.subscribers))
	for _, sub := range s.subscribers {
		subs = append(subs, sub)
	}

	s.muSubscribers.RUnlock()

	for _, sub := range subs {
		if sub.predicate == nil || !sub.predicate(msg) {
			continue
		}

		// Never block the receive loop on slow subscribers.
		select {
		case sub.ch <- msg:
		default:
		}
	}
}

// start creates the txRx connection and blocks until the connection is closed.
func (s *State) start(ctx context.Context, createTxRxFn func() (TxRx, <-chan error, error)) error {
	txRx, txRxErrChan, err := createTxRxFn()
	if err != nil {
		return fmt.Errorf("failed to create gRPC TxRx: %w", err)
	}

	s.txRx = txRx
	incoming := txRx.Incoming()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-incoming:
			if !ok {
				incoming = nil
				continue
			}

			s.Handle(msg)
		case errTxRx, ok := <-txRxErrChan:
			if !ok {
				return nil
			}

			return fmt.Errorf("gRPC TxRx error: %w", errTxRx)
		}
	}
}

func (s *State) getQueryHandlerIDs() []revent.QueryID {
	s.muQueryHandlers.RLock()
	defer s.muQueryHandlers.RUnlock()

	queryHandlers := make([]revent.QueryID, 0, len(s.queryHandlers))
	for queryID := range s.queryHandlers {
		queryHandlers = append(queryHandlers, queryID)
	}

	return queryHandlers
}
