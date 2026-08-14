package revent_sdk_go

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"

	"github.com/manuelarte/revent-sdk-go/internal"
	"github.com/manuelarte/revent-sdk-go/internal/txrx"
	"github.com/manuelarte/revent-sdk-go/logger"
	"github.com/manuelarte/revent-sdk-go/revent"
)

var _ internal.SubscriptionManager = new(ServerManager)

type (
	// ServerManager manages a persistent gRPC connection with automatic reconnection
	//
	//go:structinit
	ServerManager struct {
		logger   logger.ILogger
		clientID revent.ClientID

		once sync.Once
		txRx txrx.TxRx

		muSubscribers   sync.RWMutex
		subscribers     map[uuid.UUID]stateSubscription
		muQueryHandlers sync.RWMutex
		queryHandlers   map[revent.QueryID]any
	}

	stateSubscription struct {
		predicate func(msg revent.ServerMsg) bool
		ch        chan<- revent.ServerMsg
	}
)

func NewState(cfg Config) (*ServerManager, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &ServerManager{
		logger:        cfg.Logger,
		clientID:      cfg.ClientID,
		subscribers:   make(map[uuid.UUID]stateSubscription),
		queryHandlers: make(map[revent.QueryID]any),
	}, nil
}

func (s *ServerManager) Subscribe(
	id uuid.UUID,
	pred func(msg revent.ServerMsg) bool,
	ch chan<- revent.ServerMsg,
) error {
	s.muSubscribers.Lock()
	defer s.muSubscribers.Unlock()

	s.subscribers[id] = stateSubscription{predicate: pred, ch: ch}

	return nil
}

func (s *ServerManager) Unsubscribe(id uuid.UUID) error {
	s.muSubscribers.Lock()
	defer s.muSubscribers.Unlock()

	delete(s.subscribers, id)

	return nil
}

// start creates the txRx connection and blocks until the connection is closed.
func (s *ServerManager) start(ctx context.Context, createTxRxFn func() (txrx.TxRx, <-chan txrx.SessionEvent, error)) error {
	txRx, txRxSessionChan, err := createTxRxFn()
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

			s.dispatchServerMessage(msg)
		case sessionEvent, ok := <-txRxSessionChan:
			if !ok {
				return nil
			}

			if sessionEvent.Err == nil {
				//TODO: update state
				continue
			}

			return fmt.Errorf("gRPC TxRx error: %w", sessionEvent.Err)
		}
	}
}

func (s *ServerManager) dispatchServerMessage(msg revent.ServerMsg) {
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

func (s *ServerManager) getQueryHandlerIDs() []revent.QueryID {
	s.muQueryHandlers.RLock()
	defer s.muQueryHandlers.RUnlock()

	queryHandlers := make([]revent.QueryID, 0, len(s.queryHandlers))
	for queryID := range s.queryHandlers {
		queryHandlers = append(queryHandlers, queryID)
	}

	return queryHandlers
}
