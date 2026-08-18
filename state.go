package revent_sdk_go

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"

	"github.com/manuelarte/revent-sdk-go/internal"
	"github.com/manuelarte/revent-sdk-go/internal/revent/actions"
	"github.com/manuelarte/revent-sdk-go/internal/revent/messages"
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

		queryHandling *actions.QueryHandling

		muSubscribers   sync.RWMutex
		subscribers     map[uuid.UUID]serverMessageSubscription
		muQueryHandlers sync.RWMutex
		queryHandlers   map[revent.QueryID]actions.QueryHandler

		stateChan chan txrx.ConnectionState
	}

	serverMessageSubscription struct {
		predicate func(msg messages.ServerMsg) bool
		ch        chan<- messages.ServerMsg
	}
)

func NewState(cfg Config) (*ServerManager, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &ServerManager{
		logger:        cfg.Logger,
		clientID:      cfg.ClientID,
		subscribers:   make(map[uuid.UUID]serverMessageSubscription),
		queryHandlers: make(map[revent.QueryID]actions.QueryHandler),
		stateChan:     make(chan txrx.ConnectionState, 1),
	}, nil
}

func (s *ServerManager) Subscribe(
	id uuid.UUID,
	pred func(msg messages.ServerMsg) bool,
	ch chan<- messages.ServerMsg,
) {
	s.muSubscribers.Lock()
	defer s.muSubscribers.Unlock()

	s.subscribers[id] = serverMessageSubscription{predicate: pred, ch: ch}
}

func (s *ServerManager) Unsubscribe(id uuid.UUID) error {
	s.muSubscribers.Lock()
	defer s.muSubscribers.Unlock()

	delete(s.subscribers, id)

	return nil
}

func (s *ServerManager) StateChangesChan() <-chan txrx.ConnectionState {
	return s.stateChan
}

// start creates the txRx connection and blocks until the connection is closed.
func (s *ServerManager) start(
	ctx context.Context,
	createTxRxFn func() (txrx.TxRx, error),
) error {
	txRx, err := createTxRxFn()
	if err != nil {
		return fmt.Errorf("failed to create gRPC TxRx: %w", err)
	}

	s.txRx = txRx
	s.queryHandling = actions.NewQueryHandling(s.logger, s.txRx, s.queryHandler)
	incoming := txRx.Incoming()
	txRxSessionChan := txRx.SessionEvent()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-incoming:
			if !ok {
				incoming = nil

				continue
			}

			s.handleIncomingMessage(ctx, msg)
			s.dispatchServerMessage(msg)
		case sessionEvent, ok := <-txRxSessionChan:
			if !ok {
				return nil
			}

			if sessionEvent.Err == nil {
				s.publishStateChange(sessionEvent.State)

				continue
			}

			return fmt.Errorf("gRPC TxRx error: %w", sessionEvent.Err)
		}
	}
}

func (s *ServerManager) publishStateChange(state txrx.ConnectionState) {
	select {
	case s.stateChan <- state:
	default:
		// Keep the channel non-blocking and prefer the latest state.
		select {
		case <-s.stateChan:
		default:
		}

		select {
		case s.stateChan <- state:
		default:
		}
	}
}

func (s *ServerManager) handleIncomingMessage(ctx context.Context, msg messages.ServerMsg) {
	if queryRequested, ok := msg.(*messages.QueryRequestedMsg); ok && s.queryHandling != nil {
		go s.queryHandling.Do(ctx, actions.QueryHandlingParams{Msg: queryRequested})
	}
}

func (s *ServerManager) queryHandler(queryID revent.QueryID) (actions.QueryHandler, bool) {
	s.muQueryHandlers.RLock()
	handler, ok := s.queryHandlers[queryID]
	s.muQueryHandlers.RUnlock()

	return handler, ok
}

func (s *ServerManager) dispatchServerMessage(msg messages.ServerMsg) {
	if msg == nil {
		return
	}

	s.muSubscribers.RLock()

	subs := make([]serverMessageSubscription, 0, len(s.subscribers))
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
