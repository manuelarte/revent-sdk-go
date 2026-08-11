package revent_sdk_go

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"

	reventv1 "github.com/manuelarte/revent-sdk-go/internal/api/gRPC/revent/v1"
	"github.com/manuelarte/revent-sdk-go/internal/flow"
	"github.com/manuelarte/revent-sdk-go/logger"
	"github.com/manuelarte/revent-sdk-go/revent"
)

type ConnectionState string

type stateSubscription struct {
	predicate func(msg *reventv1.ServerToClientMessage) bool
	ch        chan<- *reventv1.ServerToClientMessage
}

// State manages a persistent gRPC connection with automatic reconnection
//
//go:structinit
type State struct {
	logger logger.ILogger
	cfg    Config

	once sync.Once
	txRx TxRx

	muSubscribers   sync.RWMutex
	subscribers     map[uuid.UUID]stateSubscription
	muQueryHandlers sync.RWMutex
	queryHandlers   map[revent.QueryID]any
}

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

func (s *State) RegisterClient() error {
	return s.txRx.Send(&reventv1.ClientToServerMessage{
		Payload: &reventv1.ClientToServerMessage_RegisterClient{
			RegisterClient: &reventv1.RegisterClient{
				ClientId:      s.cfg.ClientID.String(),
				QueryHandlers: s.getQueryHandlerIDs(),
			},
		},
	})
}

func (s *State) QueryRequest(requestID revent.RequestID, queryID revent.QueryID) error {
	return s.txRx.Send(&reventv1.ClientToServerMessage{
		Payload: &reventv1.ClientToServerMessage_QueryRequest{
			QueryRequest: &reventv1.QueryRequest{
				RequestId: requestID.String(),
				QueryId:   string(queryID),
			},
		},
	})
}

func (s *State) Subscribe(
	id uuid.UUID,
	pred func(msg *reventv1.ServerToClientMessage) bool,
	ch chan<- *reventv1.ServerToClientMessage,
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

// start creates the txRx connection and blocks until the connection is closed.
func (s *State) start(ctx context.Context) error {
	onConnected := func() {
		errReg := flow.NewClientRegistration(s.logger, s.cfg.ClientID.String()).Do(ctx, s)
		if errReg != nil {
			s.logger.Error("Failed to register client", "error", errReg)
		}
	}
	txRx, txRxErrChan, err := newGRPCTxRx(ctx, s.logger, s.cfg.GRPCCfg, onConnected, s)
	if err != nil {
		return fmt.Errorf("failed to create gRPC TxRx: %w", err)
	}
	s.txRx = txRx

	for {
		select {
		case <-ctx.Done():
			return nil
		case errTxRx := <-txRxErrChan:
			return fmt.Errorf("gRPC TxRx error: %w", errTxRx)
		}
	}
}

func (s *State) NotifySubscribers(msg *reventv1.ServerToClientMessage) {
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

func (s *State) getQueryHandlerIDs() []string {
	s.muQueryHandlers.RLock()
	defer s.muQueryHandlers.RUnlock()

	queryHandlers := make([]string, 0, len(s.queryHandlers))
	for queryID := range s.queryHandlers {
		queryHandlers = append(queryHandlers, string(queryID))
	}

	return queryHandlers
}
