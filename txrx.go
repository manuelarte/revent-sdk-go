package revent_sdk_go

import (
	"context"
	"sync"

	"github.com/google/uuid"
	"google.golang.org/grpc"

	reventv1 "github.com/manuelarte/revent-sdk-go/internal/api/gRPC/revent/v1"
)

var _ TxRx = new(gRPCTxRx)

type (
	TxRx interface {
		Subscribe(
			id uuid.UUID,
			pred func(msg *reventv1.ServerToClientMessage,
			) bool, ch chan<- *reventv1.ServerToClientMessage)
		Unsubscribe(id uuid.UUID)
		Pump(ctx context.Context) error
		RegisterClient(clientID string, queryHandlers []string) error
	}

	streamSubscription struct {
		predicate func(msg *reventv1.ServerToClientMessage) bool
		ch        chan<- *reventv1.ServerToClientMessage
	}

	gRPCTxRx struct {
		stream grpc.BidiStreamingClient[reventv1.ClientToServerMessage, reventv1.ServerToClientMessage]
		mu     sync.RWMutex
		subs   map[uuid.UUID]streamSubscription
	}
)

func (g *gRPCTxRx) Subscribe(
	id uuid.UUID,
	pred func(msg *reventv1.ServerToClientMessage) bool,
	ch chan<- *reventv1.ServerToClientMessage,
) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.subs == nil {
		g.subs = make(map[uuid.UUID]streamSubscription)
	}

	g.subs[id] = streamSubscription{predicate: pred, ch: ch}
}

func (g *gRPCTxRx) Unsubscribe(id uuid.UUID) {
	g.mu.Lock()
	defer g.mu.Unlock()

	delete(g.subs, id)
}

func (g *gRPCTxRx) Pump(_ context.Context) error {
	msg, err := g.stream.Recv()
	if err != nil {
		return err
	}

	g.mu.RLock()
	defer g.mu.RUnlock()

	for _, sub := range g.subs {
		if sub.predicate == nil || !sub.predicate(msg) {
			continue
		}

		// Never block the receive loop on slow subscribers.
		select {
		case sub.ch <- msg:
		default:
		}
	}

	return nil
}

func (g *gRPCTxRx) RegisterClient(clientID string, queryHandlers []string) error {
	return g.stream.Send(&reventv1.ClientToServerMessage{
		Payload: &reventv1.ClientToServerMessage_RegisterClient{
			RegisterClient: &reventv1.RegisterClient{
				ClientId:      clientID,
				QueryHandlers: queryHandlers,
			},
		},
	})
}
