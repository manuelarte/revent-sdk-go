package revent_sdk_go

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	reventv1 "github.com/manuelarte/revent-sdk-go/internal/api/gRPC/revent/v1"
	"github.com/manuelarte/revent-sdk-go/revent"
)

var ErrClientIDRequired = errors.New("ClientID is required")

type (
	// ClientID defines the client id to register to R-event.
	ClientID string

	State struct {
		cfg             Config
		muQueryHandlers sync.RWMutex
		queryHandlers   map[revent.QueryID]any
	}
)

func (c ClientID) Validate() error {
	if c == "" {
		return ErrClientIDRequired
	}

	return nil
}

func (c ClientID) String() string {
	return string(c)
}

func NewState(cfg Config) *State {
	return &State{
		cfg:           cfg,
		queryHandlers: make(map[revent.QueryID]any),
	}
}

// init initializes the state with the given stream.
//
//nolint:gocognit //TODO: refactor
func (s *State) init(
	ctx context.Context,
	stream grpc.BidiStreamingClient[
		reventv1.ClientToServerMessage,
		reventv1.ServerToClientMessage,
	],
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)
	// Buffer the first control message so startup does not block if sender exits early.
	sendCh := make(chan *reventv1.ClientToServerMessage, 1)

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case m, ok := <-sendCh:
				if !ok {
					return nil
				}
				if m == nil {
					continue
				}

				if err := stream.Send(m); err != nil {
					return fmt.Errorf("failed to send stream message: %w", err)
				}
			}
		}
	})

	g.Go(func() error {
		for {
			msg, errRecv := stream.Recv()
			if errRecv != nil {
				// Recv is bound to the stream context and will unblock on cancellation.
				if errors.Is(errRecv, io.EOF) || isContextShutdownError(ctx, errRecv) {
					cancel()

					return nil
				}

				return fmt.Errorf("failed to receive stream message: %w", errRecv)
			}
			// switch payload := msg.Payload.(type) { ... }
			fmt.Printf("Received: %v\n", msg)
		}
	})

	registerMsg := &reventv1.ClientToServerMessage{
		Payload: &reventv1.ClientToServerMessage_RegisterClient{
			RegisterClient: &reventv1.RegisterClient{
				ClientId:      s.cfg.ClientID.String(),
				QueryHandlers: nil,
			},
		},
	}

	select {
	case <-ctx.Done():
		return nil
	case sendCh <- registerMsg:
	}

	return g.Wait()
}

func isContextShutdownError(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return true
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	errCode := status.Code(err)
	return errCode == codes.Canceled || errCode == codes.DeadlineExceeded
}
