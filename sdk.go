package revent_sdk_go

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	reventv1 "github.com/manuelarte/revent-sdk-go/internal/api/gRPC/revent/v1"
	"github.com/manuelarte/revent-sdk-go/revent"
)

// OpenSession TODO: pending.
func OpenSession(ctx context.Context, state *State) error {
	if err := state.cfg.Validate(); err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	gRPCClientConn, err := grpc.NewClient(
		state.cfg.GetGRPCAddress(),
		// grpc.WithTransportCredentials(insecure.NewCredentials()),
		// Inject tracing information for R-Event
		// grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		return fmt.Errorf("failed to instantiate GRPC client: %w", err)
	}

	cc := reventv1.NewControlClient(gRPCClientConn)

	_, err = cc.OpenSession(ctx)
	if err != nil {
		return fmt.Errorf("failed to open session: %w", err)
	}

	return nil
}

// RegisterQueryHandler registers a query handler for a specific query ID.
// It ensures that only one handler is registered for each query ID and returns an error
// if a handler already exists for the given query ID.
func RegisterQueryHandler[I revent.QueryRequestParameter, O revent.QueryResponse](
	s *State,
	queryID revent.QueryID,
	qh revent.QueryHandler[I, O],
) error {
	s.muQueryHandlers.Lock()
	defer s.muQueryHandlers.Unlock()

	if _, ok := s.queryHandlers[queryID]; ok {
		return revent.QueryHandlerAlreadyRegisteredError{
			QueryID: queryID,
		}
	}

	s.queryHandlers[queryID] = qh

	return nil
}

func RegisterSourceEventHandler[E revent.Event, S revent.SourceEvent[E]](eh revent.SourceEventHandler[E, S]) error {
	// TODO: here we need the Client struct and add the event handler for that event id.
	return nil
}
