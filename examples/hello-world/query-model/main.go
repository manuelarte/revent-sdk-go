package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	reventsdkgo "github.com/manuelarte/revent-sdk-go"
	"github.com/manuelarte/revent-sdk-go/internal/txrx"
	"github.com/manuelarte/revent-sdk-go/revent"
)

func main() {
	logger := slog.Default()
	if err := run(logger); err != nil {
		logger.Error("Error running the application", slog.Any("error", err))
	}
}

func run(logger *slog.Logger) error {
	ctx := context.Background()
	cfg := reventsdkgo.DefaultConfig()
	cfg.ClientID = cfg.ClientID + "-query"
	cfg.Logger = logger
	s, err := reventsdkgo.NewState(cfg)
	if err != nil {
		return fmt.Errorf("failed to create state: %w", err)
	}
	clientID := cfg.ClientID
	logger.InfoContext(ctx, "Starting Query app", slog.Any("clientID", clientID))

	uqh := userQueryHandler{users: make(map[int]user)}

	errRegisteringHandlers := errors.Join(
		// TODO: do flow of handle query so then the query part is finished.
		reventsdkgo.RegisterQueryHandler(s, getUserByID, uqh.GetUserByID),
		reventsdkgo.RegisterSourceEventHandler(uqh.OnUserCreatedEvent),
	)
	if errRegisteringHandlers != nil {
		return fmt.Errorf("failed to register R-Event query and event handlers: %w", errRegisteringHandlers)
	}

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		if errSession := reventsdkgo.OpenSession(ctx, s, cfg.GRPCCfg); errSession != nil {
			logger.ErrorContext(ctx, "Failed to open session", slog.Any("clientID", clientID), slog.Any("error", errSession))
			return errSession
		}
		return nil
	})

	// add http server with endpoint to ask for users by id

	// Wait until the session reports a connected state before sending queries.
clientRegisteredLoop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case stateEvent := <-s.StateChangesChan():
			if stateEvent == txrx.ClientRegisteredState {
				logger.InfoContext(ctx, "Session connected", slog.Any("clientID", clientID))
				break clientRegisteredLoop
			}
		}
	}

	requestID := revent.RequestID(uuid.New())
	if _, errQueryRequest := reventsdkgo.QueryRequest(ctx, s, requestID, getAllUsers, getAllUsersParams{}); errQueryRequest != nil {
		logger.ErrorContext(ctx, "failed to process query request", slog.Any("err", errQueryRequest))
	}

	return g.Wait()
}
