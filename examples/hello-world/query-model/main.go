package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	reventsdkgo "github.com/manuelarte/revent-sdk-go"
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
	time.Sleep(4 * time.Second) // wait for the session to be established
	requestID := revent.RequestID(uuid.New())
	if _, errQueryRequest := reventsdkgo.QueryRequest(ctx, s, requestID, getAllUsers, getAllUsersParams{}); errQueryRequest != nil {
		return fmt.Errorf("failed to send query request: %w", errQueryRequest)
	}

	return g.Wait()
}
