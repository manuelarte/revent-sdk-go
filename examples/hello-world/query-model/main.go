package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	reventsdkgo "github.com/manuelarte/revent-sdk-go"
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
	s, err := reventsdkgo.NewState(cfg)
	if err != nil {
		return fmt.Errorf("failed to create state: %w", err)
	}
	clientID := cfg.ClientID
	logger.InfoContext(ctx, "Starting Query app", slog.Any("clientID", clientID))

	uqh := userQueryHandler{users: make(map[int]user)}

	errRegisteringHandlers := errors.Join(
		reventsdkgo.RegisterQueryHandler(s, getUserByID, uqh.GetUserByID),
		reventsdkgo.RegisterQueryHandler(s, getAllUsers, uqh.GetAllUsers),
		reventsdkgo.RegisterSourceEventHandler(uqh.OnUserCreatedEvent),
	)
	if errRegisteringHandlers != nil {
		return fmt.Errorf("failed to register R-Event query and event handlers: %w", errRegisteringHandlers)
	}

	if errSession := reventsdkgo.OpenSession(ctx, s); errSession != nil {
		logger.ErrorContext(ctx, "Failed to open session", slog.Any("clientID", clientID), slog.Any("error", errSession))
	}

	// add http server with endpoint to ask for users by id

	return nil
}
