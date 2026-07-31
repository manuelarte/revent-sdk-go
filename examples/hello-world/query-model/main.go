package main

import (
	"context"
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
	clientConfig := reventsdkgo.Config{
		ClientID:       reventsdkgo.ClientID("hello-world"),
		ServerURL:      "http://localhost",
		ServerGRPCPort: 10000,
		ServerRestPort: 10001,
	}
	s := reventsdkgo.NewState(clientConfig)
	logger.InfoContext(ctx, "Starting Query app", slog.Any("clientID", clientConfig.ClientID))

	fmt.Printf("Hello World! %+#v", clientConfig)

	uqh := userQueryHandler{users: make(map[int]user)}

	// TODO: Think about this, to have the same signature, maybe a type ReventQuery that
	// has the QueryRequest and the QueryResponse, and the QueryID. so then the signature
	// is different.
	_ = reventsdkgo.RegisterQueryHandler(s, "examples.GetUserByID", uqh.GetUserByID)
	_ = reventsdkgo.RegisterSourceEventHandler(uqh.OnUserCreatedEvent)

	// add http server with endpoint to ask for users by id

	return nil
}
