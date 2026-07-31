package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	reventsdkgo "github.com/manuelarte/revent-sdk-go"
	"github.com/manuelarte/revent-sdk-go/examples/hello-world/events"
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

var (
	_ revent.QueryRequestParameter = new(getUserByIdQueryParams)
	_ revent.QueryResponse         = new(getUserByIdQueryResponse)
)

type (
	user struct {
		id       int
		fullName string
	}

	userQueryHandler struct {
		users map[int]user
	}

	getUserByIdQueryParams struct {
		ID int `json:"id"`
	}

	getUserByIdQueryResponse struct {
		User user `json:"user"`
		Ok   bool `json:"ok"`
	}
)

func (g getUserByIdQueryParams) UnmarshalJSON(bytes []byte) error {
	if err := json.Unmarshal(bytes, &g); err != nil {
		return err
	}

	return nil
}

func (g getUserByIdQueryResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(g)
}

// GetUserByID query handler for getting a user by ID
func (uqh *userQueryHandler) GetUserByID(_ context.Context, params getUserByIdQueryParams) getUserByIdQueryResponse {
	user, ok := uqh.users[params.ID]
	return getUserByIdQueryResponse{
		User: user,
		Ok:   ok,
	}
}

// OnUserCreatedEvent event handler for events.UserCreatedEvent
func (uqh *userQueryHandler) OnUserCreatedEvent(_ context.Context, event revent.SourceEvent[events.UserCreatedEvent]) {
	uqh.users[event.Payload.Id] = user{
		id:       event.Payload.Id,
		fullName: fmt.Sprintf("%s %s", event.Payload.Name, event.Payload.Surname),
	}
}
