package main

import (
	"context"
	"fmt"

	"github.com/manuelarte/revent-sdk-go/examples/hello-world/events"
	"github.com/manuelarte/revent-sdk-go/revent"
)

type (
	userQueryHandler struct {
		users map[int]user
	}
)

// GetUserByID query handler for getting a user by ID
func (uqh *userQueryHandler) GetUserByID(_ context.Context, params getUserByIdQueryParams) getUserByIdQueryResponse {
	user, ok := uqh.users[params.ID]
	return getUserByIdQueryResponse{
		User: user,
		Ok:   ok,
	}
}

func (uqh *userQueryHandler) GetAllUsers(_ context.Context, _ getAllUsersParams) getAllUsersResponse {
	users := make([]user, 0, len(uqh.users))
	for _, u := range uqh.users {
		users = append(users, u)
	}
	return getAllUsersResponse{
		Users: users,
	}
}

// OnUserCreatedEvent event handler for events.UserCreatedEvent
func (uqh *userQueryHandler) OnUserCreatedEvent(_ context.Context, event revent.SourceEvent[events.UserCreatedEvent]) {
	uqh.users[event.Payload.Id] = user{
		Id:       event.Payload.Id,
		FullName: fmt.Sprintf("%s %s", event.Payload.Name, event.Payload.Surname),
	}
}
