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

// OnUserCreatedEvent event handler for events.UserCreatedEvent
func (uqh *userQueryHandler) OnUserCreatedEvent(_ context.Context, event revent.SourceEvent[events.UserCreatedEvent]) {
	uqh.users[event.Payload.Id] = user{
		id:       event.Payload.Id,
		fullName: fmt.Sprintf("%s %s", event.Payload.Name, event.Payload.Surname),
	}
}
