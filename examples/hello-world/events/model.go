package events

import (
	"github.com/manuelarte/revent-sdk-go/revent"
)

var _ revent.Event = new(UserCreatedEvent)

type (
	UserCreatedEvent struct {
		Id      int    `json:"id"`
		Name    string `json:"name"`
		Surname string `json:"surname"`
	}
)

func (u UserCreatedEvent) ID() string {
	return "org.github.manuelarte.users.UserCreatedEvent"
}
