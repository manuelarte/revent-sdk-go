package events

import (
	"encoding/json"

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

func (u UserCreatedEvent) MarshalJSON() ([]byte, error) {
	return json.Marshal(u)
}

func (u UserCreatedEvent) UnmarshalJSON(bytes []byte) error {
	return json.Unmarshal(bytes, &u)
}

func (u UserCreatedEvent) ID() string {
	return "examples.UserCreatedEvent"
}
