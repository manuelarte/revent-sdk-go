package main

import (
	"encoding/json"

	"github.com/manuelarte/revent-sdk-go/revent"
)

var (
	getUserByID revent.Query[getUserByIdQueryParams, getUserByIdQueryResponse] = "examples.GetUserByID"
	getAllUsers revent.Query[getAllUsersParams, getAllUsersResponse]           = "org.github.manuelarte.users.GetAll"
)

type (
	getUserByIdQueryParams struct {
		ID int `json:"id"`
	}

	getUserByIdQueryResponse struct {
		User user `json:"user"`
		Ok   bool `json:"ok"`
	}

	getAllUsersParams struct{}

	getAllUsersResponse struct {
		Users []user `json:"users"`
	}
)

func (g getUserByIdQueryParams) UnmarshalJSON(bytes []byte) error {
	return json.Unmarshal(bytes, &g)
}

func (g getUserByIdQueryResponse) UnmarshalJSON(bytes []byte) error {
	return json.Unmarshal(bytes, &g)
}

func (g getAllUsersResponse) UnmarshalJSON(bytes []byte) error {
	return json.Unmarshal(bytes, &g)
}

func (g getAllUsersParams) UnmarshalJSON(bytes []byte) error {
	return json.Unmarshal(bytes, &g)
}

func (g getUserByIdQueryParams) MarshalJSON() ([]byte, error) {
	return json.Marshal(g)
}

func (g getUserByIdQueryResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(g)
}

func (g getAllUsersResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(g)
}

func (g getAllUsersParams) MarshalJSON() ([]byte, error) {
	return json.Marshal(g)
}
