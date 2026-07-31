package main

import (
	"encoding/json"

	"github.com/manuelarte/revent-sdk-go/revent"
)

var (
	_ revent.QueryRequestParameters = new(getUserByIdQueryParams)
	_ revent.QueryResponse          = new(getUserByIdQueryResponse)
	_ revent.QueryRequestParameters = new(getAllUsersParams)
	_ revent.QueryResponse          = new(getAllUsersResponse)
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

func (g getUserByIdQueryResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(g)
}

func (g getAllUsersParams) UnmarshalJSON(bytes []byte) error {
	return json.Unmarshal(bytes, &g)
}

func (g getAllUsersResponse) MarshalJSON() ([]byte, error) {
	return json.Marshal(g)
}
