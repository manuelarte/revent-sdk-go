package main

import (
	"encoding/json"

	"github.com/manuelarte/revent-sdk-go/revent"
)

var (
	_ revent.QueryRequestParameters = new(getUserByIdQueryParams)
	_ revent.QueryResponse          = new(getUserByIdQueryResponse)
)

type (
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
