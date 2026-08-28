package main

import (
	"github.com/manuelarte/revent-sdk-go/revent"
)

var (
	getUserByID revent.Query[getUserByIdQueryParams, getUserByIdQueryResponse] = "org.github.manuelarte.users.GetUserByID"
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
