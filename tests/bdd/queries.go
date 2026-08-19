package bdd

import (
	"github.com/manuelarte/revent-sdk-go/revent"
)

//nolint:gochecknoglobals,unused // check later
var testQuery revent.Query[*bddQueryInput, *bddQueryOutput] = "org.github.r-event.sdk.test.one-param-query"

type (
	bddQueryInput struct {
		Value string `json:"value"`
	}

	bddQueryOutput struct {
		Result string `json:"result"`
	}
)
