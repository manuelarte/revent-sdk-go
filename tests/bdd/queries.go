package bdd

import (
	"encoding/json"

	"github.com/manuelarte/revent-sdk-go/revent"
)

var (
	//nolint:gochecknoglobals,unused // check later
	testQuery revent.Query[*bddQueryInput, *bddQueryOutput] = "org.github.r-event.sdk.test.query"
	_         revent.QueryRequestParameters                 = new(bddQueryInput)
	_         revent.QueryResponse                          = new(bddQueryOutput)
)

type (
	bddQueryInput struct {
		Value string `json:"value"`
	}

	bddQueryOutput struct {
		Result string `json:"result"`
	}
)

func (t *bddQueryInput) UnmarshalJSON(b []byte) error {
	type alias bddQueryInput

	return json.Unmarshal(b, (*alias)(t))
}

func (t *bddQueryInput) MarshalJSON() ([]byte, error) {
	type alias bddQueryInput

	return json.Marshal((*alias)(t))
}

func (t *bddQueryOutput) MarshalJSON() ([]byte, error) {
	type alias bddQueryOutput

	return json.Marshal((*alias)(t))
}

func (t *bddQueryOutput) UnmarshalJSON(b []byte) error {
	type alias bddQueryOutput

	return json.Unmarshal(b, (*alias)(t))
}
