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

func (t *bddQueryInput) UnmarshalJSON(_ []byte) error {
	return nil
}

func (t *bddQueryInput) MarshalJSON() ([]byte, error) {
	return json.Marshal(t)
}

func (t bddQueryOutput) MarshalJSON() ([]byte, error) {
	return json.Marshal(t)
}

func (t bddQueryOutput) UnmarshalJSON(_ []byte) error {
	return nil
}
