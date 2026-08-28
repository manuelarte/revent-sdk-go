package bdd

import (
	"encoding/json"
	"errors"

	"github.com/manuelarte/revent-sdk-go/revent"
)

//nolint:gochecknoglobals,lll // check later
var (
	testQuery                      revent.Query[*bddQueryInput, *bddQueryOutput]        = "org.github.r-event.sdk.test.one-param-query"
	testInvalidResponseQueryServer revent.Query[*bddQueryInput, *bddQueryOutput]        = "org.github.r-event.sdk.test.invalid-response-query"
	testInvalidResponseQueryClient revent.Query[*bddQueryInput, *bddInvalidQueryOutput] = "org.github.r-event.sdk.test.invalid-response-query"
	testErrorQuery                 revent.Query[*bddQueryInput, *bddErrorQueryOutput]   = "org.github.r-event.sdk.test.error-query"
)

type (
	bddQueryInput struct {
		Value string `json:"value"`
	}

	bddQueryOutput struct {
		Result string `json:"result"`
	}

	bddInvalidQueryOutput struct {
		// This struct requires a number field, but will receive a string from bddQueryOutput
		RequiredNumber int `json:"result"` // Expects a number in the "result" field
	}

	// bddErrorQueryOutput implements json.Marshaler to fail marshaling.
	bddErrorQueryOutput struct {
		shouldFail bool
	}
)

// MarshalJSON makes bddErrorQueryOutput fail to marshal if shouldFail is true.
func (e bddErrorQueryOutput) MarshalJSON() ([]byte, error) {
	if e.shouldFail {
		return nil, errors.New("intentional marshal error for testing")
	}

	return json.Marshal(map[string]any{"result": "ok"})
}
