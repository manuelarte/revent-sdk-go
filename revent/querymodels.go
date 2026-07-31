package revent

import (
	"context"
	"encoding/json"
	"fmt"
)

var _ error = new(QueryHandlerAlreadyRegisteredError)

type (
	// QueryID defines the unique identifier for a query.
	QueryID string

	QueryRequestParameter json.Unmarshaler
	QueryResponse         json.Marshaler

	QueryHandler[I QueryRequestParameter, O QueryResponse] func(context.Context, I) O

	QueryHandlerAlreadyRegisteredError struct {
		QueryID QueryID
	}
)

func (q QueryHandlerAlreadyRegisteredError) Error() string {
	return fmt.Sprintf("query handler already registered for query ID %s", q.QueryID)
}
