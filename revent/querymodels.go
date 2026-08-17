package revent

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

var _ error = new(QueryHandlerAlreadyRegisteredError)

type (
	RequestID uuid.UUID

	// QueryID defines the unique identifier for a query.
	QueryID string

	// QueryRequestParameters is the parameter type of Query. It must be a type that
	// encoding/json can marshal and unmarshal — normally a struct with `json` tags,.
	QueryRequestParameters any

	// QueryResponse is the response type of Query. The same requirements as
	// QueryRequestParameters apply.
	QueryResponse any

	Query[I QueryRequestParameters, O QueryResponse]            QueryID
	QueryHandlerFunc[I QueryRequestParameters, O QueryResponse] func(context.Context, I) O

	QueryHandlerAlreadyRegisteredError struct {
		QueryID QueryID
	}
)

func (r RequestID) String() string {
	return uuid.UUID(r).String()
}

func (q QueryID) String() string {
	return string(q)
}

func (q QueryHandlerAlreadyRegisteredError) Error() string {
	return fmt.Sprintf("query handler already registered for query ID %s", q.QueryID)
}
