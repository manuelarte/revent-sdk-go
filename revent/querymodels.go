package revent

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

var _ error = new(QueryHandlerAlreadyRegisteredError)

type (
	RequestID uuid.UUID

	// QueryID defines the unique identifier for a query.
	QueryID string

	MarshalerAndUnmarshaler interface {
		json.Unmarshaler
		json.Marshaler
	}

	QueryRequestParameters MarshalerAndUnmarshaler
	QueryResponse          MarshalerAndUnmarshaler

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
