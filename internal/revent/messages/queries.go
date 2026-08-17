package messages

import (
	"fmt"

	"github.com/google/uuid"

	"github.com/manuelarte/revent-sdk-go/revent"
)

const (
	QueryRequestedErrorReasonRequestIDDuplicated  QueryRequestErrorReason = "RequestIdDuplicated"
	QueryRequestedErrorReasonQueryHandlerNotFound QueryRequestErrorReason = "QueryHandlerNotFound"
	QueryRequestedErrorReasonQueryTimedOut        QueryRequestErrorReason = "QueryTimedOut"
	QueryRequestedErrorReasonUnmarshalError       QueryRequestErrorReason = "UnmarshalError"
)

var (
	_ ClientMsg = new(QueryRequestMsg)
	_ ClientMsg = new(QueryResponseRawMsg)
	_ ServerMsg = new(QueryRequestResponseMsg[revent.QueryResponse])
	_ ServerMsg = new(QueryRequestErrorRawMsg)
	_ ServerMsg = new(QueryRequestedMsg)
)

var (
	_ error         = new(QueryRequestedErrorMsg)
	_ IdempotentMsg = new(QueryRequestMsg)
	_ IdempotentMsg = new(QueryRequestResponseMsg[revent.QueryResponse])
	_ IdempotentMsg = new(QueryRequestedMsg)
)

type (
	// QueryRequestResponseMsg is the response to a QueryRequest.
	// It can be either a QueryResponseMsg or a QueryRequestedErrorMsg.
	// Where the Msg is the response to the QueryRequest, and the message
	// containing the error why the query could not be processed.
	QueryRequestResponseMsg[O revent.QueryResponse] struct {
		Msg *QueryResponseMsg[O]
		Err *QueryRequestedErrorMsg
	}

	// QueryRequestMsg is the request to a Query.
	// It contains the request ID, the query ID, and the parameters.
	QueryRequestMsg struct {
		RequestID  revent.RequestID
		QueryID    revent.QueryID
		Parameters revent.QueryRequestParameters
	}

	// QueryRequestedMsg is the msg received by the server asking to solve a query.
	QueryRequestedMsg struct {
		RequestID  revent.RequestID
		QueryID    revent.QueryID
		Parameters map[string]string
	}

	// QueryResponseRawMsg is the raw response to a QueryRequest, lacking extra information.
	// Struct not to be used directly.
	QueryResponseRawMsg struct {
		RequestID revent.RequestID
		Response  []byte
	}

	// QueryResponseMsg is the response to a QueryRequest, built based on QueryResponseRawMsg.
	QueryResponseMsg[O revent.QueryResponse] struct {
		RequestID revent.RequestID
		Response  O
	}

	QueryRequestErrorReason string

	QueryRequestErrorRawMsg struct {
		RequestID revent.RequestID
		Reason    QueryRequestErrorReason
	}

	// QueryRequestedErrorMsg is the error response to a QueryRequest,
	// built based on QueryRequestErrorRawMsg.
	//nolint:errname // keep consistency with Msg at the end
	QueryRequestedErrorMsg struct {
		RequestID revent.RequestID
		QueryID   revent.QueryID
		Reason    QueryRequestErrorReason
		Details   string
	}
)

func (q QueryRequestResponseMsg[O]) GetRequestID() revent.RequestID {
	if q.Msg != nil {
		return q.Msg.RequestID
	}

	if q.Err != nil {
		return q.Err.RequestID
	}

	return revent.RequestID(uuid.Nil)
}

func (e QueryRequestedErrorMsg) Error() string {
	return fmt.Sprintf("query %q response error: %q", e.QueryID, e.Reason)
}

func (q QueryRequestMsg) GetRequestID() revent.RequestID {
	return q.RequestID
}

func (q QueryResponseRawMsg) GetRequestID() revent.RequestID {
	return q.RequestID
}

func (e QueryRequestErrorRawMsg) GetRequestID() revent.RequestID {
	return e.RequestID
}

func (e QueryRequestedErrorMsg) GetRequestID() revent.RequestID {
	return e.RequestID
}

func (q QueryRequestedMsg) GetRequestID() revent.RequestID {
	return q.RequestID
}

func (q QueryRequestMsg) clientMessage()            {}
func (q QueryResponseRawMsg) clientMessage()        {}
func (q QueryResponseRawMsg) serverMessage()        {}
func (e QueryRequestErrorRawMsg) serverMessage()    {}
func (e QueryRequestedErrorMsg) serverMessage()     {}
func (q QueryRequestResponseMsg[O]) serverMessage() {}
func (q QueryRequestedMsg) serverMessage()          {}
