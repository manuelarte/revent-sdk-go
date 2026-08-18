package messages

import (
	"fmt"

	"github.com/manuelarte/revent-sdk-go/revent"
)

const (
	QueryRequestedErrorReasonRequestIDDuplicated  QueryRequestedErrorReason = "RequestIdDuplicated"
	QueryRequestedErrorReasonQueryHandlerNotFound QueryRequestedErrorReason = "QueryHandlerNotFound"
	QueryRequestedErrorReasonQueryTimedOut        QueryRequestedErrorReason = "QueryTimedOut"
	QueryRequestedErrorReasonUnmarshalError       QueryRequestedErrorReason = "UnmarshalError"
)

var (
	_ ClientMsg = new(QueryRequestMsg)
	_ ClientMsg = new(QueryResponseRawMsg)
	_ ServerMsg = new(QueryRequestedErrorRawMsg)
	_ ServerMsg = new(QueryRequestedMsg)
)

var (
	_ error         = new(QueryRequestedErrorMsg)
	_ IdempotentMsg = new(QueryRequestMsg)
	_ IdempotentMsg = new(QueryRequestedMsg)
)

type (
	// QueryRequestMsg is the request to a Query.
	// It contains the request ID, the query ID, and the parameters.
	QueryRequestMsg struct {
		RequestID  revent.RequestID
		QueryID    revent.QueryID
		Parameters revent.QueryRequestParameters
	}

	// QueryRequestedMsg is the msg received by the server asking to handle a query.
	QueryRequestedMsg struct {
		RequestID  revent.RequestID
		QueryID    revent.QueryID
		Parameters map[string]string
	}

	// QueryResponseRawMsg is the raw response to a QueryRequest, lacking extra information.
	// It is also used to send the response to the client.
	QueryResponseRawMsg struct {
		RequestID revent.RequestID
		Response  []byte
	}

	// QueryResponseMsg is the response to a QueryRequest, built based on QueryResponseRawMsg.
	QueryResponseMsg[O revent.QueryResponse] struct {
		RequestID revent.RequestID
		Response  O
	}

	QueryRequestedErrorReason string

	// QueryRequestedErrorRawMsg is the raw error response to a QueryRequest.
	QueryRequestedErrorRawMsg struct {
		RequestID revent.RequestID
		Reason    QueryRequestedErrorReason
	}

	// QueryRequestedErrorMsg is the error response to a QueryRequest,
	// built based on QueryRequestedErrorRawMsg.
	//nolint:errname // keep consistency with Msg at the end
	QueryRequestedErrorMsg struct {
		RequestID revent.RequestID
		QueryID   revent.QueryID
		Reason    QueryRequestedErrorReason
		Details   string
	}
)

func (e QueryRequestedErrorMsg) Error() string {
	return fmt.Sprintf("query %q response error: %q", e.QueryID, e.Reason)
}

func (q QueryRequestMsg) GetRequestID() revent.RequestID {
	return q.RequestID
}

func (q QueryResponseRawMsg) GetRequestID() revent.RequestID {
	return q.RequestID
}

func (e QueryRequestedErrorRawMsg) GetRequestID() revent.RequestID {
	return e.RequestID
}

func (e QueryRequestedErrorMsg) GetRequestID() revent.RequestID {
	return e.RequestID
}

func (q QueryRequestedMsg) GetRequestID() revent.RequestID {
	return q.RequestID
}

func (q QueryRequestMsg) clientMessage()           {}
func (q QueryResponseRawMsg) clientMessage()       {}
func (e QueryRequestedErrorRawMsg) clientMessage() {}
func (q QueryResponseRawMsg) serverMessage()       {}
func (e QueryRequestedErrorRawMsg) serverMessage() {}
func (e QueryRequestedErrorMsg) serverMessage()    {}
func (q QueryRequestedMsg) serverMessage()         {}
