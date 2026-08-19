package messages

import (
	"github.com/manuelarte/revent-sdk-go/revent"
)

const (
	QueryRequestedErrorReasonRequestIDDuplicated  QueryRequestedErrorReason = "RequestIdDuplicated"
	QueryRequestedErrorReasonQueryHandlerNotFound QueryRequestedErrorReason = "QueryHandlerNotFound"
	QueryRequestedErrorReasonQueryTimedOut        QueryRequestedErrorReason = "QueryTimedOut"
	QueryRequestedErrorReasonErrorHandling        QueryRequestedErrorReason = "ErrorHandling"
)

var (
	_ ClientMsg = new(QueryRequestMsg)
	_ ClientMsg = new(QueryResponseRawMsg)
	_ ServerMsg = new(QueryRequestedErrorRawMsg)
	_ ServerMsg = new(QueryRequestedMsg)
)

var (
	_ IdempotentMsg = new(QueryRequestMsg)
	_ IdempotentMsg = new(QueryRequestedMsg)
	_ IdempotentMsg = new(QueryResponseRawMsg)
	_ IdempotentMsg = new(QueryHandlingErrorMsg)
	_ IdempotentMsg = new(QueryRequestedErrorRawMsg)
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

	QueryHandlingErrorMsg struct {
		RequestID revent.RequestID
		Reason    string
		Details   string
	}

	QueryRequestedErrorReason string

	// QueryRequestedErrorRawMsg is the raw error response to a QueryRequest.
	QueryRequestedErrorRawMsg struct {
		RequestID revent.RequestID
		Reason    QueryRequestedErrorReason
	}
)

func (q QueryRequestMsg) GetRequestID() revent.RequestID {
	return q.RequestID
}

func (q QueryResponseRawMsg) GetRequestID() revent.RequestID {
	return q.RequestID
}

func (e QueryRequestedErrorRawMsg) GetRequestID() revent.RequestID {
	return e.RequestID
}

func (e QueryHandlingErrorMsg) GetRequestID() revent.RequestID {
	return e.RequestID
}

func (q QueryRequestedMsg) GetRequestID() revent.RequestID {
	return q.RequestID
}

func (q QueryRequestMsg) clientMessage()           {}
func (q QueryResponseRawMsg) clientMessage()       {}
func (e QueryRequestedErrorRawMsg) clientMessage() {}
func (e QueryHandlingErrorMsg) clientMessage()     {}
func (q QueryResponseRawMsg) serverMessage()       {}
func (e QueryRequestedErrorRawMsg) serverMessage() {}
func (q QueryRequestedMsg) serverMessage()         {}
