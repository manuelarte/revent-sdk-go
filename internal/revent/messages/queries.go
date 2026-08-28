package messages

import (
	"github.com/manuelarte/revent-sdk-go/revent"
)

const (
	QueryRequestedFailedReasonRequestIDDuplicated  QueryRequestedFailedReason = "RequestIdDuplicated"
	QueryRequestedFailedReasonQueryHandlerNotFound QueryRequestedFailedReason = "QueryHandlerNotFound"
	QueryRequestedFailedReasonQueryTimedOut        QueryRequestedFailedReason = "QueryTimedOut"
	QueryRequestedFailedReasonErrorHandling        QueryRequestedFailedReason = "ErrorHandling"
)

var (
	_ ClientMsg = new(QueryRequestMsg)
	_ ClientMsg = new(QueryResponseRawMsg)
	_ ServerMsg = new(QueryRequestedFailedRawMsg)
	_ ServerMsg = new(QueryRequestedMsg)
)

var (
	_ IdempotentMsg = new(QueryRequestMsg)
	_ IdempotentMsg = new(QueryRequestedMsg)
	_ IdempotentMsg = new(QueryResponseRawMsg)
	_ IdempotentMsg = new(QueryHandlingFailedMsg)
	_ IdempotentMsg = new(QueryRequestedFailedRawMsg)
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

	QueryHandlingFailedMsg struct {
		RequestID revent.RequestID
		Reason    string
		Details   string
	}

	QueryRequestedFailedReason string

	// QueryRequestedFailedRawMsg is the raw error response to a QueryRequest.
	QueryRequestedFailedRawMsg struct {
		RequestID revent.RequestID
		Reason    QueryRequestedFailedReason
	}
)

func (q QueryRequestMsg) GetRequestID() revent.RequestID {
	return q.RequestID
}

func (q QueryResponseRawMsg) GetRequestID() revent.RequestID {
	return q.RequestID
}

func (e QueryRequestedFailedRawMsg) GetRequestID() revent.RequestID {
	return e.RequestID
}

func (e QueryHandlingFailedMsg) GetRequestID() revent.RequestID {
	return e.RequestID
}

func (q QueryRequestedMsg) GetRequestID() revent.RequestID {
	return q.RequestID
}

func (q QueryRequestMsg) clientMessage()            {}
func (q QueryResponseRawMsg) clientMessage()        {}
func (e QueryRequestedFailedRawMsg) clientMessage() {}
func (e QueryHandlingFailedMsg) clientMessage()     {}
func (q QueryResponseRawMsg) serverMessage()        {}
func (e QueryRequestedFailedRawMsg) serverMessage() {}
func (q QueryRequestedMsg) serverMessage()          {}
