package messages

import (
	"fmt"

	"github.com/google/uuid"

	"github.com/manuelarte/revent-sdk-go/revent"
)

const (
	QueryRequestedErrorReasonRequestIdDuplicated  QueryRequestedErrorReason = "RequestIdDuplicated"
	QueryRequestedErrorReasonQueryHandlerNotFound QueryRequestedErrorReason = "QueryHandlerNotFound"
	QueryRequestedErrorReasonQueryTimedOut        QueryRequestedErrorReason = "QueryTimedOut"
	QueryRequestedErrorReasonUnmarshalError       QueryRequestedErrorReason = "UnmarshalError"
)

var (
	_ ClientMsg = new(QueryRequestMsg)
	_ ServerMsg = new(QueryRequestResponseMsg[revent.QueryResponse])
	_ ServerMsg = new(QueryRequestedErrorRawMsg)
)

var (
	_ error         = new(QueryRequestedErrorMsg)
	_ IdempotentMsg = new(QueryRequestMsg)
	_ IdempotentMsg = new(QueryRequestResponseMsg[revent.QueryResponse])
)

type (
	QueryRequestMsg struct {
		RequestID  revent.RequestID
		QueryID    revent.QueryID
		Parameters revent.QueryRequestParameters
	}

	QueryRequestResponseMsg[O revent.QueryResponse] struct {
		Msg *QueryResponseMsg[O]
		Err *QueryRequestedErrorMsg
	}

	QueryResponseRawMsg struct {
		RequestID revent.RequestID
		Response  []byte
	}

	QueryResponseMsg[O revent.QueryResponse] struct {
		RequestID revent.RequestID
		Response  O
	}

	QueryRequestedErrorReason string

	QueryRequestedErrorRawMsg struct {
		RequestID revent.RequestID
		Reason    QueryRequestedErrorReason
	}

	//nolint:errname // keep consistency with Msg at the end
	QueryRequestedErrorMsg struct {
		RequestID revent.RequestID
		QueryID   revent.QueryID
		Reason    QueryRequestedErrorReason
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
	if e.Reason == "" {
		return fmt.Sprintf("query %q response error", e.QueryID)
	}

	return fmt.Sprintf("query %q response error: %s", e.QueryID, e.Reason)
}

func (q QueryRequestMsg) GetRequestID() revent.RequestID {
	return q.RequestID
}

func (q QueryResponseRawMsg) GetRequestID() revent.RequestID {
	return q.RequestID
}

func (q QueryRequestedErrorRawMsg) GetRequestID() revent.RequestID {
	return q.RequestID
}

func (q QueryRequestedErrorMsg) GetRequestID() revent.RequestID {
	return q.RequestID
}

func (q QueryRequestMsg) clientMessage()            {}
func (e QueryResponseRawMsg) serverMessage()        {}
func (e QueryRequestedErrorRawMsg) serverMessage()  {}
func (e QueryRequestedErrorMsg) serverMessage()     {}
func (q QueryRequestResponseMsg[O]) serverMessage() {}
