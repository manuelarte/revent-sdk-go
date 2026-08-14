package revent

import (
	"fmt"
)

var (
	_ ClientMsg = new(ClientRegistrationMsg)
	_ ServerMsg = new(ClientRegisteredMsg)
)

var (
	_ error = new(ClientRegistrationErrorMsg)
	_ error = new(QueryResponseErrorMsg)
)

type (
	Msg any

	ClientMsg interface {
		Msg
		clientMessage()
	}

	ClientRegistrationMsg struct {
		ClientID      ClientID
		QueryHandlers []QueryID
	}

	QueryRequestMsg struct {
		RequestID  RequestID
		QueryID    QueryID
		Parameters QueryRequestParameters
	}

	ServerMsg interface {
		Msg
		serverMessage()
	}

	ClientRegisteredMsg struct {
		ClientID ClientID
	}

	//nolint:errname // keep consistency with Msg at the end
	ClientRegistrationErrorMsg struct {
		ClientID ClientID
		Reason   string
	}

	QueryResponseRawMsg struct {
		RequestID RequestID
		Response  []byte
	}

	//nolint:errname // keep consistency with Msg at the end
	QueryResponseErrorMsg struct {
		RequestID RequestID
		QueryID   QueryID
		Reason    string
	}
)

func (e ClientRegistrationErrorMsg) Error() string {
	if e.Reason == "" {
		return fmt.Sprintf("client %q registration rejected", e.ClientID)
	}

	return fmt.Sprintf("client %q registration rejected: %s", e.ClientID, e.Reason)
}

func (e QueryResponseErrorMsg) Error() string {
	if e.Reason == "" {
		return fmt.Sprintf("query %q response error", e.QueryID)
	}

	return fmt.Sprintf("query %q response error: %s", e.QueryID, e.Reason)
}

func (c ClientRegistrationMsg) clientMessage()      {}
func (q QueryRequestMsg) clientMessage()            {}
func (c ClientRegisteredMsg) serverMessage()        {}
func (e ClientRegistrationErrorMsg) serverMessage() {}
func (e QueryResponseRawMsg) serverMessage()        {}
func (e QueryResponseErrorMsg) serverMessage()      {}
