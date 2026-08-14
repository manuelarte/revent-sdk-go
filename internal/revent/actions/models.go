package actions

import (
	"fmt"

	"github.com/manuelarte/revent-sdk-go/internal"
	"github.com/manuelarte/revent-sdk-go/revent/messages"
)

var _ error = new(UnexpectedMsgError)

type (
	Sender interface {
		Send(msg messages.ClientMsg) error
	}

	SendAndSubscribe interface {
		Sender
		internal.SubscriptionManager
	}

	UnexpectedMsgError struct {
		Flow string
		Msg  any
	}
)

func (u UnexpectedMsgError) Error() string {
	return fmt.Sprintf("%q: unexpected message %T", u.Flow, u.Msg)
}
