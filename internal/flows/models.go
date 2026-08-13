package flows

import (
	"fmt"

	"github.com/manuelarte/revent-sdk-go/internal"
	"github.com/manuelarte/revent-sdk-go/revent"
)

var _ error = new(UnexpectedMsgError)

type (
	Sender interface {
		Send(msg revent.ClientMessage) error
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
