package flow

import (
	"fmt"

	"github.com/manuelarte/revent-sdk-go/internal"
)

var _ error = new(UnexpectedMsgError)

type (
	SendAndSubscribe interface {
		internal.Sender
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
