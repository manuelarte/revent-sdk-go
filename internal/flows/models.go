package flows

import (
	"context"
	"fmt"

	"github.com/manuelarte/revent-sdk-go/internal"
	"github.com/manuelarte/revent-sdk-go/revent"
)

var (
	_ error      = new(UnexpectedMsgError)
	_ reventFlow = new(ClientRegistration)
	_ reventFlow = new(QueryRequisition[revent.QueryRequestParameters, revent.QueryResponse])
)

type (
	Sender interface {
		Send(msg revent.ClientMsg) error
	}

	SendAndSubscribe interface {
		Sender
		internal.SubscriptionManager
	}

	UnexpectedMsgError struct {
		Flow string
		Msg  any
	}

	// reventFlow is a flow that sends and subscribes to messages.
	// TODO: think this.
	reventFlow interface {
		Do(ctx context.Context, m SendAndSubscribe) error
	}
)

func (u UnexpectedMsgError) Error() string {
	return fmt.Sprintf("%q: unexpected message %T", u.Flow, u.Msg)
}
