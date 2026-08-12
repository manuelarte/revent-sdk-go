package flow

import "github.com/manuelarte/revent-sdk-go/internal"

type SendAndSubscribe interface {
	internal.Sender
	internal.SubscriptionManager
}
