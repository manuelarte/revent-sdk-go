package revent_sdk_go

import (
	"context"
	"errors"

	"github.com/manuelarte/revent-sdk-go/revent"
)

// ErrOpenSessionAlreadyCalled is returned when OpenSession is called more than once for the same State.
var ErrOpenSessionAlreadyCalled = errors.New("OpenSession can only be called once per state")

// OpenSession starts a session and can be called only once per State instance.
func OpenSession(ctx context.Context, s *State) error {
	var err error

	s.once.Do(func() {
		err = s.start(ctx)
	})

	if err != nil {
		return err
	}

	return nil
}

// RegisterQueryHandler registers a query handler for a specific query ID.
// It ensures that only one handler is registered for each query ID and returns an error
// if a handler already exists for the given query ID.
func RegisterQueryHandler[
	I revent.QueryRequestParameters,
	O revent.QueryResponse,
](
	s *State,
	query revent.Query[I, O],
	qh revent.QueryHandlerFunc[I, O],
) error {
	queryID := revent.QueryID(query)

	s.muQueryHandlers.Lock()
	defer s.muQueryHandlers.Unlock()

	if _, ok := s.queryHandlers[queryID]; ok {
		return revent.QueryHandlerAlreadyRegisteredError{
			QueryID: queryID,
		}
	}

	s.queryHandlers[queryID] = qh

	return nil
}

func RegisterSourceEventHandler[E revent.Event, S revent.SourceEvent[E]](eh revent.SourceEventHandler[E, S]) error {
	// TODO: here we need the Client struct and add the event handler for that event id.
	return nil
}
