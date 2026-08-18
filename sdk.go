package revent_sdk_go

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/manuelarte/revent-sdk-go/internal/revent/actions"
	"github.com/manuelarte/revent-sdk-go/internal/txrx"
	"github.com/manuelarte/revent-sdk-go/revent"
)

// OpenSession starts a session and can be called only once per ServerManager instance.
func OpenSession(ctx context.Context, s *ServerManager, cfg txrx.GrpcConfig) error {
	var err error

	s.once.Do(func() {
		createTxRxFn := func() (txrx.TxRx, error) {
			return txrx.NewGRPCTxRx(ctx, s.logger, cfg, registerClient(s))
		}
		err = s.start(ctx, createTxRxFn)
	})

	if err != nil {
		return err
	}

	return nil
}

// RegisterQueryHandler registers a query handler for a specific query ID.
// It ensures that only one handler is registered for each query ID and returns an error
// if a handler already exists for the given query ID.
//
// I and O must be types that encoding/json can handle. That is verified here against
// their zero values, so a type JSON cannot encode (one holding a channel or a func,
// for instance) is reported at registration instead of on the first live query.
func RegisterQueryHandler[
	I revent.QueryRequestParameters,
	O revent.QueryResponse,
](
	s *ServerManager,
	query revent.Query[I, O],
	qh revent.QueryHandlerFunc[I, O],
) error {
	queryID := revent.QueryID(query)

	if err := checkJSONEncodable[I](); err != nil {
		return fmt.Errorf("query %s has invalid parameters type: %w", queryID, err)
	}

	if err := checkJSONEncodable[O](); err != nil {
		return fmt.Errorf("query %s has invalid response type: %w", queryID, err)
	}

	s.muQueryHandlers.Lock()
	defer s.muQueryHandlers.Unlock()

	if _, ok := s.queryHandlers[queryID]; ok {
		return revent.QueryHandlerAlreadyRegisteredError{
			QueryID: queryID,
		}
	}

	s.queryHandlers[queryID] = func(ctx context.Context, params map[string]string) ([]byte, error) {
		var input I

		jsonBytes, err := parametersToJSON(params)
		if err != nil {
			return nil, fmt.Errorf("failed to encode query parameters: %w", err)
		}

		if errUnmarshal := json.Unmarshal(jsonBytes, &input); errUnmarshal != nil {
			return nil, fmt.Errorf("failed to unmarshal query parameters: %w", errUnmarshal)
		}

		output := qh(ctx, input)

		outputBytes, errMarshal := json.Marshal(output)
		if errMarshal != nil {
			return nil, fmt.Errorf("failed to marshal query response: %w", errMarshal)
		}

		return outputBytes, nil
	}

	return nil
}

// checkJSONEncodable reports whether encoding/json can encode T, by marshalling its
// zero value. Types json rejects outright (channels, funcs, and structs containing
// them) fail regardless of the value, so the zero value is enough to catch them.
func checkJSONEncodable[T any]() error {
	var zero T

	if _, err := json.Marshal(zero); err != nil {
		return fmt.Errorf("type %T is not JSON-serializable: %w", zero, err)
	}

	return nil
}

func parametersToJSON(params map[string]string) ([]byte, error) {
	if len(params) == 0 {
		return []byte("{}"), nil
	}

	parsed := make(map[string]any, len(params))
	for k, v := range params {
		var val any
		if err := json.Unmarshal([]byte(v), &val); err == nil {
			parsed[k] = val
		} else {
			parsed[k] = v
		}
	}

	return json.Marshal(parsed)
}

func RegisterSourceEventHandler[E revent.Event, S revent.SourceEvent[E]](eh revent.SourceEventHandler[E, S]) error {
	// TODO: here we need the Client struct and add the event handler for that event id.
	return nil
}

func QueryRequest[I revent.QueryRequestParameters, O revent.QueryResponse](
	ctx context.Context,
	s *ServerManager,
	requestID revent.RequestID,
	query revent.Query[I, O],
	params I,
) (O, error) {
	x := struct {
		txrx.TxRx
		*ServerManager
	}{
		s.txRx,
		s,
	}
	qr := actions.NewQueryRequisition[I, O](x)

	output, err := qr.Do(ctx, actions.QueryRequisitionParams[I, O]{
		RequestID: requestID,
		QueryID:   query,
		Params:    params,
	})
	if err != nil {
		var zero O

		return zero, fmt.Errorf("error sending query request: %w", err)
	}

	if output.Err != nil {
		var zero O

		return zero, output.Err
	}

	return output.Msg.Response, nil
}

func registerClient(s *ServerManager) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		x := struct {
			txrx.TxRx
			*ServerManager
		}{
			s.txRx,
			s,
		}
		cr := actions.NewClientRegistration(x)

		waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()

		output, errReg := cr.Do(waitCtx, actions.ClientRegistrationParams{
			ClientID:      s.clientID,
			QueryHandlers: s.getQueryHandlerIDs(),
		})
		if errReg != nil {
			return fmt.Errorf("failed to register client: %w", errReg)
		}

		if output.Err != nil {
			return fmt.Errorf("failed to register client: %w", output.Err)
		}

		s.logger.Info("Client registered successfully", "clientId", output.Msg.ClientID)

		return nil
	}
}
