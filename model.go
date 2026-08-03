package revent_sdk_go

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	reventv1 "github.com/manuelarte/revent-sdk-go/internal/api/gRPC/revent/v1"
	backoff2 "github.com/manuelarte/revent-sdk-go/internal/backoff"
	"github.com/manuelarte/revent-sdk-go/revent"
)

var (
	ErrClientIDRequired         = errors.New("ClientID is required")
	ErrStreamNotConnected       = errors.New("stream not connected")
	_                     error = new(CantConnectToServerError)
)

type (
	// ClientID defines the client id to register to R-event.
	ClientID string

	CantConnectToServerError struct {
		Addr        string
		NumAttempts int
	}

	ILogger interface {
		Info(msg string, args ...any)
		Error(msg string, args ...any)
		Warn(msg string, args ...any)
		Debug(msg string, args ...any)
	}

	//go:structinit
	State struct {
		// field to check that the this field is only registered once.
		openSessionCalled atomic.Bool
		logger            ILogger
		cfg               Config
		muQueryHandlers   sync.RWMutex
		queryHandlers     map[revent.QueryID]any

		sendCh   chan *reventv1.ClientToServerMessage
		muStream sync.RWMutex
		stream   grpc.BidiStreamingClient[reventv1.ClientToServerMessage, reventv1.ServerToClientMessage]
	}
)

func (c CantConnectToServerError) Error() string {
	return fmt.Sprintf("failed to connect to server at %s after %d attempts", c.Addr, c.NumAttempts)
}

func (c ClientID) Validate() error {
	if c == "" {
		return ErrClientIDRequired
	}

	return nil
}

func (c ClientID) String() string {
	return string(c)
}

func NewState(cfg Config) (*State, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &State{
		logger:        slog.Default(),
		cfg:           cfg,
		queryHandlers: make(map[revent.QueryID]any),
		// Buffer the first control message so startup does not block if sender exits early.
		sendCh: make(chan *reventv1.ClientToServerMessage, 1),
	}, nil
}

func (s *State) init(ctx context.Context) error {
	err := s.connect(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to R-event: %w", err)
	}

	if errInit := s.run(ctx); errInit != nil {
		return fmt.Errorf("session ended with error: %w", errInit)
	}

	return nil
}

func (s *State) connect(ctx context.Context) error {
	gRPCClientConn, errClient := grpc.NewClient(
		s.cfg.GetGRPCAddress(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           s.cfg.BackoffCfg,
			MinConnectTimeout: 1 * time.Second,
		}),
		// Inject tracing information for R-Event
		// grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if errClient != nil {
		return fmt.Errorf("failed to instantiate GRPC client: %w", errClient)
	}

	cc := reventv1.NewControlClient(gRPCClientConn)
	expBackoff := backoff2.Exponential{Config: s.cfg.BackoffCfg}

	maxAttempts := int(s.cfg.NumberOfRetries)
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		stream, err := cc.OpenSession(ctx)
		if err != nil {
			if st, ok := status.FromError(err); ok {
				if st.Code() == codes.Unavailable {
					s.logger.Warn("R-Event server is not available, retrying...")

					time.Sleep(expBackoff.Backoff(attempt))

					continue
				}
			}

			return fmt.Errorf("failed to open session: %w", err)
		}

		s.setStream(stream)

		return nil
	}

	return CantConnectToServerError{
		Addr:        s.cfg.ServerURL,
		NumAttempts: maxAttempts,
	}
}

// run initializes the state.
//
//nolint:gocognit //TODO: refactor
func (s *State) run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case m, ok := <-s.sendCh:
				if !ok {
					return nil
				}

				if m == nil {
					continue
				}

				stream := s.getStream()
				if stream == nil {
					return ErrStreamNotConnected
				}

				if err := stream.Send(m); err != nil {
					return fmt.Errorf("failed to send stream message: %w", err)
				}
			}
		}
	})

	g.Go(func() error {
		for {
			stream := s.getStream()
			if stream == nil {
				return ErrStreamNotConnected
			}

			msg, errRecv := s.stream.Recv()
			if errRecv != nil {
				// Recv is bound to the stream context and will unblock on cancellation.
				if errors.Is(errRecv, io.EOF) {
					if err := s.init(ctx); err != nil {
						return fmt.Errorf("failed to reconnect: %w", err)
					}

					continue
				}

				if isContextShutdownError(ctx, errRecv) {
					cancel()

					return nil
				}

				return fmt.Errorf("failed to receive stream message: %w", errRecv)
			}
			// switch payload := msg.Payload.(type) { ... }
			//nolint:forbidigo // TODO: just for debugging for now
			fmt.Printf("Received: %v\n", msg)
		}
	})

	registerMsg := &reventv1.ClientToServerMessage{
		Payload: &reventv1.ClientToServerMessage_RegisterClient{
			RegisterClient: &reventv1.RegisterClient{
				ClientId:      s.cfg.ClientID.String(),
				QueryHandlers: nil,
			},
		},
	}

	select {
	case <-ctx.Done():
		return nil
	case s.sendCh <- registerMsg:
	}

	return g.Wait()
}

func (s *State) setStream(stream grpc.BidiStreamingClient[reventv1.ClientToServerMessage, reventv1.ServerToClientMessage]) {
	s.muStream.Lock()
	defer s.muStream.Unlock()

	s.stream = stream
}

func (s *State) getStream() grpc.BidiStreamingClient[reventv1.ClientToServerMessage, reventv1.ServerToClientMessage] {
	s.muStream.RLock()
	defer s.muStream.RUnlock()

	return s.stream
}

func isContextShutdownError(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return true
	}

	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	errCode := status.Code(err)

	return errCode == codes.Canceled || errCode == codes.DeadlineExceeded
}
