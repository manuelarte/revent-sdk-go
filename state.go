package revent_sdk_go

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	reventv1 "github.com/manuelarte/revent-sdk-go/internal/api/gRPC/revent/v1"
	backoff2 "github.com/manuelarte/revent-sdk-go/internal/backoff"
	"github.com/manuelarte/revent-sdk-go/internal/flow"
	"github.com/manuelarte/revent-sdk-go/logger"
	"github.com/manuelarte/revent-sdk-go/revent"
)

const (
	connectedState    = "Connected"
	connectingState   = "Connecting"
	disconnectedState = "Disconnected"
)

type ConnectionState string

// State manages a persistent gRPC connection with automatic reconnection
//
//go:structinit
type State struct {
	logger logger.ILogger
	cfg    Config
	// field to check that the client is connecting to R-Event.
	connecting atomic.Bool

	once   sync.Once
	mu     sync.RWMutex
	conn   *grpc.ClientConn
	stream TxRx
	state  ConnectionState

	// Connection lifecycle management
	streamUpdates   chan TxRx
	muQueryHandlers sync.RWMutex
	queryHandlers   map[revent.QueryID]any
}

func NewState(cfg Config) (*State, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &State{
		logger:        slog.Default(),
		cfg:           cfg,
		streamUpdates: make(chan TxRx, 1),
		queryHandlers: make(map[revent.QueryID]any),
	}, nil
}

func (s *State) RegisterClient(clientID string) error {
	stream := s.getStream()
	if stream == nil {
		return errors.New("stream is not connected")
	}

	return stream.RegisterClient(clientID, s.getQueryHandlerIDs())
}

func (s *State) Subscribe(
	id uuid.UUID,
	pred func(msg *reventv1.ServerToClientMessage) bool,
	ch chan<- *reventv1.ServerToClientMessage,
) error {
	stream := s.getStream()
	if stream == nil {
		return errors.New("stream is not connected")
	}

	stream.Subscribe(id, pred, ch)

	return nil
}

func (s *State) Unsubscribe(id uuid.UUID) error {
	stream := s.getStream()
	if stream == nil {
		return errors.New("stream is not connected")
	}

	stream.Unsubscribe(id)

	return nil
}

func (s *State) start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s.setState(disconnectedState)
	// run goroutines for connection management and stream listening
	g, ctx := errgroup.WithContext(ctx)

	// Goroutine 1: Manages the connection lifecycle
	g.Go(func() error {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				s.logger.Info("Connection manager shutting down")

				return ctx.Err()
			case <-ticker.C:
				if s.getState() != disconnectedState {
					continue
				}

				s.logger.Debug("Connection manager trying to connect")

				if err := s.connect(ctx); err != nil {
					s.logger.Error("Failed to connect", "error", err)

					return err
				}

				s.logger.Info("Connection manager connected")

				errReg := flow.NewClientRegistration(s.logger, s.cfg.ClientID.String()).Do(ctx, s)
				if errReg != nil {
					s.logger.Error("Failed to register client", "error", errReg)
				}
			}
		}
	})

	// Goroutine 2: Listens to whichever stream is currently active.
	g.Go(func() error {
		s.listenToStream(ctx)

		return nil
	})

	return g.Wait()
}

// listenToStream continuously reads from the gRPC stream and processes incoming messages.
func (s *State) listenToStream(ctx context.Context) {
	var stream TxRx

	for {
		if stream == nil {
			select {
			case <-ctx.Done():
				s.logger.Info("Stream listener context cancelled")

				return
			case stream = <-s.streamUpdates:
				s.logger.Info("Stream listener attached to stream")
			}
		}

		err := stream.Pump(ctx)
		if err != nil {
			if ctx.Err() != nil {
				s.logger.Info("Stream listener context cancelled")

				return
			}

			s.logger.Error("Error receiving message from stream",
				"error", err,
			)
			// Mark as disconnected so reconnection is triggered
			s.setStream(nil)
			s.setState(disconnectedState)

			stream = nil

			continue
		}
	}
}

//nolint:gocognit // refactor later
func (s *State) connect(ctx context.Context) error {
	if !s.connecting.CompareAndSwap(false, true) {
		// Another goroutine is already connecting
		return nil
	}
	defer s.connecting.CompareAndSwap(true, false)

	s.setState(connectingState)

	// Close old connection if it exists
	oldConn := s.getConn()
	if oldConn != nil {
		s.setStream(nil)

		if err := oldConn.Close(); err != nil {
			s.logger.Warn("Failed to close old gRPC connection", "error", err)
		}
	}

	// Create new gRPC client connection
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
		s.setState(disconnectedState)

		return fmt.Errorf("failed to instantiate GRPC client: %w", errClient)
	}

	cc := reventv1.NewControlClient(gRPCClientConn)
	expBackoff := backoff2.Exponential{Config: s.cfg.BackoffCfg}

	maxAttempts := int(s.cfg.NumberOfRetries)
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		gRPCStream, err := cc.OpenSession(ctx)
		if err != nil {
			if st, ok := status.FromError(err); ok {
				if st.Code() == codes.Unavailable {
					s.logger.Warn("R-Event server is not available",
						"attempt", attempt,
						"maxAttempts", maxAttempts,
					)

					time.Sleep(expBackoff.Backoff(attempt))

					continue
				}
			}

			// For non-retryable errors, close connection and fail
			if errClose := gRPCClientConn.Close(); errClose != nil {
				s.logger.Warn("Failed to close gRPC connection after session error", "error", errClose)
			}

			s.setState(disconnectedState)

			return fmt.Errorf("failed to open session: %w", err)
		}

		// Connection successful
		stream := &gRPCTxRx{stream: gRPCStream}

		s.setConn(gRPCClientConn)
		s.setStream(stream)
		s.setState(connectedState)

		// Keep only the latest stream notification in the 1-slot channel.
		select {
		case <-s.streamUpdates:
		default:
		}

		s.streamUpdates <- stream

		return nil
	}

	// All attempts exhausted
	if err := gRPCClientConn.Close(); err != nil {
		s.logger.Warn("Failed to close gRPC connection after max retries", "error", err)
	}

	s.setState(disconnectedState)

	return CantConnectToServerError{
		Addr:        s.cfg.ServerURL,
		NumAttempts: maxAttempts,
	}
}

func (s *State) setStream(
	stream TxRx,
) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.stream = stream
}

func (s *State) getStream() TxRx {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.stream
}

func (s *State) setConn(conn *grpc.ClientConn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.conn = conn
}

func (s *State) getQueryHandlerIDs() []string {
	s.muQueryHandlers.RLock()
	defer s.muQueryHandlers.RUnlock()

	queryHandlers := make([]string, 0, len(s.queryHandlers))
	for queryID := range s.queryHandlers {
		queryHandlers = append(queryHandlers, string(queryID))
	}

	return queryHandlers
}

func (s *State) getConn() *grpc.ClientConn {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.conn
}

func (s *State) setState(state ConnectionState) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.state == state {
		// No state change
		return
	}

	s.state = state
}

func (s *State) getState() ConnectionState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.state
}
