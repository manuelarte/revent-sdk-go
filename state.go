package revent_sdk_go

import (
	"context"
	"fmt"
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
	logger ILogger
	cfg    Config
	// field to check that the client is connecting to R-Event.
	connecting atomic.Bool

	once   sync.Once
	mu     sync.RWMutex
	conn   *grpc.ClientConn
	stream grpc.BidiStreamingClient[reventv1.ClientToServerMessage, reventv1.ServerToClientMessage]
	state  ConnectionState

	// Connection lifecycle management
	stateChangedChan chan ConnectionState
	muQueryHandlers  sync.RWMutex
	queryHandlers    map[revent.QueryID]any
}

func NewState(cfg Config) (*State, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	return &State{
		logger:           slog.Default(),
		cfg:              cfg,
		stateChangedChan: make(chan ConnectionState, 1),
		queryHandlers:    make(map[revent.QueryID]any),
	}, nil
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
			}
		}
	})

	// Goroutine 2: Listens to state changes and manages stream listeners
	g.Go(func() error {
		var (
			listenerCtx    context.Context
			listenerCancel context.CancelFunc
		)

		for {
			select {
			case <-ctx.Done():
				s.logger.Info("Stream listener manager shutting down")
				if listenerCancel != nil {
					listenerCancel()
				}
				return ctx.Err()
			case state := <-s.stateChangedChan:
				// If disconnected or connecting, stop current listener
				if state != connectedState {
					if listenerCancel != nil {
						s.logger.Info("Stopping stream listener due to state change", "newState", state)
						listenerCancel()
						listenerCtx = nil
						listenerCancel = nil
					}
					continue
				}

				// If now connected and no listener running, start one
				if state == connectedState && listenerCancel == nil {
					s.logger.Info("Starting stream listener")
					listenerCtx, listenerCancel = context.WithCancel(ctx)
					currentListenerCtx := listenerCtx
					g.Go(func() error {
						s.listenToStream(currentListenerCtx)
						return nil
					})
				}
			}
		}
	})

	return g.Wait()
}

// listenToStream continuously reads from the gRPC stream and processes incoming messages
func (s *State) listenToStream(ctx context.Context) {
	stream := s.getStream()
	if stream == nil {
		s.logger.Warn("Stream is nil when starting listener")
		return
	}

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Stream listener context cancelled")
			return
		default:
		}

		// Receive message from stream
		msg, err := stream.Recv()
		if err != nil {
			s.logger.Error("Error receiving message from stream",
				"error", err,
			)
			// Mark as disconnected so reconnection is triggered
			s.setState(disconnectedState)
			return
		}

		// Process the received message
		s.handleStreamMessage(msg)
	}
}

// handleStreamMessage processes a message received from the stream
func (s *State) handleStreamMessage(msg *reventv1.ServerToClientMessage) {
	if msg == nil {
		return
	}

	// TODO: Implement message handling logic
	s.logger.Debug("Received message from stream", "message", msg)
}

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
		stream, err := cc.OpenSession(ctx)
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
			if err := gRPCClientConn.Close(); err != nil {
				s.logger.Warn("Failed to close gRPC connection after session error", "error", err)
			}
			s.setState(disconnectedState)
			return fmt.Errorf("failed to open session: %w", err)
		}

		// Connection successful
		s.setConn(gRPCClientConn)
		s.setStream(stream)
		s.setState(connectedState)

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

func (s *State) setStream(stream grpc.BidiStreamingClient[reventv1.ClientToServerMessage, reventv1.ServerToClientMessage]) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.stream = stream
}

func (s *State) getStream() grpc.BidiStreamingClient[reventv1.ClientToServerMessage, reventv1.ServerToClientMessage] {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.stream
}

func (s *State) setConn(conn *grpc.ClientConn) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.conn = conn
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

	// Non-blocking send to notify state change
	select {
	case s.stateChangedChan <- state:
	default:
	}
}

func (s *State) getState() ConnectionState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.state
}

// Stop gracefully shuts down the connection manager
func (s *State) Stop() error {
	conn := s.getConn()
	if conn != nil {
		return conn.Close()
	}
	return nil
}

// IsConnected returns whether the client is currently connected
func (s *State) IsConnected() bool {
	return s.getState() == connectedState
}
