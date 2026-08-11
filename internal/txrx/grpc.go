package txrx

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/manuelarte/revent-sdk-go/internal"
	reventv1 "github.com/manuelarte/revent-sdk-go/internal/api/gRPC/revent/v1"
	backoff2 "github.com/manuelarte/revent-sdk-go/internal/backoff"
	"github.com/manuelarte/revent-sdk-go/logger"
)

const (
	defaultMaxNumberOfRetries                 = 2
	connectedState            connectionState = "Connected"
	connectingState           connectionState = "Connecting"
	disconnectedState         connectionState = "Disconnected"
)

var (
	ErrGRPCAddress        = errors.New("GRPCAddress is required")
	ErrStreamClosed       = errors.New("stream closed")
	_               error = new(CantConnectToServerError)
)

type (
	// CantConnectToServerError is returned when the client fails to connect to the server after multiple attempts.
	CantConnectToServerError struct {
		Addr        string
		NumAttempts int
	}

	//go:structinit
	GrpcConfig struct {
		// GRPCAddress R-Event gRPC server.
		GRPCAddress string
		// BackoffCfg backoff configuration
		BackoffCfg backoff.Config
		// NumberOfRetries number of retries to connect to R-Event server.
		NumberOfRetries uint
	}

	// GRPC implements the TxRx interface using gRPC.
	//go:structinit
	GRPC struct {
		cfg         GrpcConfig
		m           internal.Manager
		logger      logger.ILogger
		state       connectionState
		onConnected func()

		// field to check that the client is connecting to R-Event.
		connecting atomic.Bool
		conn       *grpc.ClientConn
		mu         sync.RWMutex
		stream     grpc.BidiStreamingClient[reventv1.ClientToServerMessage, reventv1.ServerToClientMessage]
	}

	connectionState string
)

func DefaultGrpcConfig() GrpcConfig {
	return GrpcConfig{
		GRPCAddress:     "localhost:10000",
		BackoffCfg:      backoff.DefaultConfig,
		NumberOfRetries: defaultMaxNumberOfRetries,
	}
}

func (g GrpcConfig) Validate() error {
	if g.GRPCAddress == "" {
		return ErrGRPCAddress
	}

	return nil
}

// NewGRPCTxRx creates a new gRPC TxRx.
func NewGRPCTxRx(
	ctx context.Context,
	logger logger.ILogger,
	cfg GrpcConfig,
	onConnected func(),
	m internal.Manager,
) (*GRPC, <-chan error, error) {
	txRx := GRPC{
		cfg:         cfg,
		m:           m,
		logger:      logger,
		state:       disconnectedState,
		onConnected: onConnected,
	}
	// launch goroutines to manage the connection
	// run goroutines for connection management and stream listening
	g, ctx := errgroup.WithContext(ctx)

	// Goroutine 1: Manages the connection lifecycle
	g.Go(func() error {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				txRx.logger.Info("Connection manager shutting down")

				return ctx.Err()
			case <-ticker.C:
				if txRx.getState() != disconnectedState {
					continue
				}

				txRx.logger.Debug("Connection manager trying to connect")

				if err := txRx.connect(ctx); err != nil {
					txRx.logger.Error("Failed to connect", "error", err)

					return err
				}

				txRx.logger.Info("Connected to gRPC server")

				if txRx.onConnected != nil {
					txRx.onConnected()
				}
			}
		}
	})

	g.Go(func() error {
		txRx.listenToStream(ctx)

		return nil
	})

	errChan := make(chan error, 1)

	go func() {
		errWait := g.Wait()
		if errWait != nil {
			errChan <- errWait

			close(errChan)
		}
	}()

	return &txRx, errChan, nil
}

func (g *GRPC) Send(m *reventv1.ClientToServerMessage) error {
	if g.getStream() == nil {
		return ErrStreamClosed
	}

	return g.getStream().Send(m)
}

func (g *GRPC) connect(ctx context.Context) error {
	if !g.connecting.CompareAndSwap(false, true) {
		// Another goroutine is already connecting
		return nil
	}
	defer g.connecting.CompareAndSwap(true, false)

	g.setState(connectingState)

	// Close old connection if it exists
	oldConn := g.getConn()
	if oldConn != nil {
		g.setStream(nil)

		if err := oldConn.Close(); err != nil {
			g.logger.Warn("Failed to close old gRPC connection", "error", err)
		}
	}

	// Create new gRPC client connection
	gRPCClientConn, errClient := grpc.NewClient(
		g.cfg.GRPCAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           g.cfg.BackoffCfg,
			MinConnectTimeout: 1 * time.Second,
		}),
		// Inject tracing information for R-Event
		// grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if errClient != nil {
		g.setState(disconnectedState)

		return fmt.Errorf("failed to instantiate GRPC client: %w", errClient)
	}

	cc := reventv1.NewControlClient(gRPCClientConn)
	expBackoff := backoff2.Exponential{Config: g.cfg.BackoffCfg}

	maxAttempts := int(g.cfg.NumberOfRetries)
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		gRPCStream, err := cc.OpenSession(ctx)
		if err != nil {
			if st, ok := status.FromError(err); ok {
				if st.Code() == codes.Unavailable {
					g.logger.Warn("R-Event server is not available",
						"attempt", attempt,
						"maxAttempts", maxAttempts,
					)

					time.Sleep(expBackoff.Backoff(attempt))

					continue
				}
			}

			// For non-retryable errors, close connection and fail
			if errClose := gRPCClientConn.Close(); errClose != nil {
				g.logger.Warn("Failed to close gRPC connection after session error", "error", errClose)
			}

			g.setState(disconnectedState)

			return fmt.Errorf("failed to open session: %w", err)
		}

		// Connection successful
		g.setConn(gRPCClientConn)
		g.setStream(gRPCStream)
		g.setState(connectedState)

		return nil
	}

	// All attempts exhausted
	if err := gRPCClientConn.Close(); err != nil {
		g.logger.Warn("Failed to close gRPC connection after max retries", "error", err)
	}

	g.setState(disconnectedState)

	return CantConnectToServerError{
		Addr:        g.cfg.GRPCAddress,
		NumAttempts: maxAttempts,
	}
}

// listenToStream continuously reads from the gRPC stream and processes incoming messages.
func (g *GRPC) listenToStream(ctx context.Context) {
	//nolint:mnd // refactor later
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		stream := g.getStream()
		if stream == nil {
			select {
			case <-ctx.Done():
				g.logger.Debug("Stream listener context cancelled")

				return
			case <-ticker.C:
				continue
			}
		}

		msg, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				g.logger.Debug("Stream listener context cancelled")

				return
			}

			g.logger.Error("Error receiving message from stream",
				"error", err,
			)
			// Mark as disconnected so reconnection is triggered
			g.setStream(nil)
			g.setState(disconnectedState)

			continue
		}

		g.m.Recv(msg)
	}
}

func (g *GRPC) getConn() *grpc.ClientConn {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.conn
}

func (g *GRPC) setConn(conn *grpc.ClientConn) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.conn = conn
}

func (g *GRPC) setState(state connectionState) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.state == state {
		// No state change
		return
	}

	g.state = state
}

func (g *GRPC) getState() connectionState {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.state
}

func (g *GRPC) setStream(
	stream grpc.BidiStreamingClient[reventv1.ClientToServerMessage, reventv1.ServerToClientMessage],
) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.stream = stream
}

func (g *GRPC) getStream() grpc.BidiStreamingClient[reventv1.ClientToServerMessage, reventv1.ServerToClientMessage] {
	g.mu.RLock()
	defer g.mu.RUnlock()

	return g.stream
}

func (c CantConnectToServerError) Error() string {
	return fmt.Sprintf("failed to connect to server at %s after %d attempts", c.Addr, c.NumAttempts)
}
