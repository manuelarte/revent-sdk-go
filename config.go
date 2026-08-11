package revent_sdk_go

import (
	"errors"
	"fmt"
	"os"

	"google.golang.org/grpc/backoff"

	"github.com/manuelarte/revent-sdk-go/logger"
)

const defaultMaxNumberOfRetries = 2

var (
	ErrNumberOfRetries = errors.New("NumberOfRetries must be greater than 0 and lower than 10")
	ErrGRPCAddress     = errors.New("GRPCAddress is required")
)

type (
	//go:structinit
	Config struct {
		// ClientID to be used to register in R-Event.
		ClientID ClientID
		// logger interface
		Logger logger.ILogger
		// gRPC config
		GRPCCfg GrpcConfig
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
)

// DefaultConfig returns a default configuration.
func DefaultConfig() Config {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "local"
	}

	return Config{
		ClientID: ClientID(hostname),
		Logger:   &logger.EmptyLogger{},
		GRPCCfg: GrpcConfig{
			GRPCAddress:     "localhost:10000",
			BackoffCfg:      backoff.DefaultConfig,
			NumberOfRetries: defaultMaxNumberOfRetries,
		},
	}
}

func (c Config) Validate() error {
	if err := c.ClientID.Validate(); err != nil {
		return fmt.Errorf("invalid ClientID: %w", err)
	}

	if c.GRPCCfg.GRPCAddress == "" {
		return ErrGRPCAddress
	}

	if c.GRPCCfg.NumberOfRetries == 0 {
		return ErrNumberOfRetries
	}

	return nil
}
