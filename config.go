package revent_sdk_go

import (
	"fmt"
	"os"

	"github.com/manuelarte/revent-sdk-go/internal/txrx"
	"github.com/manuelarte/revent-sdk-go/logger"
)

type (
	//go:structinit
	Config struct {
		// ClientID to be used to register in R-Event.
		ClientID ClientID
		// logger interface
		Logger logger.ILogger
		// gRPC config
		GRPCCfg txrx.GrpcConfig
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
		GRPCCfg:  txrx.DefaultGrpcConfig(),
	}
}

func (c Config) Validate() error {
	if err := c.ClientID.Validate(); err != nil {
		return fmt.Errorf("invalid ClientID: %w", err)
	}

	if err := c.GRPCCfg.Validate(); err != nil {
		return fmt.Errorf("invalid GRPCCfg: %w", err)
	}

	return nil
}
