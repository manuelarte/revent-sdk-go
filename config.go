package revent_sdk_go

import (
	"errors"
	"fmt"
	"os"

	"google.golang.org/grpc/backoff"
)

const defaultMaxNumberOfRetries = 4

var (
	ErrNumberOfRetries = errors.New("NumberOfRetries must be greater than 0 and lower than 10")
	ErrServerURL       = errors.New("ServerURL is required")
	ErrServerGRPCPort  = errors.New("ServerGRPCPort is required")
	ErrServerRestPort  = errors.New("ServerRestPort is required")
)

//go:structinit
type Config struct {
	// ClientID to be used to register in R-Event.
	ClientID ClientID
	// ServerURL R-Event server url.
	ServerURL string
	// ServerGRPCPort R-Event gRPC port.
	ServerGRPCPort int
	// ServerRestPort R-Event REST port.
	ServerRestPort int
	// NumberOfRetries number of retries to connect to R-Event server.
	NumberOfRetries uint
	// Backoff configuration
	BackoffCfg backoff.Config
}

// DefaultConfig returns a default configuration.
func DefaultConfig() Config {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "local"
	}

	return Config{
		ClientID:        ClientID(hostname),
		ServerURL:       "localhost",
		ServerGRPCPort:  10000,
		ServerRestPort:  10001,
		NumberOfRetries: defaultMaxNumberOfRetries,
		BackoffCfg:      backoff.DefaultConfig,
	}
}

func (c Config) GetGRPCAddress() string {
	return fmt.Sprintf("%s:%d", c.ServerURL, c.ServerGRPCPort)
}

func (c Config) Validate() error {
	if err := c.ClientID.Validate(); err != nil {
		return fmt.Errorf("invalid ClientID: %w", err)
	}

	if c.ServerURL == "" {
		return ErrServerURL
	}

	if c.ServerGRPCPort <= 0 {
		return ErrServerGRPCPort
	}

	if c.ServerRestPort <= 0 {
		return ErrServerRestPort
	}

	if c.NumberOfRetries == 0 {
		return ErrNumberOfRetries
	}

	return nil
}
