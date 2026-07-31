package revent_sdk_go

import (
	"errors"
	"fmt"
	"os"
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
}

// DefaultConfig returns a default configuration.
func DefaultConfig() Config {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "local"
	}

	return Config{
		ClientID:       ClientID(hostname),
		ServerURL:      "http://localhost",
		ServerGRPCPort: 10000,
		ServerRestPort: 10001,
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
		return errors.New("server URL is required")
	}

	if c.ServerGRPCPort <= 0 {
		return errors.New("server gRPC port is required")
	}

	if c.ServerRestPort <= 0 {
		return errors.New("server REST port is required")
	}

	return nil
}
