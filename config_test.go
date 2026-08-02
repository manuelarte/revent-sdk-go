package revent_sdk_go

import (
	"errors"
	"testing"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr error
	}{
		{
			name: "valid configuration",
			config: Config{
				ClientID:        "test-client",
				ServerURL:       "localhost",
				ServerGRPCPort:  5000,
				ServerRestPort:  8080,
				NumberOfRetries: 4,
			},
			wantErr: nil,
		},
		{
			name: "missing client ID",
			config: Config{
				ClientID:        "",
				ServerURL:       "localhost",
				ServerGRPCPort:  5000,
				ServerRestPort:  8080,
				NumberOfRetries: 4,
			},
			wantErr: ErrClientIDRequired,
		},
		{
			name: "missing server URL",
			config: Config{
				ClientID:        "test-client",
				ServerURL:       "",
				ServerGRPCPort:  5000,
				ServerRestPort:  8080,
				NumberOfRetries: 4,
			},
			wantErr: ErrServerURL,
		},
		{
			name: "invalid gRPC port - zero",
			config: Config{
				ClientID:        "test-client",
				ServerURL:       "localhost",
				ServerGRPCPort:  0,
				ServerRestPort:  8080,
				NumberOfRetries: 4,
			},
			wantErr: ErrServerGRPCPort,
		},
		{
			name: "invalid gRPC port - negative",
			config: Config{
				ClientID:        "test-client",
				ServerURL:       "localhost",
				ServerGRPCPort:  -1,
				ServerRestPort:  8080,
				NumberOfRetries: 4,
			},
			wantErr: ErrServerGRPCPort,
		},
		{
			name: "invalid REST port - zero",
			config: Config{
				ClientID:        "test-client",
				ServerURL:       "localhost",
				ServerGRPCPort:  5000,
				ServerRestPort:  0,
				NumberOfRetries: 4,
			},
			wantErr: ErrServerRestPort,
		},
		{
			name: "invalid REST port - negative",
			config: Config{
				ClientID:        "test-client",
				ServerURL:       "localhost",
				ServerGRPCPort:  5000,
				ServerRestPort:  -1,
				NumberOfRetries: 4,
			},
			wantErr: ErrServerRestPort,
		},
		{
			name: "all fields empty",
			config: Config{
				ClientID:        "",
				ServerURL:       "",
				ServerGRPCPort:  0,
				ServerRestPort:  0,
				NumberOfRetries: 4,
			},
			wantErr: ErrClientIDRequired,
		},
		{
			name: "high port numbers",
			config: Config{
				ClientID:        "test-client",
				ServerURL:       "example.com",
				ServerGRPCPort:  65535,
				ServerRestPort:  65535,
				NumberOfRetries: 4,
			},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if !errors.Is(err, tt.wantErr) {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)

				return
			}
		})
	}
}

func TestGetGRPCAddress(t *testing.T) {
	tests := []struct {
		name     string
		config   Config
		expected string
	}{
		{
			name: "localhost with standard gRPC port",
			config: Config{
				ServerURL:      "localhost",
				ServerGRPCPort: 5000,
			},
			expected: "localhost:5000",
		},
		{
			name: "IP address with gRPC port",
			config: Config{
				ServerURL:      "192.168.1.1",
				ServerGRPCPort: 9090,
			},
			expected: "192.168.1.1:9090",
		},
		{
			name: "domain with gRPC port",
			config: Config{
				ServerURL:      "grpc.example.com",
				ServerGRPCPort: 443,
			},
			expected: "grpc.example.com:443",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.config.GetGRPCAddress()
			if result != tt.expected {
				t.Errorf("GetGRPCAddress() = %q, want %q", result, tt.expected)
			}
		})
	}
}
