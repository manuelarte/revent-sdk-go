package revent_sdk_go

import (
	"testing"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid configuration",
			config: Config{
				ClientID:       "test-client",
				ServerURL:      "localhost",
				ServerGRPCPort: 5000,
				ServerRestPort: 8080,
			},
			wantErr: false,
		},
		{
			name: "missing client ID",
			config: Config{
				ClientID:       "",
				ServerURL:      "localhost",
				ServerGRPCPort: 5000,
				ServerRestPort: 8080,
			},
			wantErr: true,
			errMsg:  "invalid ClientID: ClientID is required",
		},
		{
			name: "missing server URL",
			config: Config{
				ClientID:       "test-client",
				ServerURL:      "",
				ServerGRPCPort: 5000,
				ServerRestPort: 8080,
			},
			wantErr: true,
			errMsg:  "server URL is required",
		},
		{
			name: "invalid gRPC port - zero",
			config: Config{
				ClientID:       "test-client",
				ServerURL:      "localhost",
				ServerGRPCPort: 0,
				ServerRestPort: 8080,
			},
			wantErr: true,
			errMsg:  "server gRPC port is required",
		},
		{
			name: "invalid gRPC port - negative",
			config: Config{
				ClientID:       "test-client",
				ServerURL:      "localhost",
				ServerGRPCPort: -1,
				ServerRestPort: 8080,
			},
			wantErr: true,
			errMsg:  "server gRPC port is required",
		},
		{
			name: "invalid REST port - zero",
			config: Config{
				ClientID:       "test-client",
				ServerURL:      "localhost",
				ServerGRPCPort: 5000,
				ServerRestPort: 0,
			},
			wantErr: true,
			errMsg:  "server REST port is required",
		},
		{
			name: "invalid REST port - negative",
			config: Config{
				ClientID:       "test-client",
				ServerURL:      "localhost",
				ServerGRPCPort: 5000,
				ServerRestPort: -1,
			},
			wantErr: true,
			errMsg:  "server REST port is required",
		},
		{
			name: "all fields empty",
			config: Config{
				ClientID:       "",
				ServerURL:      "",
				ServerGRPCPort: 0,
				ServerRestPort: 0,
			},
			wantErr: true,
			errMsg:  "invalid ClientID: " + ErrClientIDRequired.Error(),
		},
		{
			name: "high port numbers",
			config: Config{
				ClientID:       "test-client",
				ServerURL:      "example.com",
				ServerGRPCPort: 65535,
				ServerRestPort: 65535,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if tt.wantErr && err.Error() != tt.errMsg {
				t.Errorf("Validate() error = %q, want %q", err.Error(), tt.errMsg)
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
