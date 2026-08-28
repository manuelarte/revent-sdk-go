package revent_sdk_go

import (
	"testing"

	"github.com/manuelarte/revent-sdk-go/internal/txrx"
	"github.com/manuelarte/revent-sdk-go/logger"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.ClientID == "" {
		t.Error("DefaultConfig() ClientID should not be empty")
	}

	if cfg.Logger == nil {
		t.Error("DefaultConfig() Logger should not be nil")
	}

	err := cfg.Validate()
	if err != nil {
		t.Errorf("DefaultConfig() Validate() error = %v, want nil", err)
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "valid config with all fields set",
			config: Config{
				ClientID: "test-client",
				Logger:   &logger.EmptyLogger{},
				GRPCCfg:  txrx.DefaultGrpcConfig(),
			},
			wantErr: false,
		},
		{
			name: "invalid config with empty ClientID",
			config: Config{
				ClientID: "",
				Logger:   &logger.EmptyLogger{},
				GRPCCfg:  txrx.DefaultGrpcConfig(),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
