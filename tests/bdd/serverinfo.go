//nolint:mnd // magic numbers related to timeouts.
package bdd

import (
	"context"
	"fmt"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	reventsdkgo "github.com/manuelarte/revent-sdk-go"
)

type serverInfo struct {
	container testcontainers.Container
	host      string
	grpcPort  int
	restPort  int
}

func (s *serverInfo) update(cfg *reventsdkgo.Config) {
	cfg.ServerURL = s.host
	cfg.ServerGRPCPort = s.grpcPort
	cfg.ServerRestPort = s.restPort
}

func startServer(ctx context.Context) (*serverInfo, error) {
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        reventImage,
			ExposedPorts: []string{"10000/tcp", "10001/tcp"},
			WaitingFor: wait.ForAll(
				wait.ForListeningPort("10000/tcp"),
				wait.ForListeningPort("10001/tcp"),
			).WithDeadline(15 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		return nil, fmt.Errorf("error starting container: %w", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(ctx)

		return nil, fmt.Errorf("error hosting container: %w", err)
	}

	grpcPort, err := container.MappedPort(ctx, "10000/tcp")
	if err != nil {
		_ = container.Terminate(ctx)

		return nil, fmt.Errorf("error mapping gRPC port: %w", err)
	}

	restPort, err := container.MappedPort(ctx, "10001/tcp")
	if err != nil {
		_ = container.Terminate(ctx)

		return nil, fmt.Errorf("error mapping REST port: %w", err)
	}

	return &serverInfo{
		container: container,
		host:      host,
		grpcPort:  int(grpcPort.Num()),
		restPort:  int(restPort.Num()),
	}, nil
}
