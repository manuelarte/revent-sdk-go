package bdd

import (
	"context"
	"fmt"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type serverInfo struct {
	container testcontainers.Container
	host      string
	grpcPort  int
	restPort  int
}

func startServer(ctx context.Context) (*serverInfo, error) {
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        reventImage,
			ExposedPorts: []string{"10000/tcp", "10001/tcp"},
			WaitingFor: wait.ForAll(
				wait.ForListeningPort("10000/tcp"),
				wait.ForListeningPort("10001/tcp"),
			).WithDeadline(60 * time.Second),
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

func (s *serverInfo) restart(ctx context.Context) error {
	// Restart the container completely by terminating and creating a new one
	container := s.container

	// Terminate the old container
	terminateCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	_ = container.Terminate(terminateCtx)
	cancel()

	// Wait a tiny bit
	time.Sleep(200 * time.Millisecond)

	// Start a new container with the same configuration
	newContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        reventImage,
			ExposedPorts: []string{"10000/tcp", "10001/tcp"},
			WaitingFor: wait.ForAll(
				wait.ForListeningPort("10000/tcp"),
				wait.ForListeningPort("10001/tcp"),
			).WithDeadline(60 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		return fmt.Errorf("error starting new container after restart: %w", err)
	}

	// Get the new host and ports
	host, err := newContainer.Host(ctx)
	if err != nil {
		_ = newContainer.Terminate(ctx)
		return fmt.Errorf("error getting new container host: %w", err)
	}

	grpcPort, err := newContainer.MappedPort(ctx, "10000/tcp")
	if err != nil {
		_ = newContainer.Terminate(ctx)
		return fmt.Errorf("error mapping new gRPC port: %w", err)
	}

	restPort, err := newContainer.MappedPort(ctx, "10001/tcp")
	if err != nil {
		_ = newContainer.Terminate(ctx)
		return fmt.Errorf("error mapping new REST port: %w", err)
	}

	// Update serverInfo with the new container and port info
	s.container = newContainer
	s.host = host
	s.grpcPort = int(grpcPort.Num())
	s.restPort = int(restPort.Num())

	return nil
}
