//nolint:mnd // magic numbers related to timeouts.
package bdd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
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
	gRPCPort, err := getFreePort()
	if err != nil {
		return nil, fmt.Errorf("error getting free port: %w", err)
	}

	restPort, err := getFreePort()
	if err != nil {
		return nil, fmt.Errorf("error getting free port: %w", err)
	}

	s := &serverInfo{
		grpcPort: gRPCPort,
		restPort: restPort,
	}

	errStart := s.startServer(ctx)

	return s, errStart
}

func (s *serverInfo) startServer(ctx context.Context) error {
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        reventImage,
			ExposedPorts: []string{"10000/tcp", "10001/tcp"},
			HostConfigModifier: func(hc *container.HostConfig) {
				hc.PortBindings = map[network.Port][]network.PortBinding{
					network.MustParsePort("10000/tcp"): {{HostIP: netip.IPv4Unspecified(), HostPort: strconv.Itoa(s.grpcPort)}},
					network.MustParsePort("10001/tcp"): {{HostIP: netip.IPv4Unspecified(), HostPort: strconv.Itoa(s.restPort)}},
				}
			},
			WaitingFor: wait.ForAll(
				wait.ForListeningPort("10000/tcp"),
				wait.ForListeningPort("10001/tcp"),
			).WithDeadline(15 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		return fmt.Errorf("error starting container: %w", err)
	}

	host, err := c.Host(ctx)
	if err != nil {
		_ = c.Terminate(ctx)

		return fmt.Errorf("error hosting container: %w", err)
	}

	s.container = c
	s.host = host

	return nil
}

func (s *serverInfo) restartServer(ctx context.Context) error {
	terminateCtx, terminateCancel := context.WithTimeout(ctx, 5*time.Second)
	_ = s.container.Terminate(terminateCtx)

	terminateCancel()

	time.Sleep(500 * time.Millisecond) // Give some time for the container to terminate before starting a new one.

	return s.startServer(ctx)
}

// getFreePort asks the kernel for a free open port that is ready to use.
func getFreePort() (int, error) {
	a, errResolve := net.ResolveTCPAddr("tcp", "localhost:0")
	if errResolve != nil {
		return 0, fmt.Errorf("error resolving localhost: %w", errResolve)
	}

	l, err := net.ListenTCP("tcp", a)
	if err != nil {
		return 0, fmt.Errorf("error listening on localhost: %w", err)
	}
	defer func(l *net.TCPListener) {
		_ = l.Close()
	}(l)

	casted, ok := l.Addr().(*net.TCPAddr)
	if !ok {
		return 0, errors.New("error converting laddr to TCPAddr")
	}

	return casted.Port, nil
}
