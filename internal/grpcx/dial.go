// Package rpc holds the gRPC client settings shared by every NebulaOS
// component.
package grpcx

import (
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

// Dial creates a lazily-connecting client. gRPC's default reconnect backoff
// grows to two minutes, which would make a rebooted node invisible to the
// leader for far longer than our failure detector's timeout. Cap it at one
// second so recovery is bounded by our own timers, not the transport's.
func Dial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  50 * time.Millisecond,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   time.Second,
			},
			MinConnectTimeout: time.Second,
		}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20), grpc.MaxCallSendMsgSize(64<<20)),
	)
}
