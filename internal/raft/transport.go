package raft

import (
	"context"
	"sync"

	pb "github.com/coolstack-coder/nebulaos/gen/nebula/v1"
	"github.com/coolstack-coder/nebulaos/internal/grpcx"
	"google.golang.org/grpc"
)

// GRPCTransport sends Raft RPCs over gRPC, caching one client connection per
// peer address. gRPC reconnects on its own, so a peer that crashes and comes
// back is picked up without any bookkeeping here.
type GRPCTransport struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

func NewGRPCTransport() *GRPCTransport {
	return &GRPCTransport{conns: map[string]*grpc.ClientConn{}}
}

func (t *GRPCTransport) client(addr string) (pb.RaftClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.conns[addr]; ok {
		return pb.NewRaftClient(c), nil
	}
	c, err := grpcx.Dial(addr)
	if err != nil {
		return nil, err
	}
	t.conns[addr] = c
	return pb.NewRaftClient(c), nil
}

func (t *GRPCTransport) RequestVote(ctx context.Context, addr string, req *pb.VoteRequest) (*pb.VoteResponse, error) {
	c, err := t.client(addr)
	if err != nil {
		return nil, err
	}
	return c.RequestVote(ctx, req)
}

func (t *GRPCTransport) AppendEntries(ctx context.Context, addr string, req *pb.AppendRequest) (*pb.AppendResponse, error) {
	c, err := t.client(addr)
	if err != nil {
		return nil, err
	}
	return c.AppendEntries(ctx, req)
}

func (t *GRPCTransport) InstallSnapshot(ctx context.Context, addr string, req *pb.SnapshotRequest) (*pb.SnapshotResponse, error) {
	c, err := t.client(addr)
	if err != nil {
		return nil, err
	}
	return c.InstallSnapshot(ctx, req)
}

func (t *GRPCTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, c := range t.conns {
		_ = c.Close()
	}
	t.conns = map[string]*grpc.ClientConn{}
}

// GRPCServer adapts a Node to the generated Raft service interface.
type GRPCServer struct {
	pb.UnimplementedRaftServer
	Node *Node
}

func (s *GRPCServer) RequestVote(_ context.Context, req *pb.VoteRequest) (*pb.VoteResponse, error) {
	return s.Node.HandleRequestVote(req), nil
}

func (s *GRPCServer) AppendEntries(_ context.Context, req *pb.AppendRequest) (*pb.AppendResponse, error) {
	return s.Node.HandleAppendEntries(req), nil
}

func (s *GRPCServer) InstallSnapshot(_ context.Context, req *pb.SnapshotRequest) (*pb.SnapshotResponse, error) {
	return s.Node.HandleInstallSnapshot(req), nil
}
