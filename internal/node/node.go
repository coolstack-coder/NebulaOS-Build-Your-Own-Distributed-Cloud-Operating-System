// Package node wires a NebulaOS process together: a Raft voter with its
// state machine and leader-only controller, a node agent with a runtime,
// the gRPC services peers talk to and the HTTP API humans talk to.
//
// A process can be a control-plane member (it appears in Peers) or a
// worker-only node that just runs an agent and joins through any server.
package node

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	pb "github.com/coolstack-coder/nebulaos/gen/nebula/v1"
	"github.com/coolstack-coder/nebulaos/internal/agent"
	"github.com/coolstack-coder/nebulaos/internal/controller"
	"github.com/coolstack-coder/nebulaos/internal/grpcx"
	"github.com/coolstack-coder/nebulaos/internal/metrics"
	"github.com/coolstack-coder/nebulaos/internal/raft"
	"github.com/coolstack-coder/nebulaos/internal/runtime"
	"github.com/coolstack-coder/nebulaos/internal/state"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Config struct {
	ID      string
	DataDir string
	// GRPCAddr serves Raft and the Cluster API; APIAddr serves HTTP.
	GRPCAddr string
	APIAddr  string
	// Advertise addresses default to the listen addresses.
	AdvertiseGRPC string
	AdvertiseAPI  string

	Peers map[string]string // control-plane voters: id -> gRPC address
	Join  []string          // for worker-only nodes: servers to heartbeat to

	Capacity state.Resources
	Labels   map[string]string
	Runtime  string
	Isolate  bool

	HeartbeatInterval time.Duration
	NodeTimeout       time.Duration
	ElectionTimeout   time.Duration
	SnapshotThreshold uint64
	Fsync             bool
	Logger            *slog.Logger

	// UI, if set, is served at / by the HTTP API.
	UI http.Handler
}

type Node struct {
	cfg  Config
	log  *slog.Logger
	fsm  *state.FSM
	raft *raft.Node
	ctrl *controller.Controller
	ag   *agent.Agent
	rt   runtime.Runtime

	grpcSrv *grpc.Server
	httpSrv *http.Server
	trans   *raft.GRPCTransport

	mu      sync.Mutex
	conns   map[string]*grpc.ClientConn
	cancel  context.CancelFunc
	agentWG sync.WaitGroup
	started time.Time
}

func (c *Config) isVoter() bool { _, ok := c.Peers[c.ID]; return ok }

func New(cfg Config) (*Node, error) {
	if cfg.ID == "" {
		return nil, errors.New("node id is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.AdvertiseGRPC == "" {
		cfg.AdvertiseGRPC = cfg.GRPCAddr
	}
	if cfg.AdvertiseAPI == "" {
		cfg.AdvertiseAPI = cfg.APIAddr
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}
	n := &Node{cfg: cfg, log: cfg.Logger.With("node", cfg.ID), conns: map[string]*grpc.ClientConn{}}

	rt, err := runtime.New(cfg.Runtime, runtime.Options{NodeID: cfg.ID, DataDir: cfg.DataDir, Isolate: cfg.Isolate})
	if err != nil {
		return nil, err
	}
	n.rt = rt

	if cfg.isVoter() {
		n.fsm = state.NewFSM()
		n.ctrl = controller.New(controller.Config{
			NodeID: cfg.ID, NodeTimeout: cfg.NodeTimeout, Logger: cfg.Logger,
		}, n.fsm, n.Submit)
		store, err := raft.NewFileStorage(filepath.Join(cfg.DataDir, "raft"), cfg.Fsync)
		if err != nil {
			return nil, err
		}
		n.trans = raft.NewGRPCTransport()
		et := cfg.ElectionTimeout
		n.raft, err = raft.New(raft.Config{
			ID: cfg.ID, Peers: cfg.Peers,
			ElectionTimeoutMin: et, ElectionTimeoutMax: 2 * et,
			SnapshotThreshold: cfg.SnapshotThreshold,
			Storage:           store, Transport: n.trans, FSM: n.fsm, Logger: cfg.Logger,
			OnLeaderChange: n.onLeaderChange,
		})
		if err != nil {
			return nil, err
		}
	}

	servers := cfg.Join
	if cfg.isVoter() {
		// Our own server forwards to the leader, so heartbeat locally first.
		// Other voters forward too, which keeps us heartbeating while our
		// own Raft instance is still catching up after a restart.
		servers = []string{cfg.AdvertiseGRPC}
		for id, addr := range cfg.Peers {
			if id != cfg.ID {
				servers = append(servers, addr)
			}
		}
		servers = append(servers, cfg.Join...)
	}
	n.ag = agent.New(agent.Config{
		NodeID: cfg.ID, Address: cfg.AdvertiseGRPC, APIAddress: cfg.AdvertiseAPI,
		Capacity: cfg.Capacity, Labels: cfg.Labels, Voter: cfg.isVoter(),
		Interval: cfg.HeartbeatInterval, Servers: servers, Logger: cfg.Logger,
	}, rt)
	return n, nil
}

func (n *Node) onLeaderChange(leader bool, term uint64) {
	metrics.RaftIsLeader.Set(map[bool]float64{true: 1, false: 0}[leader])
	if leader {
		metrics.RaftLeaderChanges.Inc()
		n.ctrl.Start(term)
	} else {
		n.ctrl.Stop()
	}
}

func (n *Node) Start() error {
	n.started = time.Now()
	lis, err := net.Listen("tcp", n.cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}
	n.grpcSrv = grpc.NewServer(grpc.MaxRecvMsgSize(64<<20), grpc.MaxSendMsgSize(64<<20))
	pb.RegisterClusterServer(n.grpcSrv, &clusterServer{n: n})
	if n.raft != nil {
		pb.RegisterRaftServer(n.grpcSrv, &raft.GRPCServer{Node: n.raft})
	}
	go func() { _ = n.grpcSrv.Serve(lis) }()

	hl, err := net.Listen("tcp", n.cfg.APIAddr)
	if err != nil {
		return fmt.Errorf("http listen: %w", err)
	}
	n.httpSrv = &http.Server{Handler: n.routes(), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = n.httpSrv.Serve(hl) }()

	if n.raft != nil {
		n.raft.Start()
		go n.raftGauges()
	}
	ctx, cancel := context.WithCancel(context.Background())
	n.cancel = cancel
	n.agentWG.Add(1)
	go func() {
		defer n.agentWG.Done()
		n.ag.Run(ctx)
	}()
	n.log.Info("node started", "grpc", n.cfg.GRPCAddr, "api", n.cfg.APIAddr, "voter", n.cfg.isVoter(), "runtime", n.rt.Name())
	return nil
}

func (n *Node) Stop() {
	if n.cancel != nil {
		n.cancel()
	}
	n.agentWG.Wait()
	if n.ctrl != nil {
		n.ctrl.Stop()
	}
	if n.raft != nil {
		n.raft.Stop()
	}
	if n.httpSrv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = n.httpSrv.Shutdown(ctx)
		cancel()
	}
	if n.grpcSrv != nil {
		n.grpcSrv.Stop()
	}
	if n.trans != nil {
		n.trans.Close()
	}
	_ = n.rt.Close()
}

func (n *Node) raftGauges() {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for range t.C {
		s := n.raft.Status()
		metrics.RaftTerm.Set(float64(s.Term))
		metrics.RaftCommitIndex.Set(float64(s.CommitIndex))
	}
}

func (n *Node) peerClient(addr string) (pb.ClusterClient, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if c, ok := n.conns[addr]; ok {
		return pb.NewClusterClient(c), nil
	}
	c, err := grpcx.Dial(addr)
	if err != nil {
		return nil, err
	}
	n.conns[addr] = c
	return pb.NewClusterClient(c), nil
}

// leaderClient returns a client for the current leader, waiting briefly if
// an election is in progress.
func (n *Node) leaderClient(ctx context.Context) (pb.ClusterClient, string, error) {
	if n.raft == nil {
		if len(n.cfg.Join) == 0 {
			return nil, "", errors.New("worker node has no servers to join")
		}
		c, err := n.peerClient(n.cfg.Join[0])
		return c, n.cfg.Join[0], err
	}
	for {
		id, addr := n.raft.Leader()
		if id != "" && id != n.cfg.ID {
			c, err := n.peerClient(addr)
			return c, id, err
		}
		if id == n.cfg.ID {
			return nil, id, nil
		}
		select {
		case <-ctx.Done():
			return nil, "", status.Error(codes.Unavailable, "no leader elected")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

const hopKey = "x-nebula-forwarded"

// Submit replicates cmd through Raft, forwarding to the leader if needed,
// and returns the state machine's error, if any.
func (n *Node) Submit(ctx context.Context, cmd state.Command) error {
	if cmd.Time == 0 {
		cmd.Time = time.Now().UnixMilli()
	}
	data := cmd.Encode()
	for {
		if n.raft != nil && n.raft.IsLeader() {
			res, err := n.raft.Apply(ctx, data)
			if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
				if ctx.Err() != nil {
					return err
				}
				continue
			}
			if err != nil {
				return err
			}
			if e, ok := res.(error); ok && e != nil {
				return e
			}
			return nil
		}
		c, _, err := n.leaderClient(ctx)
		if err != nil {
			return err
		}
		if c == nil {
			continue // we just became leader
		}
		fctx := metadata.AppendToOutgoingContext(ctx, hopKey, n.cfg.ID)
		resp, err := c.Submit(fctx, &pb.SubmitRequest{Command: data})
		if err != nil {
			if ctx.Err() != nil {
				return err
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if resp.Error == errNotLeaderMsg {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if resp.Error != "" {
			return errors.New(resp.Error)
		}
		return nil
	}
}

const errNotLeaderMsg = "not leader"

type clusterServer struct {
	pb.UnimplementedClusterServer
	n *Node
}

func forwarded(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	return ok && len(md.Get(hopKey)) > 0
}

func (s *clusterServer) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	n := s.n
	if n.raft != nil && n.raft.IsLeader() {
		return n.ctrl.Heartbeat(req), nil
	}
	if forwarded(ctx) {
		return nil, status.Error(codes.FailedPrecondition, errNotLeaderMsg)
	}
	wctx, cancel := context.WithTimeout(ctx, 800*time.Millisecond)
	defer cancel()
	c, _, err := n.leaderClient(wctx)
	if err != nil {
		return nil, err
	}
	if c == nil {
		return n.ctrl.Heartbeat(req), nil
	}
	return c.Heartbeat(metadata.AppendToOutgoingContext(wctx, hopKey, n.cfg.ID), req)
}

func (s *clusterServer) Submit(ctx context.Context, req *pb.SubmitRequest) (*pb.SubmitResponse, error) {
	n := s.n
	if n.raft == nil || !n.raft.IsLeader() {
		id, addr := "", ""
		if n.raft != nil {
			id, addr = n.raft.Leader()
		}
		return &pb.SubmitResponse{Error: errNotLeaderMsg, LeaderId: id, LeaderAddress: addr}, nil
	}
	res, err := n.raft.Apply(ctx, req.Command)
	if err != nil {
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
			return &pb.SubmitResponse{Error: errNotLeaderMsg}, nil
		}
		return &pb.SubmitResponse{Error: err.Error()}, nil
	}
	if e, ok := res.(error); ok && e != nil {
		return &pb.SubmitResponse{Error: e.Error()}, nil
	}
	return &pb.SubmitResponse{}, nil
}
