// Package agent is NebulaOS's node agent (the kubelet equivalent). It
// heartbeats to the control plane, receives the full set of tasks the leader
// wants on this node, and converges the local runtime towards it.
//
// The agent is level-triggered and keeps no durable state of its own: on
// start it reaps anything a previous incarnation left behind, and while the
// control plane is unreachable it keeps running whatever it already has.
package agent

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	pb "github.com/coolstack-coder/nebulaos/gen/nebula/v1"
	"github.com/coolstack-coder/nebulaos/internal/grpcx"
	"github.com/coolstack-coder/nebulaos/internal/runtime"
	"github.com/coolstack-coder/nebulaos/internal/state"
	"google.golang.org/grpc"
)

type Config struct {
	NodeID     string
	Address    string // gRPC address of this node's server, if it runs one
	APIAddress string
	Capacity   state.Resources
	Labels     map[string]string
	Voter      bool
	Interval   time.Duration
	// Servers lists control-plane gRPC addresses to try, in order.
	Servers []string
	// MaxFailures is how many consecutive crashes a task may have before it
	// is reported failed (and the leader reschedules it elsewhere).
	MaxFailures int
	Logger      *slog.Logger
}

type localTask struct {
	spec        runtime.Spec
	restarts    int32
	consecutive int
	nextRestart time.Time
	runningFrom time.Time
}

type Agent struct {
	cfg Config
	rt  runtime.Runtime
	log *slog.Logger

	mu       sync.Mutex
	conns    map[string]*grpc.ClientConn
	tasks    map[string]*localTask
	server   int
	leader   string
	lastBeat time.Time
	lastErr  error
	count    int // len(tasks), readable without racing the sync loop
}

func New(cfg Config, rt runtime.Runtime) *Agent {
	if cfg.Interval == 0 {
		cfg.Interval = 500 * time.Millisecond
	}
	if cfg.MaxFailures == 0 {
		cfg.MaxFailures = 3
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Agent{
		cfg: cfg, rt: rt,
		log:   cfg.Logger.With("component", "agent", "node", cfg.NodeID),
		conns: map[string]*grpc.ClientConn{},
		tasks: map[string]*localTask{},
	}
}

// Run blocks until ctx is cancelled.
func (a *Agent) Run(ctx context.Context) {
	if ids, err := a.rt.List(ctx); err == nil {
		for _, id := range ids {
			a.log.Info("reaping leftover task", "task", id)
			_ = a.rt.Stop(ctx, id)
		}
	}
	t := time.NewTicker(a.cfg.Interval)
	defer t.Stop()
	for {
		a.beat(ctx)
		select {
		case <-ctx.Done():
			for id := range a.tasks {
				_ = a.rt.Stop(context.Background(), id)
			}
			for _, c := range a.conns {
				_ = c.Close()
			}
			return
		case <-t.C:
		}
	}
}

func (a *Agent) client(addr string) (pb.ClusterClient, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if c, ok := a.conns[addr]; ok {
		return pb.NewClusterClient(c), nil
	}
	c, err := grpcx.Dial(addr)
	if err != nil {
		return nil, err
	}
	a.conns[addr] = c
	return pb.NewClusterClient(c), nil
}

func (a *Agent) beat(ctx context.Context) {
	req := a.report()
	var (
		resp *pb.HeartbeatResponse
		err  error
	)
	// Try each control-plane server once, starting with the last one that
	// worked. Servers forward to the leader, so any healthy one will do.
	for i := 0; i < len(a.cfg.Servers); i++ {
		addr := a.cfg.Servers[(a.server+i)%len(a.cfg.Servers)]
		c, cerr := a.client(addr)
		if cerr != nil {
			err = cerr
			continue
		}
		hctx, cancel := context.WithTimeout(ctx, time.Second)
		resp, err = c.Heartbeat(hctx, req)
		cancel()
		if err == nil {
			a.server = (a.server + i) % len(a.cfg.Servers)
			break
		}
	}
	if err != nil || resp == nil {
		if err == nil {
			err = errors.New("no control-plane servers configured")
		}
		a.mu.Lock()
		a.lastErr = err
		a.mu.Unlock()
		// Keep running what we have; the control plane may just be electing.
		a.superviseLocal(ctx)
		return
	}
	a.mu.Lock()
	a.leader, a.lastBeat, a.lastErr = resp.LeaderId, time.Now(), nil
	a.mu.Unlock()
	a.converge(ctx, resp.Assignments)
	a.mu.Lock()
	a.count = len(a.tasks)
	a.mu.Unlock()
}

func (a *Agent) report() *pb.HeartbeatRequest {
	req := &pb.HeartbeatRequest{
		NodeId: a.cfg.NodeID, Address: a.cfg.Address, ApiAddress: a.cfg.APIAddress,
		Capacity: &pb.Resources{CpuMillis: a.cfg.Capacity.CPU, MemoryMb: a.cfg.Capacity.Memory},
		Labels:   a.cfg.Labels, Runtime: a.rt.Name(), Voter: a.cfg.Voter,
	}
	var used state.Resources
	for id, lt := range a.tasks {
		st, ok := a.rt.Status(id)
		ts := &pb.TaskStatus{TaskId: id, Restarts: lt.restarts}
		switch {
		case !ok:
			ts.State = "starting"
		case st.State == runtime.Running:
			ts.State = "running"
			ts.CpuMillis, ts.MemoryMb = st.CPU, st.Memory
			ts.StartedUnixMs = st.StartedAt.UnixMilli()
			used = used.Add(state.Resources{CPU: st.CPU, Memory: st.Memory})
		case (st.State == runtime.Failed || st.State == runtime.Exited) && lt.consecutive >= a.cfg.MaxFailures:
			ts.State, ts.Message = "failed", st.Message
		default:
			ts.State, ts.Message = "starting", st.Message
		}
		req.Tasks = append(req.Tasks, ts)
	}
	req.Usage = &pb.Resources{CpuMillis: used.CPU, MemoryMb: used.Memory}
	return req
}

func specOf(as *pb.TaskAssignment) runtime.Spec {
	return runtime.Spec{
		TaskID: as.TaskId, Service: as.Service, Image: as.Image, Command: as.Command, Env: as.Env,
		CPU: as.GetRequest().GetCpuMillis(), Memory: as.GetRequest().GetMemoryMb(), Port: int(as.Port),
		SimDemand: as.SimDemandMillis, Replicas: int(as.Replicas),
	}
}

// converge makes the runtime match the leader's desired set.
func (a *Agent) converge(ctx context.Context, want []*pb.TaskAssignment) {
	desired := make(map[string]*pb.TaskAssignment, len(want))
	for _, as := range want {
		desired[as.TaskId] = as
	}
	for id := range a.tasks {
		if _, ok := desired[id]; !ok {
			a.log.Info("stopping task", "task", id)
			_ = a.rt.Stop(ctx, id)
			delete(a.tasks, id)
		}
	}
	for id, as := range desired {
		spec := specOf(as)
		lt, ok := a.tasks[id]
		if !ok {
			a.log.Info("starting task", "task", id, "image", spec.Image)
			lt = &localTask{spec: spec}
			a.tasks[id] = lt
			if err := a.rt.Start(ctx, spec); err != nil {
				a.log.Warn("start failed", "task", id, "err", err)
				lt.consecutive++
				lt.nextRestart = time.Now().Add(time.Second)
			}
			continue
		}
		lt.spec = spec
		a.rt.Update(spec)
	}
	a.superviseLocal(ctx)
}

// superviseLocal restarts crashed tasks in place with exponential backoff.
func (a *Agent) superviseLocal(ctx context.Context) {
	now := time.Now()
	for id, lt := range a.tasks {
		st, ok := a.rt.Status(id)
		if ok && st.State == runtime.Running {
			if lt.runningFrom.IsZero() {
				lt.runningFrom = now
			}
			if now.Sub(lt.runningFrom) > 10*time.Second {
				lt.consecutive = 0
			}
			continue
		}
		if ok && st.State == runtime.Starting {
			continue
		}
		if now.Before(lt.nextRestart) || lt.consecutive >= a.cfg.MaxFailures {
			continue
		}
		lt.restarts++
		lt.consecutive++
		lt.runningFrom = time.Time{}
		backoff := min(time.Second<<lt.consecutive, 30*time.Second)
		lt.nextRestart = now.Add(backoff)
		a.log.Info("restarting task", "task", id, "attempt", lt.consecutive, "backoff", backoff)
		_ = a.rt.Stop(ctx, id)
		if err := a.rt.Start(ctx, lt.spec); err != nil {
			a.log.Warn("restart failed", "task", id, "err", err)
		}
	}
}

type Status struct {
	Leader      string `json:"leader"`
	LastBeatMs  int64  `json:"last_heartbeat_ms"`
	Error       string `json:"error,omitempty"`
	LocalTasks  int    `json:"local_tasks"`
	RuntimeName string `json:"runtime"`
}

func (a *Agent) Status() Status {
	a.mu.Lock()
	defer a.mu.Unlock()
	s := Status{Leader: a.leader, LocalTasks: a.count, RuntimeName: a.rt.Name()}
	if !a.lastBeat.IsZero() {
		s.LastBeatMs = time.Since(a.lastBeat).Milliseconds()
	}
	if a.lastErr != nil {
		s.Error = a.lastErr.Error()
	}
	return s
}
