// Package controller contains the control loops that only the Raft leader
// runs: failure detection, replica reconciliation, scheduling, recovery
// tracking and autoscaling.
//
// Every decision is turned into a state.Command and replicated before it
// takes effect, so if the leader dies mid-decision the next leader simply
// observes the replicated state and carries on. Liveness data (heartbeats,
// resource usage) is deliberately *not* replicated: it changes constantly
// and a new leader rebuilds it within one heartbeat interval.
package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"sync"
	"time"

	pb "github.com/coolstack-coder/nebulaos/gen/nebula/v1"
	"github.com/coolstack-coder/nebulaos/internal/metrics"
	"github.com/coolstack-coder/nebulaos/internal/scheduler"
	"github.com/coolstack-coder/nebulaos/internal/state"
)

type Submitter func(ctx context.Context, c state.Command) error

type Config struct {
	NodeID string
	// NodeTimeout is how long a node may go without a heartbeat before it is
	// declared down and its tasks are rescheduled.
	NodeTimeout       time.Duration
	Tick              time.Duration
	AutoscaleInterval time.Duration
	// ScaleDownWindow is the stabilisation window for scale-down, as in the
	// Kubernetes HPA: we only shrink to the highest recommendation seen
	// during the window, which stops flapping.
	ScaleDownWindow time.Duration
	ScaleUpCooldown time.Duration
	Logger          *slog.Logger
}

func (c *Config) defaults() {
	if c.NodeTimeout == 0 {
		c.NodeTimeout = 3 * time.Second
	}
	if c.Tick == 0 {
		c.Tick = 100 * time.Millisecond
	}
	if c.AutoscaleInterval == 0 {
		c.AutoscaleInterval = 2 * time.Second
	}
	if c.ScaleDownWindow == 0 {
		c.ScaleDownWindow = 15 * time.Second
	}
	if c.ScaleUpCooldown == 0 {
		c.ScaleUpCooldown = 3 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

type liveNode struct {
	lastSeen time.Time
	usage    state.Resources
	tasks    map[string]*pb.TaskStatus
}

type recommendation struct {
	at      time.Time
	desired int
}

type incident struct {
	start time.Time
	nodes []string
}

type Controller struct {
	cfg    Config
	fsm    *state.FSM
	submit Submitter
	log    *slog.Logger

	mu          sync.Mutex
	running     bool
	cancel      context.CancelFunc
	done        chan struct{}
	live        map[string]*liveNode
	updates     map[string]state.TaskUpdate
	registering map[string]time.Time
	incident    *incident
	recs        map[string][]recommendation
	lastScale   map[string]time.Time
	lastAuto    time.Time
	schedLat    []float64 // recent scheduling latencies in ms, for the UI
	recoveries  []Recovery
}

type Recovery struct {
	At         int64    `json:"at"`
	Nodes      []string `json:"nodes"`
	DurationMs int64    `json:"duration_ms"`
}

func New(cfg Config, fsm *state.FSM, submit Submitter) *Controller {
	cfg.defaults()
	return &Controller{
		cfg: cfg, fsm: fsm, submit: submit,
		log:         cfg.Logger.With("component", "controller"),
		live:        map[string]*liveNode{},
		updates:     map[string]state.TaskUpdate{},
		registering: map[string]time.Time{},
		recs:        map[string][]recommendation{},
		lastScale:   map[string]time.Time{},
	}
}

// Start begins the control loops. It is called when this node becomes the
// Raft leader.
func (c *Controller) Start(term uint64) {
	c.mu.Lock()
	if c.running {
		c.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.running, c.cancel, c.done = true, cancel, make(chan struct{})
	// Everybody gets a full timeout's grace from the moment we take over: we
	// have no heartbeat history and must not shoot healthy nodes.
	now := time.Now()
	c.live = map[string]*liveNode{}
	c.fsm.Read(func(s *state.Snapshot) {
		for id := range s.Nodes {
			c.live[id] = &liveNode{lastSeen: now, tasks: map[string]*pb.TaskStatus{}}
		}
	})
	c.updates = map[string]state.TaskUpdate{}
	c.registering = map[string]time.Time{}
	// A new leader has no autoscaling history. Treat every service as just
	// scaled so the scale-down stabilisation window starts now; otherwise a
	// failover would shrink services on the strength of one noisy sample.
	c.recs = map[string][]recommendation{}
	c.lastScale = map[string]time.Time{}
	c.fsm.Read(func(s *state.Snapshot) {
		for name := range s.Services {
			c.lastScale[name] = now
		}
	})
	done := c.done
	c.mu.Unlock()

	go func() {
		defer close(done)
		c.emit(ctx, "leader_elected", fmt.Sprintf("%s elected leader for term %d", c.cfg.NodeID, term), c.cfg.NodeID, "", 0)
		c.loop(ctx)
	}()
}

// Stop halts the loops; called when leadership is lost.
func (c *Controller) Stop() {
	c.mu.Lock()
	if !c.running {
		c.mu.Unlock()
		return
	}
	c.running = false
	c.cancel()
	done := c.done
	c.mu.Unlock()
	<-done
}

func (c *Controller) loop(ctx context.Context) {
	t := time.NewTicker(c.cfg.Tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		c.detectFailures(ctx)
		c.flushUpdates(ctx)
		c.reconcile(ctx)
		c.schedule(ctx)
		c.trackRecovery(ctx)
		if time.Since(c.lastAuto) >= c.cfg.AutoscaleInterval {
			c.lastAuto = time.Now()
			c.autoscale(ctx)
		}
		c.updateGauges()
	}
}

func (c *Controller) do(ctx context.Context, cmd state.Command) error {
	cmd.Time = time.Now().UnixMilli()
	sctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	start := time.Now()
	err := c.submit(sctx, cmd)
	metrics.RaftApplyLatency.Observe(time.Since(start).Seconds())
	if err != nil && ctx.Err() == nil {
		c.log.Warn("command failed", "op", cmd.Op, "err", err)
	}
	return err
}

func (c *Controller) emit(ctx context.Context, kind, msg, node, svc string, dur int64) {
	_ = c.do(ctx, state.Command{Op: state.OpRecordEvent, Event: &state.Event{
		Kind: kind, Message: msg, Node: node, Service: svc, DurationMs: dur,
	}})
}

// ---------------------------------------------------------------- heartbeats

// Heartbeat records a node's report and returns its desired task set. Only
// called on the leader.
func (c *Controller) Heartbeat(req *pb.HeartbeatRequest) *pb.HeartbeatResponse {
	now := time.Now()
	c.mu.Lock()
	ln := c.live[req.NodeId]
	if ln == nil {
		ln = &liveNode{}
		c.live[req.NodeId] = ln
	}
	ln.lastSeen = now
	ln.usage = state.Resources{CPU: req.GetUsage().GetCpuMillis(), Memory: req.GetUsage().GetMemoryMb()}
	ln.tasks = make(map[string]*pb.TaskStatus, len(req.Tasks))
	for _, t := range req.Tasks {
		ln.tasks[t.TaskId] = t
	}
	c.mu.Unlock()

	resp := &pb.HeartbeatResponse{LeaderId: c.cfg.NodeID}
	needRegister := false
	// Lock order is always c.mu before the FSM lock, so collect updates
	// here and record them after Read returns.
	var ups []state.TaskUpdate
	c.fsm.Read(func(s *state.Snapshot) {
		n, ok := s.Nodes[req.NodeId]
		resp.Known = ok
		needRegister = !ok || n.Status == state.NodeDown || n.Address != req.Address || n.APIAddress != req.ApiAddress
		if !ok || n.Status == state.NodeDown {
			return
		}
		for _, ts := range req.Tasks {
			t, ok := s.Tasks[ts.TaskId]
			if !ok || t.Node != req.NodeId || !t.State.Active() {
				continue
			}
			var want state.TaskState
			switch ts.State {
			case "running":
				want = state.TaskRunning
			case "failed":
				want = state.TaskFailed
			default:
				continue
			}
			if t.State != want || t.Restarts != ts.Restarts {
				ups = append(ups, state.TaskUpdate{TaskID: t.ID, NodeID: req.NodeId, State: want, Message: ts.Message, Restarts: ts.Restarts})
			}
		}
		for _, t := range s.Tasks {
			if t.Node != req.NodeId || !t.State.Active() {
				continue
			}
			svc := s.Services[t.Service]
			if svc == nil {
				continue
			}
			sp := svc.Spec
			resp.Assignments = append(resp.Assignments, &pb.TaskAssignment{
				TaskId: t.ID, Service: t.Service, Image: sp.Image, Command: sp.Command, Env: sp.Env,
				Request:         &pb.Resources{CpuMillis: t.Resources.CPU, MemoryMb: t.Resources.Memory},
				Port:            int32(sp.Port),
				SimDemandMillis: sp.SimDemand,
				Replicas:        int32(sp.Replicas),
			})
		}
	})
	if len(ups) > 0 {
		c.mu.Lock()
		for _, u := range ups {
			c.updates[u.TaskID] = u
		}
		c.mu.Unlock()
	}
	if needRegister {
		c.register(req)
	}
	return resp
}

func (c *Controller) register(req *pb.HeartbeatRequest) {
	c.mu.Lock()
	if t, ok := c.registering[req.NodeId]; ok && time.Since(t) < 2*time.Second {
		c.mu.Unlock()
		return
	}
	c.registering[req.NodeId] = time.Now()
	c.mu.Unlock()
	node := &state.Node{
		ID: req.NodeId, Address: req.Address, APIAddress: req.ApiAddress,
		Capacity: state.Resources{CPU: req.GetCapacity().GetCpuMillis(), Memory: req.GetCapacity().GetMemoryMb()},
		Labels:   req.Labels, Voter: req.Voter, Runtime: req.Runtime,
	}
	go func() {
		_ = c.do(context.Background(), state.Command{Op: state.OpRegisterNode, Node: node})
		c.mu.Lock()
		delete(c.registering, req.NodeId)
		c.mu.Unlock()
	}()
}

func (c *Controller) flushUpdates(ctx context.Context) {
	c.mu.Lock()
	if len(c.updates) == 0 {
		c.mu.Unlock()
		return
	}
	ups := make([]state.TaskUpdate, 0, len(c.updates))
	for _, u := range c.updates {
		ups = append(ups, u)
	}
	c.updates = map[string]state.TaskUpdate{}
	c.mu.Unlock()

	// Measure create→running before the update lands.
	var created = map[string]int64{}
	c.fsm.Read(func(s *state.Snapshot) {
		for _, u := range ups {
			if t := s.Tasks[u.TaskID]; t != nil && u.State == state.TaskRunning && t.State != state.TaskRunning {
				created[u.TaskID] = t.CreatedAt
			}
		}
	})
	if c.do(ctx, state.Command{Op: state.OpUpdateTasks, Updates: ups}) == nil {
		now := time.Now().UnixMilli()
		for _, ts := range created {
			metrics.TaskStartup.Observe(float64(now-ts) / 1000)
		}
	}
}

// ---------------------------------------------------------------- failures

func (c *Controller) detectFailures(ctx context.Context) {
	now := time.Now()
	type down struct {
		id     string
		silent time.Duration
	}
	var dead []down
	c.mu.Lock()
	c.fsm.Read(func(s *state.Snapshot) {
		for id, n := range s.Nodes {
			if n.Status == state.NodeDown {
				continue
			}
			ln := c.live[id]
			if ln == nil {
				ln = &liveNode{lastSeen: now, tasks: map[string]*pb.TaskStatus{}}
				c.live[id] = ln
			}
			if silent := now.Sub(ln.lastSeen); silent > c.cfg.NodeTimeout {
				dead = append(dead, down{id, silent})
			}
		}
	})
	c.mu.Unlock()

	for _, d := range dead {
		reason := fmt.Sprintf("no heartbeat for %.1fs", d.silent.Seconds())
		if c.do(ctx, state.Command{Op: state.OpSetNodeStatus, NodeID: d.id, Status: state.NodeDown, Reason: reason}) != nil {
			continue
		}
		c.log.Warn("node down", "node", d.id, "reason", reason)
		metrics.NodeFailures.Inc()
		c.mu.Lock()
		if c.incident == nil {
			c.incident = &incident{start: now}
		}
		c.incident.nodes = append(c.incident.nodes, d.id)
		c.mu.Unlock()
	}
}

func (c *Controller) trackRecovery(ctx context.Context) {
	c.mu.Lock()
	inc := c.incident
	c.mu.Unlock()
	if inc == nil {
		return
	}
	healthy := true
	total := 0
	c.fsm.Read(func(s *state.Snapshot) {
		running := map[string]int{}
		for _, t := range s.Tasks {
			if t.State == state.TaskRunning {
				running[t.Service]++
			}
		}
		for name, svc := range s.Services {
			total += svc.Spec.Replicas
			if running[name] < svc.Spec.Replicas {
				healthy = false
			}
		}
	})
	elapsed := time.Since(inc.start)
	if !healthy && elapsed < 2*time.Minute {
		return
	}
	c.mu.Lock()
	c.incident = nil
	c.mu.Unlock()
	if !healthy {
		c.emit(ctx, "recovery_incomplete", fmt.Sprintf("not enough capacity to restore all replicas after losing %v", inc.nodes), "", "", 0)
		return
	}
	ms := elapsed.Milliseconds()
	metrics.RecoveryTime.Observe(elapsed.Seconds())
	c.mu.Lock()
	c.recoveries = append(c.recoveries, Recovery{At: time.Now().UnixMilli(), Nodes: inc.nodes, DurationMs: ms})
	if len(c.recoveries) > 20 {
		c.recoveries = c.recoveries[1:]
	}
	c.mu.Unlock()
	c.emit(ctx, "recovered", fmt.Sprintf("self-healed: all %d replicas running again %dms after %v failed", total, ms, inc.nodes), "", "", ms)
}

// ---------------------------------------------------------------- reconcile

func (c *Controller) reconcile(ctx context.Context) {
	var (
		create []state.Task
		remove []string
	)
	c.fsm.Read(func(s *state.Snapshot) {
		names := make([]string, 0, len(s.Services))
		for n := range s.Services {
			names = append(names, n)
		}
		sort.Strings(names)
		perNode := map[string]int{}
		for _, t := range s.Tasks {
			if t.State.Active() && t.Node != "" {
				perNode[t.Node]++
			}
		}
		for _, name := range names {
			svc := s.Services[name]
			var active, dead []*state.Task
			for _, t := range s.TasksFor(name) {
				if t.State.Active() {
					active = append(active, t)
				} else {
					dead = append(dead, t)
				}
			}
			for i := len(active); i < svc.Spec.Replicas; i++ {
				t := state.Task{ID: name + "-" + shortID(), Service: name, Resources: svc.Spec.Resources}
				if j := i - len(active); j < len(dead) {
					t.ReplacedNode = dead[j].Node
				}
				create = append(create, t)
			}
			for _, t := range dead {
				remove = append(remove, t.ID)
			}
			if extra := len(active) - svc.Spec.Replicas; extra > 0 {
				// Scale down the least valuable replicas first: unscheduled,
				// then not-yet-running, then those on the busiest nodes.
				rank := map[state.TaskState]int{state.TaskPending: 0, state.TaskAssigned: 1, state.TaskRunning: 2}
				sort.SliceStable(active, func(i, j int) bool {
					a, b := active[i], active[j]
					if rank[a.State] != rank[b.State] {
						return rank[a.State] < rank[b.State]
					}
					return perNode[a.Node] > perNode[b.Node]
				})
				for _, t := range active[:extra] {
					remove = append(remove, t.ID)
				}
			}
		}
	})
	if len(create) > 0 {
		_ = c.do(ctx, state.Command{Op: state.OpCreateTasks, Tasks: create})
	}
	if len(remove) > 0 {
		_ = c.do(ctx, state.Command{Op: state.OpRemoveTasks, TaskIDs: remove})
	}
}

func shortID() string {
	b := make([]byte, 3)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (c *Controller) schedule(ctx context.Context) {
	var res scheduler.Result
	created := map[string]int64{}
	now := time.Now().UnixMilli()
	c.fsm.Read(func(s *state.Snapshot) {
		res = scheduler.Schedule(s, scheduler.Options{Now: now})
		for _, a := range res.Assignments {
			created[a.TaskID] = s.Tasks[a.TaskID].CreatedAt
		}
	})
	if len(res.Assignments) == 0 {
		return
	}
	if c.do(ctx, state.Command{Op: state.OpAssignTasks, Assignments: res.Assignments}) != nil {
		return
	}
	done := time.Now().UnixMilli()
	c.mu.Lock()
	for _, a := range res.Assignments {
		ms := float64(done - created[a.TaskID])
		metrics.SchedulingLatency.Observe(ms / 1000)
		c.schedLat = append(c.schedLat, ms)
	}
	if len(c.schedLat) > 500 {
		c.schedLat = c.schedLat[len(c.schedLat)-500:]
	}
	c.mu.Unlock()
}

// ---------------------------------------------------------------- autoscale

// utilization returns average CPU used/requested over a service's running
// replicas, and how many replicas contributed.
func (c *Controller) utilization(s *state.Snapshot, name string) (float64, int) {
	var sum float64
	n := 0
	for _, t := range s.Tasks {
		if t.Service != name || t.State != state.TaskRunning || t.Resources.CPU == 0 {
			continue
		}
		ln := c.live[t.Node]
		if ln == nil {
			continue
		}
		if ts := ln.tasks[t.ID]; ts != nil && ts.State == "running" {
			sum += float64(ts.CpuMillis) / float64(t.Resources.CPU)
			n++
		}
	}
	if n == 0 {
		return 0, 0
	}
	return sum / float64(n), n
}

func (c *Controller) autoscale(ctx context.Context) {
	type decision struct {
		name     string
		from, to int
		reason   string
		dir      string
	}
	var decisions []decision
	now := time.Now()
	c.mu.Lock()
	c.fsm.Read(func(s *state.Snapshot) {
		for name, svc := range s.Services {
			as := svc.Spec.Autoscale
			if as == nil || as.TargetCPU <= 0 {
				continue
			}
			util, n := c.utilization(s, name)
			if n == 0 {
				continue
			}
			cur := svc.Spec.Replicas
			ratio := util * 100 / float64(as.TargetCPU)
			desired := cur
			// 10% tolerance band, like the HPA default.
			if math.Abs(ratio-1) > 0.1 {
				desired = int(math.Ceil(float64(n) * ratio))
			}
			desired = min(max(desired, as.Min, 1), max(as.Max, as.Min))

			recs := append(c.recs[name], recommendation{now, desired})
			cut := 0
			for cut < len(recs) && now.Sub(recs[cut].at) > c.cfg.ScaleDownWindow {
				cut++
			}
			recs = recs[cut:]
			c.recs[name] = recs

			reason := fmt.Sprintf("cpu %.0f%% vs target %d%%", util*100, as.TargetCPU)
			switch {
			case desired > cur && now.Sub(c.lastScale[name]) >= c.cfg.ScaleUpCooldown:
				decisions = append(decisions, decision{name, cur, desired, reason, "up"})
			case desired < cur:
				best := 0
				for _, r := range recs {
					best = max(best, r.desired)
				}
				if best < cur && now.Sub(c.lastScale[name]) >= c.cfg.ScaleDownWindow {
					decisions = append(decisions, decision{name, cur, best, reason, "down"})
				}
			}
		}
	})
	c.mu.Unlock()

	for _, d := range decisions {
		to := d.to
		if c.do(ctx, state.Command{Op: state.OpUpdateService, Name: d.name, Reason: d.reason, Patch: &state.ServicePatch{Replicas: &to}}) == nil {
			c.mu.Lock()
			c.lastScale[d.name] = time.Now()
			c.mu.Unlock()
			metrics.Autoscales.WithLabelValues(d.name, d.dir).Inc()
			c.log.Info("autoscaled", "service", d.name, "from", d.from, "to", d.to, "reason", d.reason)
		}
	}
}

// ---------------------------------------------------------------- views

func (c *Controller) updateGauges() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fsm.Read(func(s *state.Snapshot) {
		nodes := map[state.NodeStatus]float64{state.NodeReady: 0, state.NodeDown: 0, state.NodeDraining: 0}
		for id, n := range s.Nodes {
			nodes[n.Status]++
			if ln := c.live[id]; ln != nil {
				metrics.NodeCPU.WithLabelValues(id).Set(float64(ln.usage.CPU))
				metrics.NodeMemory.WithLabelValues(id).Set(float64(ln.usage.Memory))
			}
		}
		for st, v := range nodes {
			metrics.Nodes.WithLabelValues(string(st)).Set(v)
		}
		tasks := map[state.TaskState]float64{state.TaskPending: 0, state.TaskAssigned: 0, state.TaskRunning: 0, state.TaskFailed: 0, state.TaskLost: 0}
		running := map[string]float64{}
		for _, t := range s.Tasks {
			tasks[t.State]++
			if t.State == state.TaskRunning {
				running[t.Service]++
			}
		}
		for st, v := range tasks {
			metrics.Tasks.WithLabelValues(string(st)).Set(v)
		}
		for name, svc := range s.Services {
			metrics.ServiceReplicas.WithLabelValues(name, "desired").Set(float64(svc.Spec.Replicas))
			metrics.ServiceReplicas.WithLabelValues(name, "running").Set(running[name])
			util, _ := c.utilization(s, name)
			metrics.ServiceCPUUtil.WithLabelValues(name).Set(util)
		}
	})
}

type LiveNode struct {
	LastSeenMs int64           `json:"last_seen_ms"`
	Usage      state.Resources `json:"usage"`
}

type LiveTask struct {
	CPU      int64  `json:"cpu_millis"`
	Memory   int64  `json:"memory_mb"`
	State    string `json:"state"`
	Restarts int32  `json:"restarts"`
}

type Live struct {
	Nodes        map[string]LiveNode `json:"nodes"`
	Tasks        map[string]LiveTask `json:"tasks"`
	ServiceUtil  map[string]float64  `json:"service_cpu_util"`
	SchedP50Ms   float64             `json:"sched_p50_ms"`
	SchedP99Ms   float64             `json:"sched_p99_ms"`
	Recoveries   []Recovery          `json:"recoveries"`
	IncidentOpen bool                `json:"incident_open"`
}

// Live returns the leader's soft state for the API.
func (c *Controller) Live() *Live {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.running {
		return nil
	}
	now := time.Now()
	out := &Live{
		Nodes: map[string]LiveNode{}, Tasks: map[string]LiveTask{}, ServiceUtil: map[string]float64{},
		Recoveries: append([]Recovery(nil), c.recoveries...), IncidentOpen: c.incident != nil,
	}
	for id, ln := range c.live {
		out.Nodes[id] = LiveNode{LastSeenMs: now.Sub(ln.lastSeen).Milliseconds(), Usage: ln.usage}
		for tid, ts := range ln.tasks {
			out.Tasks[tid] = LiveTask{CPU: ts.CpuMillis, Memory: ts.MemoryMb, State: ts.State, Restarts: ts.Restarts}
		}
	}
	c.fsm.Read(func(s *state.Snapshot) {
		for name := range s.Services {
			out.ServiceUtil[name], _ = c.utilization(s, name)
		}
	})
	if n := len(c.schedLat); n > 0 {
		sorted := append([]float64(nil), c.schedLat...)
		sort.Float64s(sorted)
		out.SchedP50Ms = sorted[n/2]
		out.SchedP99Ms = sorted[min(n-1, int(float64(n)*0.99))]
	}
	return out
}
