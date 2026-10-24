package controller

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/coolstack-coder/nebulaos/gen/nebula/v1"
	"github.com/coolstack-coder/nebulaos/internal/state"
)

// harness applies commands straight to an FSM, standing in for Raft.
func harness(t *testing.T, cfg Config) (*Controller, *state.FSM) {
	fsm := state.NewFSM()
	var idx atomic.Uint64
	submit := func(_ context.Context, c state.Command) error {
		if res := fsm.Apply(idx.Add(1), c.Encode()); res != nil {
			if err, ok := res.(error); ok {
				return err
			}
		}
		return nil
	}
	cfg.NodeID = "n1"
	cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(cfg, fsm, submit), fsm
}

func seed(t *testing.T, c *Controller, replicas int) {
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		id := fmt.Sprintf("n%d", i)
		_ = c.do(ctx, state.Command{Op: state.OpRegisterNode, Node: &state.Node{ID: id, Capacity: state.Resources{CPU: 8000, Memory: 16384}}})
	}
	spec := state.ServiceSpec{Name: "web", Image: "nginx", Replicas: replicas, Resources: state.Resources{CPU: 100, Memory: 64},
		Autoscale: &state.Autoscale{Min: 2, Max: 10, TargetCPU: 50}}
	if err := c.do(ctx, state.Command{Op: state.OpCreateService, Spec: &spec}); err != nil {
		t.Fatal(err)
	}
	c.reconcile(ctx)
	c.schedule(ctx)
}

// report sends a heartbeat for every node claiming each task runs at cpu.
func report(c *Controller, fsm *state.FSM, cpu int64) {
	v := fsm.View()
	for id := range v.Nodes {
		req := &pb.HeartbeatRequest{NodeId: id}
		for _, tk := range v.TasksOn(id) {
			req.Tasks = append(req.Tasks, &pb.TaskStatus{TaskId: tk.ID, State: "running", CpuMillis: cpu})
		}
		c.Heartbeat(req)
	}
	c.flushUpdates(context.Background())
}

func replicas(fsm *state.FSM) int { return fsm.View().Services["web"].Spec.Replicas }

func TestReconcileAndScheduleBringUpReplicas(t *testing.T) {
	c, fsm := harness(t, Config{})
	seed(t, c, 4)
	report(c, fsm, 10)
	running := 0
	for _, tk := range fsm.View().Tasks {
		if tk.State == state.TaskRunning {
			running++
		}
	}
	if running != 4 {
		t.Fatalf("want 4 running replicas, got %d", running)
	}
}

func TestAutoscalerScalesOutAndStabilisesScaleIn(t *testing.T) {
	c, fsm := harness(t, Config{ScaleDownWindow: 300 * time.Millisecond, ScaleUpCooldown: time.Millisecond})
	seed(t, c, 2)
	ctx := context.Background()

	// 2 replicas at 150% of request vs a 50% target → 6 replicas.
	report(c, fsm, 150)
	c.autoscale(ctx)
	if got := replicas(fsm); got != 6 {
		t.Fatalf("scale out: want 6 replicas, got %d", got)
	}

	// Load vanishes. The scale-down window must hold the line at first.
	c.reconcile(ctx)
	c.schedule(ctx)
	report(c, fsm, 5)
	c.autoscale(ctx)
	if got := replicas(fsm); got != 6 {
		t.Fatalf("scaled in before the stabilisation window: %d", got)
	}
	time.Sleep(350 * time.Millisecond)
	report(c, fsm, 5)
	c.autoscale(ctx)
	if got := replicas(fsm); got != 2 {
		t.Fatalf("after the window: want min replicas 2, got %d", got)
	}
}

func TestFailureDetectorMarksSilentNodesDown(t *testing.T) {
	c, fsm := harness(t, Config{NodeTimeout: 50 * time.Millisecond})
	seed(t, c, 3)
	c.Start(1)
	defer c.Stop()
	// Only n1 and n2 keep heartbeating.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c.Heartbeat(&pb.HeartbeatRequest{NodeId: "n1"})
		c.Heartbeat(&pb.HeartbeatRequest{NodeId: "n2"})
		if fsm.View().Nodes["n3"].Status == state.NodeDown {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	v := fsm.View()
	if v.Nodes["n3"].Status != state.NodeDown {
		t.Fatal("silent node was never declared down")
	}
	if v.Nodes["n1"].Status != state.NodeReady || v.Nodes["n2"].Status != state.NodeReady {
		t.Fatal("healthy node declared down")
	}
}

func TestNewLeaderDoesNotScaleDownImmediately(t *testing.T) {
	c, fsm := harness(t, Config{ScaleDownWindow: time.Hour, AutoscaleInterval: time.Hour})
	seed(t, c, 6)
	c.Start(2) // take over leadership with no autoscaling history
	defer c.Stop()
	report(c, fsm, 5) // load is low: the HPA formula alone would say 2
	c.autoscale(context.Background())
	if got := replicas(fsm); got != 6 {
		t.Fatalf("new leader scaled down to %d before its stabilisation window", got)
	}
}
