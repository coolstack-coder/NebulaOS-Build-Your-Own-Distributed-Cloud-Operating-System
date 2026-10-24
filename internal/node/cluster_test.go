package node

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/coolstack-coder/nebulaos/internal/state"
)

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// startCluster boots n full nodes in-process, talking real gRPC on loopback.
func startCluster(t *testing.T, n int) map[string]*Node {
	t.Helper()
	peers := map[string]string{}
	apis := map[string]string{}
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("n%d", i)
		peers[id] = freePort(t)
		apis[id] = freePort(t)
	}
	nodes := map[string]*Node{}
	for id := range peers {
		nd, err := New(Config{
			ID: id, DataDir: t.TempDir(), GRPCAddr: peers[id], APIAddr: apis[id], Peers: peers,
			Capacity: state.Resources{CPU: 2000, Memory: 4096}, Runtime: "sim",
			HeartbeatInterval: 100 * time.Millisecond, NodeTimeout: 800 * time.Millisecond,
			ElectionTimeout: 200 * time.Millisecond, Fsync: false,
			Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := nd.Start(); err != nil {
			t.Fatal(err)
		}
		nodes[id] = nd
	}
	t.Cleanup(func() {
		for _, nd := range nodes {
			nd.Stop()
		}
	})
	return nodes
}

func leaderOf(t *testing.T, nodes map[string]*Node) *Node {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, nd := range nodes {
			if nd.raft.IsLeader() {
				return nd
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader")
	return nil
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func running(o *Overview, svc string) int {
	for _, s := range o.Services {
		if s.Spec.Name == svc {
			return s.Running
		}
	}
	return -1
}

func TestClusterSchedulesAndSelfHeals(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	nodes := startCluster(t, 3)
	leader := leaderOf(t, nodes)
	waitFor(t, "all nodes registered", 5*time.Second, func() bool {
		return len(leader.Overview().Nodes) == 3
	})

	// Submit through a follower to exercise forwarding.
	var follower *Node
	for _, nd := range nodes {
		if nd != leader {
			follower = nd
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	spec := state.ServiceSpec{Name: "web", Image: "nginx", Replicas: 6, Resources: state.Resources{CPU: 200, Memory: 128}}
	if err := follower.Submit(ctx, state.Command{Op: state.OpCreateService, Spec: &spec}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "6 replicas running", 10*time.Second, func() bool { return running(leader.Overview(), "web") == 6 })

	// Every node should have received a share.
	perNode := map[string]int{}
	for _, tk := range leader.Overview().Tasks {
		perNode[tk.Node]++
	}
	if len(perNode) != 3 {
		t.Fatalf("replicas not spread across nodes: %v", perNode)
	}

	// Crash a follower that is running work.
	victim := follower
	victim.Stop()
	delete(nodes, victim.cfg.ID)

	waitFor(t, "victim declared down", 10*time.Second, func() bool {
		for _, n := range leader.Overview().Nodes {
			if n.ID == victim.cfg.ID {
				return n.Status == state.NodeDown
			}
		}
		return false
	})
	waitFor(t, "replicas rescheduled", 10*time.Second, func() bool {
		o := leader.Overview()
		if running(o, "web") != 6 {
			return false
		}
		for _, tk := range o.Tasks {
			if tk.Node == victim.cfg.ID && tk.State.Active() {
				return false
			}
		}
		return true
	})
	found := false
	for _, e := range leader.Overview().Events {
		if e.Kind == "recovered" {
			found = true
		}
	}
	if !found {
		t.Fatal("no recovery event recorded")
	}
}

func TestLeaderFailoverKeepsState(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	nodes := startCluster(t, 3)
	leader := leaderOf(t, nodes)
	waitFor(t, "all nodes registered", 5*time.Second, func() bool { return len(leader.Overview().Nodes) == 3 })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	spec := state.ServiceSpec{Name: "api", Image: "x", Replicas: 3}
	if err := Validate(&spec); err != nil {
		t.Fatal(err)
	}
	if err := leader.Submit(ctx, state.Command{Op: state.OpCreateService, Spec: &spec}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "running", 10*time.Second, func() bool { return running(leader.Overview(), "api") == 3 })

	old := leader.cfg.ID
	leader.Stop()
	delete(nodes, old)
	nl := leaderOf(t, nodes)
	if nl.cfg.ID == old {
		t.Fatal("dead node still leader")
	}
	waitFor(t, "new leader restores replicas", 15*time.Second, func() bool {
		o := nl.Overview()
		if running(o, "api") != 3 {
			return false
		}
		for _, tk := range o.Tasks {
			if tk.Node == old && tk.State.Active() {
				return false
			}
		}
		return true
	})
}
