package scheduler

import (
	"fmt"
	"strings"
	"testing"

	"github.com/coolstack-coder/nebulaos/internal/state"
)

func snapshot(nodes int, cpu, mem int64) *state.Snapshot {
	s := &state.Snapshot{Nodes: map[string]*state.Node{}, Services: map[string]*state.Service{}, Tasks: map[string]*state.Task{}}
	for i := 1; i <= nodes; i++ {
		id := fmt.Sprintf("n%d", i)
		s.Nodes[id] = &state.Node{ID: id, Status: state.NodeReady, Capacity: state.Resources{CPU: cpu, Memory: mem}}
	}
	return s
}

func addService(s *state.Snapshot, name string, replicas int, req state.Resources) {
	s.Services[name] = &state.Service{Spec: state.ServiceSpec{Name: name, Replicas: replicas, Resources: req}}
	for i := 0; i < replicas; i++ {
		id := fmt.Sprintf("%s-%d", name, i)
		s.Tasks[id] = &state.Task{ID: id, Service: name, State: state.TaskPending, Resources: req, CreatedAt: int64(i)}
	}
}

func TestSpreadsReplicasAcrossNodes(t *testing.T) {
	s := snapshot(5, 4000, 8192)
	addService(s, "web", 5, state.Resources{CPU: 250, Memory: 256})
	res := Schedule(s, Options{Now: 1e12})
	if len(res.Assignments) != 5 {
		t.Fatalf("placed %d/5", len(res.Assignments))
	}
	seen := map[string]int{}
	for _, a := range res.Assignments {
		seen[a.NodeID]++
	}
	if len(seen) != 5 {
		t.Fatalf("replicas not spread: %v", seen)
	}
}

func TestNeverOvercommits(t *testing.T) {
	s := snapshot(2, 1000, 1024)
	addService(s, "big", 5, state.Resources{CPU: 400, Memory: 100})
	res := Schedule(s, Options{Now: 1e12})
	if len(res.Assignments) != 4 {
		t.Fatalf("expected 4 placements (2 per node), got %d", len(res.Assignments))
	}
	if len(res.Unschedulable) != 1 {
		t.Fatalf("expected one unschedulable task, got %v", res.Unschedulable)
	}
	for _, why := range res.Unschedulable {
		if !strings.Contains(why, "insufficient resources") {
			t.Fatalf("unexpected reason %q", why)
		}
	}
}

func TestSkipsDownAndSelectorMismatch(t *testing.T) {
	s := snapshot(3, 4000, 8192)
	s.Nodes["n1"].Status = state.NodeDown
	s.Nodes["n2"].Labels = map[string]string{"zone": "a"}
	s.Nodes["n3"].Labels = map[string]string{"zone": "b"}
	addService(s, "db", 2, state.Resources{CPU: 100, Memory: 100})
	s.Services["db"].Spec.NodeSelector = map[string]string{"zone": "b"}
	res := Schedule(s, Options{Now: 1e12})
	for _, a := range res.Assignments {
		if a.NodeID != "n3" {
			t.Fatalf("placed on %s, want n3", a.NodeID)
		}
	}
	if len(res.Assignments) != 2 {
		t.Fatalf("placed %d/2", len(res.Assignments))
	}
}

func TestPrefersEmptierNode(t *testing.T) {
	s := snapshot(2, 4000, 8192)
	s.Services["filler"] = &state.Service{Spec: state.ServiceSpec{Name: "filler"}}
	s.Tasks["f"] = &state.Task{ID: "f", Service: "filler", Node: "n1", State: state.TaskRunning, Resources: state.Resources{CPU: 3000, Memory: 6000}}
	addService(s, "api", 1, state.Resources{CPU: 200, Memory: 200})
	res := Schedule(s, Options{Now: 1e12})
	if res.Assignments[0].NodeID != "n2" {
		t.Fatalf("expected the idle node, got %s", res.Assignments[0].NodeID)
	}
}

func TestAvoidsFreshlyRecoveredNode(t *testing.T) {
	s := snapshot(2, 4000, 8192)
	now := int64(100_000)
	s.Nodes["n1"].ChangedAt = now - 1_000 // flapped a second ago
	s.Nodes["n2"].ChangedAt = now - 60_000
	addService(s, "api", 1, state.Resources{CPU: 200, Memory: 200})
	res := Schedule(s, Options{Now: now})
	if res.Assignments[0].NodeID != "n2" {
		t.Fatalf("expected the stable node, got %s", res.Assignments[0].NodeID)
	}
}

func BenchmarkSchedule(b *testing.B) {
	s := snapshot(50, 16000, 65536)
	addService(s, "web", 500, state.Resources{CPU: 100, Memory: 128})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Schedule(s, Options{Now: 1e12})
	}
}
