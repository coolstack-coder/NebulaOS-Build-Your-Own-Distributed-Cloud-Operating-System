package state

import (
	"testing"
)

func apply(t *testing.T, f *FSM, idx uint64, c Command) {
	t.Helper()
	if res := f.Apply(idx, c.Encode()); res != nil {
		if err, ok := res.(error); ok && err != nil {
			t.Fatalf("apply %s: %v", c.Op, err)
		}
	}
}

func TestNodeDownMarksTasksLost(t *testing.T) {
	f := NewFSM()
	apply(t, f, 1, Command{Op: OpRegisterNode, Time: 1, Node: &Node{ID: "n1", Capacity: Resources{1000, 1000}}})
	apply(t, f, 2, Command{Op: OpRegisterNode, Time: 1, Node: &Node{ID: "n2", Capacity: Resources{1000, 1000}}})
	apply(t, f, 3, Command{Op: OpCreateService, Time: 2, Spec: &ServiceSpec{Name: "web", Image: "nginx", Replicas: 2}})
	apply(t, f, 4, Command{Op: OpCreateTasks, Time: 3, Tasks: []Task{{ID: "web-a", Service: "web"}, {ID: "web-b", Service: "web"}}})
	apply(t, f, 5, Command{Op: OpAssignTasks, Time: 4, Assignments: []Assignment{{TaskID: "web-a", NodeID: "n1"}, {TaskID: "web-b", NodeID: "n2"}}})
	apply(t, f, 6, Command{Op: OpUpdateTasks, Time: 5, Updates: []TaskUpdate{{TaskID: "web-a", NodeID: "n1", State: TaskRunning}}})

	apply(t, f, 7, Command{Op: OpSetNodeStatus, Time: 6, NodeID: "n1", Status: NodeDown, Reason: "test"})
	v := f.View()
	if v.Tasks["web-a"].State != TaskLost {
		t.Fatalf("task on dead node is %s, want lost", v.Tasks["web-a"].State)
	}
	if v.Tasks["web-b"].State != TaskAssigned {
		t.Fatalf("task on healthy node changed to %s", v.Tasks["web-b"].State)
	}
	// A late report from the dead node must not resurrect its task.
	apply(t, f, 8, Command{Op: OpUpdateTasks, Time: 7, Updates: []TaskUpdate{{TaskID: "web-a", NodeID: "n1", State: TaskRunning}}})
	if f.View().Tasks["web-a"].State != TaskLost {
		t.Fatal("stale report resurrected a lost task")
	}
	// Nothing can be scheduled onto a down node.
	apply(t, f, 9, Command{Op: OpCreateTasks, Time: 8, Tasks: []Task{{ID: "web-c", Service: "web"}}})
	apply(t, f, 10, Command{Op: OpAssignTasks, Time: 9, Assignments: []Assignment{{TaskID: "web-c", NodeID: "n1"}}})
	if f.View().Tasks["web-c"].State != TaskPending {
		t.Fatal("task assigned to a down node")
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	f := NewFSM()
	apply(t, f, 1, Command{Op: OpRegisterNode, Time: 1, Node: &Node{ID: "n1"}})
	apply(t, f, 2, Command{Op: OpCreateService, Time: 2, Spec: &ServiceSpec{Name: "api", Image: "x", Replicas: 3,
		Autoscale: &Autoscale{Min: 1, Max: 5, TargetCPU: 50}}})
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	g := NewFSM()
	if err := g.Restore(snap); err != nil {
		t.Fatal(err)
	}
	v := g.View()
	if v.Services["api"].Spec.Autoscale.Max != 5 || v.Nodes["n1"] == nil || v.Applied != 2 || len(v.Events) != 2 {
		t.Fatalf("snapshot lost data: %+v", v)
	}
}

func TestDeterministicApply(t *testing.T) {
	cmds := []Command{
		{Op: OpRegisterNode, Time: 1, Node: &Node{ID: "n1"}},
		{Op: OpCreateService, Time: 2, Spec: &ServiceSpec{Name: "s", Image: "x", Replicas: 1}},
		{Op: OpCreateTasks, Time: 3, Tasks: []Task{{ID: "s-1", Service: "s"}}},
		{Op: OpAssignTasks, Time: 4, Assignments: []Assignment{{TaskID: "s-1", NodeID: "n1", Score: 90}}},
		{Op: OpUpdateService, Time: 5, Name: "s", Patch: &ServicePatch{Replicas: ptr(4)}, Reason: "cpu 99%"},
	}
	a, b := NewFSM(), NewFSM()
	for i, c := range cmds {
		apply(t, a, uint64(i+1), c)
		apply(t, b, uint64(i+1), c)
	}
	sa, _ := a.Snapshot()
	sb, _ := b.Snapshot()
	if string(sa) != string(sb) {
		t.Fatal("two replicas applying the same log diverged")
	}
}

func ptr[T any](v T) *T { return &v }
