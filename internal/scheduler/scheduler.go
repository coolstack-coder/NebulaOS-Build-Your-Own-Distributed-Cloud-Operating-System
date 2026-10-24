// Package scheduler decides which node runs each pending task.
//
// It follows the same two-phase shape as kube-scheduler: a set of filters
// removes nodes that cannot run the task at all, then weighted scorers rank
// the rest. It is a pure function of a state snapshot, which makes it easy to
// test and lets the leader run it without holding any locks.
package scheduler

import (
	"fmt"
	"hash/fnv"
	"sort"

	"github.com/coolstack-coder/nebulaos/internal/state"
)

type Weights struct {
	LeastAllocated int // prefer nodes with the most free capacity
	Balanced       int // prefer nodes whose CPU and memory usage stay even
	Spread         int // prefer nodes with fewer replicas of the same service
	Stability      int // avoid nodes that only just came back from failure
}

var DefaultWeights = Weights{LeastAllocated: 3, Balanced: 1, Spread: 4, Stability: 2}

type Options struct {
	Weights Weights
	Now     int64 // unix ms; used by the stability scorer
	// StabilityWindowMs is how long a recovered node is treated as suspect.
	StabilityWindowMs int64
}

type Result struct {
	Assignments []state.Assignment
	// Unschedulable maps task id to the reason no node fit.
	Unschedulable map[string]string
}

type nodeInfo struct {
	node  *state.Node
	alloc state.Resources
	// per-service replica count, updated as we place tasks in this batch
	perService map[string]int
}

// Schedule places as many pending tasks as possible. Tasks are handled in
// creation order and each placement is reflected in the bookkeeping before
// the next one, so a single batch never overcommits a node.
func Schedule(snap *state.Snapshot, opts Options) Result {
	if opts.Weights == (Weights{}) {
		opts.Weights = DefaultWeights
	}
	if opts.StabilityWindowMs == 0 {
		opts.StabilityWindowMs = 30_000
	}
	alloc := snap.Allocated()
	infos := map[string]*nodeInfo{}
	for id, n := range snap.Nodes {
		infos[id] = &nodeInfo{node: n, alloc: alloc[id], perService: map[string]int{}}
	}
	for _, t := range snap.Tasks {
		if t.Node != "" && t.State.Active() {
			if ni := infos[t.Node]; ni != nil {
				ni.perService[t.Service]++
			}
		}
	}

	var pending []*state.Task
	for _, t := range snap.Tasks {
		if t.State == state.TaskPending {
			pending = append(pending, t)
		}
	}
	sort.Slice(pending, func(i, j int) bool {
		if pending[i].CreatedAt != pending[j].CreatedAt {
			return pending[i].CreatedAt < pending[j].CreatedAt
		}
		return pending[i].ID < pending[j].ID
	})

	res := Result{Unschedulable: map[string]string{}}
	for _, t := range pending {
		svc := snap.Services[t.Service]
		if svc == nil {
			continue
		}
		best, bestScore, reason := "", -1, ""
		ids := sortedIDs(infos)
		rejected := map[string]int{}
		for _, id := range ids {
			ni := infos[id]
			if why := filter(ni, t, svc); why != "" {
				rejected[why]++
				continue
			}
			score := scoreNode(ni, t, opts)
			// Deterministic tie-break that still spreads ties across nodes
			// instead of always favouring n1.
			if score > bestScore || (score == bestScore && tieBreak(t.ID, id) < tieBreak(t.ID, best)) {
				best, bestScore = id, score
			}
		}
		if best == "" {
			reason = describe(rejected, len(ids))
			res.Unschedulable[t.ID] = reason
			continue
		}
		ni := infos[best]
		ni.alloc = ni.alloc.Add(t.Resources)
		ni.perService[t.Service]++
		res.Assignments = append(res.Assignments, state.Assignment{TaskID: t.ID, NodeID: best, Score: bestScore})
	}
	return res
}

func filter(ni *nodeInfo, t *state.Task, svc *state.Service) string {
	if ni.node.Status != state.NodeReady {
		return "not ready"
	}
	for k, v := range svc.Spec.NodeSelector {
		if ni.node.Labels[k] != v {
			return "selector mismatch"
		}
	}
	if !ni.alloc.Add(t.Resources).Fits(ni.node.Capacity) {
		return "insufficient resources"
	}
	return ""
}

func scoreNode(ni *nodeInfo, t *state.Task, opts Options) int {
	w := opts.Weights
	cap := ni.node.Capacity
	after := ni.alloc.Add(t.Resources)
	cpuFrac := frac(after.CPU, cap.CPU)
	memFrac := frac(after.Memory, cap.Memory)

	least := (1 - (cpuFrac+memFrac)/2) * 100
	balanced := (1 - abs(cpuFrac-memFrac)) * 100
	spread := 100 / float64(1+ni.perService[t.Service])
	stability := 100.0
	if age := opts.Now - ni.node.ChangedAt; ni.node.ChangedAt > 0 && age < opts.StabilityWindowMs {
		stability = 100 * float64(max(age, 0)) / float64(opts.StabilityWindowMs)
	}

	total := w.LeastAllocated + w.Balanced + w.Spread + w.Stability
	sum := float64(w.LeastAllocated)*least + float64(w.Balanced)*balanced +
		float64(w.Spread)*spread + float64(w.Stability)*stability
	return int(sum / float64(total))
}

func describe(rejected map[string]int, nodes int) string {
	if nodes == 0 {
		return "no nodes registered"
	}
	keys := make([]string, 0, len(rejected))
	for k := range rejected {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := fmt.Sprintf("0/%d nodes available:", nodes)
	for _, k := range keys {
		out += fmt.Sprintf(" %d %s;", rejected[k], k)
	}
	return out
}

func frac(used, total int64) float64 {
	if total <= 0 {
		return 1
	}
	return float64(used) / float64(total)
}

func abs(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}

func tieBreak(task, node string) uint32 {
	if node == "" {
		return ^uint32(0)
	}
	h := fnv.New32a()
	h.Write([]byte(task))
	h.Write([]byte{0})
	h.Write([]byte(node))
	return h.Sum32()
}

func sortedIDs(m map[string]*nodeInfo) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
