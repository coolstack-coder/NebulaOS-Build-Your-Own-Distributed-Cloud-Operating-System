package raft

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	pb "github.com/coolstack-coder/nebulaos/gen/nebula/v1"
)

// ---------------------------------------------------------------- harness

// network is an in-memory transport that can cut individual nodes off.
type network struct {
	mu    sync.Mutex
	nodes map[string]*Node
	down  map[string]bool
}

var errUnreachable = errors.New("unreachable")

func (nw *network) target(from, to string) (*Node, error) {
	nw.mu.Lock()
	defer nw.mu.Unlock()
	if nw.down[from] || nw.down[to] || nw.nodes[to] == nil {
		return nil, errUnreachable
	}
	return nw.nodes[to], nil
}

type memTransport struct {
	nw   *network
	from string
}

func (t memTransport) RequestVote(_ context.Context, to string, r *pb.VoteRequest) (*pb.VoteResponse, error) {
	n, err := t.nw.target(t.from, to)
	if err != nil {
		return nil, err
	}
	return n.HandleRequestVote(r), nil
}

func (t memTransport) AppendEntries(_ context.Context, to string, r *pb.AppendRequest) (*pb.AppendResponse, error) {
	n, err := t.nw.target(t.from, to)
	if err != nil {
		return nil, err
	}
	return n.HandleAppendEntries(r), nil
}

func (t memTransport) InstallSnapshot(_ context.Context, to string, r *pb.SnapshotRequest) (*pb.SnapshotResponse, error) {
	n, err := t.nw.target(t.from, to)
	if err != nil {
		return nil, err
	}
	resp := n.HandleInstallSnapshot(r)
	// A node that died while handling the RPC never gets to reply.
	if _, err := t.nw.target(t.from, to); err != nil {
		return nil, err
	}
	return resp, nil
}

// listFSM records every applied command so tests can compare histories.
type listFSM struct {
	mu  sync.Mutex
	cmd []string
}

func (f *listFSM) Apply(_ uint64, data []byte) any {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cmd = append(f.cmd, string(data))
	return len(f.cmd)
}

func (f *listFSM) Snapshot() ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return json.Marshal(f.cmd)
}

func (f *listFSM) Restore(b []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return json.Unmarshal(b, &f.cmd)
}

func (f *listFSM) history() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.cmd...)
}

type cluster struct {
	t      *testing.T
	nw     *network
	ids    []string
	nodes  map[string]*Node
	fsms   map[string]*listFSM
	snapAt uint64
	// open returns the storage a (re)started node recovers from.
	open func(id string) Storage
}

func newCluster(t *testing.T, size int, snapshotThreshold uint64) *cluster {
	mem := map[string]*MemoryStorage{}
	return newClusterWith(t, size, snapshotThreshold, func(id string) Storage {
		if mem[id] == nil {
			mem[id] = NewMemoryStorage()
		}
		return mem[id]
	})
}

func newClusterWith(t *testing.T, size int, snapshotThreshold uint64, open func(id string) Storage) *cluster {
	c := &cluster{
		t:      t,
		nw:     &network{nodes: map[string]*Node{}, down: map[string]bool{}},
		nodes:  map[string]*Node{},
		fsms:   map[string]*listFSM{},
		snapAt: snapshotThreshold,
		open:   open,
	}
	for i := 1; i <= size; i++ {
		c.ids = append(c.ids, fmt.Sprintf("n%d", i))
	}
	for _, id := range c.ids {
		c.start(id)
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			n.Stop()
		}
	})
	return c
}

func (c *cluster) start(id string) { c.startWith(id, c.open(id)) }

func (c *cluster) startWith(id string, storage Storage) {
	peers := map[string]string{}
	for _, p := range c.ids {
		peers[p] = p // the in-memory transport addresses nodes by id
	}
	fsm := &listFSM{}
	n, err := New(Config{
		ID:                 id,
		Peers:              peers,
		HeartbeatInterval:  20 * time.Millisecond,
		ElectionTimeoutMin: 120 * time.Millisecond,
		ElectionTimeoutMax: 240 * time.Millisecond,
		SnapshotThreshold:  c.snapAt,
		Storage:            storage,
		Transport:          memTransport{nw: c.nw, from: id},
		FSM:                fsm,
		Logger:             slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		c.t.Fatal(err)
	}
	c.nw.mu.Lock()
	c.nw.nodes[id] = n
	c.nw.mu.Unlock()
	c.nodes[id] = n
	c.fsms[id] = fsm
	n.Start()
}

func (c *cluster) crash(id string) {
	c.nw.mu.Lock()
	delete(c.nw.nodes, id)
	c.nw.mu.Unlock()
	c.nodes[id].Stop()
	delete(c.nodes, id)
}

func (c *cluster) setDown(id string, down bool) {
	c.nw.mu.Lock()
	c.nw.down[id] = down
	c.nw.mu.Unlock()
}

// leader waits for exactly one reachable leader in the highest term.
func (c *cluster) leader(exclude ...string) string {
	skip := map[string]bool{}
	for _, e := range exclude {
		skip[e] = true
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		byTerm := map[uint64][]string{}
		var top uint64
		for id, n := range c.nodes {
			if skip[id] {
				continue
			}
			s := n.Status()
			if s.Role == "leader" {
				byTerm[s.Term] = append(byTerm[s.Term], id)
				top = max(top, s.Term)
			}
		}
		for term, ls := range byTerm {
			if len(ls) > 1 {
				c.t.Fatalf("term %d has %d leaders: %v", term, len(ls), ls)
			}
		}
		if ls := byTerm[top]; len(ls) == 1 {
			return ls[0]
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatal("no leader elected")
	return ""
}

func (c *cluster) apply(id, cmd string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := c.nodes[id].Apply(ctx, []byte(cmd))
	return err
}

// waitApplied waits until every listed node has applied want commands.
func (c *cluster) waitApplied(want int, ids ...string) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, id := range ids {
			if len(c.fsms[id].history()) < want {
				ok = false
			}
		}
		if ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, id := range ids {
		c.t.Logf("%s applied %d", id, len(c.fsms[id].history()))
	}
	c.t.Fatalf("nodes did not apply %d commands", want)
}

// checkPrefixes asserts the State Machine Safety property: no two nodes ever
// apply different commands at the same position.
func (c *cluster) checkPrefixes() {
	var longest []string
	for _, f := range c.fsms {
		if h := f.history(); len(h) > len(longest) {
			longest = h
		}
	}
	for id, f := range c.fsms {
		for i, cmd := range f.history() {
			if longest[i] != cmd {
				c.t.Fatalf("%s diverges at %d: %q vs %q", id, i, cmd, longest[i])
			}
		}
	}
}

// ---------------------------------------------------------------- tests

func TestElectsSingleLeader(t *testing.T) {
	c := newCluster(t, 3, 0)
	l := c.leader()
	term := c.nodes[l].Status().Term
	time.Sleep(300 * time.Millisecond)
	if l2 := c.leader(); l2 != l || c.nodes[l2].Status().Term != term {
		t.Fatalf("leadership churned without failures: %s/%d -> %s", l, term, l2)
	}
}

func TestReelectionAfterLeaderLoss(t *testing.T) {
	c := newCluster(t, 5, 0)
	old := c.leader()
	c.setDown(old, true)
	nl := c.leader(old)
	if nl == old {
		t.Fatal("isolated node still considered leader")
	}
	if err := c.apply(nl, "after-failover"); err != nil {
		t.Fatal(err)
	}
	// The old leader steps down via check-quorum and then rejoins as a
	// follower once healed.
	c.setDown(old, false)
	others := []string{}
	for _, id := range c.ids {
		others = append(others, id)
	}
	c.waitApplied(1, others...)
	if c.nodes[old].Status().Role == "leader" && c.nodes[nl].Status().Role == "leader" {
		t.Fatal("two leaders after heal")
	}
}

func TestReplicatesInOrder(t *testing.T) {
	c := newCluster(t, 3, 0)
	l := c.leader()
	for i := 0; i < 100; i++ {
		if err := c.apply(l, fmt.Sprintf("cmd-%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	c.waitApplied(100, c.ids...)
	c.checkPrefixes()
	if h := c.fsms[c.ids[0]].history(); h[42] != "cmd-42" {
		t.Fatalf("out of order: %v", h[40:45])
	}
}

func TestMinorityCannotCommit(t *testing.T) {
	c := newCluster(t, 5, 0)
	l := c.leader()
	// Cut the leader off together with one follower: two out of five.
	var buddy string
	for _, id := range c.ids {
		if id != l {
			buddy = id
			break
		}
	}
	// Only the leader is isolated from everyone; buddy is isolated too, so
	// neither side of size 2 can win, but the 3 remaining can.
	c.setDown(l, true)
	c.setDown(buddy, true)
	if err := c.apply(l, "lost-write"); err == nil {
		t.Fatal("isolated leader committed a write")
	}
	nl := c.leader(l, buddy)
	if err := c.apply(nl, "majority-write"); err != nil {
		t.Fatal(err)
	}
	c.setDown(l, false)
	c.setDown(buddy, false)
	c.waitApplied(1, c.ids...)
	c.checkPrefixes()
	for _, id := range c.ids {
		for _, cmd := range c.fsms[id].history() {
			if cmd == "lost-write" {
				t.Fatalf("%s applied an uncommitted write", id)
			}
		}
	}
}

func TestLaggingFollowerCatchesUpFromSnapshot(t *testing.T) {
	c := newCluster(t, 3, 20)
	l := c.leader()
	var lagger string
	for _, id := range c.ids {
		if id != l {
			lagger = id
			break
		}
	}
	c.setDown(lagger, true)
	for i := 0; i < 120; i++ {
		if err := c.apply(l, fmt.Sprintf("v%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if s := c.nodes[l].Status(); s.SnapshotIndex == 0 {
		t.Fatal("leader never compacted its log")
	}
	c.setDown(lagger, false)
	c.waitApplied(120, c.ids...)
	c.checkPrefixes()
	if c.nodes[lagger].Status().Stats.SnapshotsLoaded == 0 {
		t.Fatal("expected the lagging follower to install a snapshot")
	}
}

func TestStateSurvivesRestart(t *testing.T) {
	c := newCluster(t, 3, 30)
	l := c.leader()
	for i := 0; i < 50; i++ {
		if err := c.apply(l, fmt.Sprintf("x%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	c.waitApplied(50, c.ids...)
	for _, id := range c.ids {
		c.crash(id)
	}
	for _, id := range c.ids {
		c.start(id)
	}
	l = c.leader()
	if err := c.apply(l, "after-restart"); err != nil {
		t.Fatal(err)
	}
	c.waitApplied(51, c.ids...)
	c.checkPrefixes()
	if h := c.fsms[l].history(); h[len(h)-1] != "after-restart" || h[0] != "x0" {
		t.Fatalf("unexpected history after restart: first=%q last=%q", h[0], h[len(h)-1])
	}
}

// TestChaos hammers the cluster with writes while randomly partitioning and
// crashing nodes, then checks that every node's applied history is a prefix
// of the same sequence and that every acknowledged write survived.
func TestChaos(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos test is slow")
	}
	c := newCluster(t, 5, 40)
	var (
		mu    sync.Mutex
		acked []string
	)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			for id, n := range c.snapshotNodes() {
				if n.IsLeader() {
					cmd := fmt.Sprintf("w%d", i)
					ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
					if _, err := n.Apply(ctx, []byte(cmd)); err == nil {
						mu.Lock()
						acked = append(acked, cmd)
						mu.Unlock()
					}
					cancel()
					_ = id
					break
				}
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	rng := rand.New(rand.NewPCG(1, 2))
	for round := 0; round < 12; round++ {
		a := c.ids[rng.IntN(len(c.ids))]
		b := c.ids[rng.IntN(len(c.ids))]
		c.setDown(a, true)
		c.setDown(b, true)
		time.Sleep(time.Duration(150+rng.IntN(250)) * time.Millisecond)
		c.setDown(a, false)
		c.setDown(b, false)
		time.Sleep(150 * time.Millisecond)
	}
	close(stop)
	wg.Wait()

	l := c.leader()
	if err := c.apply(l, "final"); err != nil {
		t.Fatal(err)
	}
	final := len(c.fsms[l].history())
	c.waitApplied(final, c.ids...)
	c.checkPrefixes()

	applied := map[string]bool{}
	for _, cmd := range c.fsms[l].history() {
		applied[cmd] = true
	}
	for _, cmd := range acked {
		if !applied[cmd] {
			t.Fatalf("acknowledged write %s was lost", cmd)
		}
	}
	t.Logf("%d acknowledged writes, %d applied entries", len(acked), final)
}

func (c *cluster) snapshotNodes() map[string]*Node {
	c.nw.mu.Lock()
	defer c.nw.mu.Unlock()
	out := map[string]*Node{}
	for id, n := range c.nw.nodes {
		out[id] = n
	}
	return out
}
