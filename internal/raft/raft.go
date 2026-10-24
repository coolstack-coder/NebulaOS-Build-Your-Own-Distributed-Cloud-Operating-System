// Package raft is a from-scratch implementation of the Raft consensus
// algorithm used by NebulaOS to replicate cluster state.
//
// Implemented: leader election with randomized timeouts, pre-vote, log
// replication with fast conflict backtracking, the "only commit entries from
// the current term" rule (plus a no-op on election), durable hard state and
// log, snapshots with InstallSnapshot, and check-quorum so a partitioned
// leader steps down instead of serving stale state.
//
// Not implemented: dynamic membership changes. The voter set is static and
// comes from configuration; worker-only nodes join the cluster outside of
// Raft through the heartbeat API.
package raft

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	pb "github.com/coolstack-coder/nebulaos/gen/nebula/v1"
)

var (
	ErrNotLeader      = errors.New("raft: not the leader")
	ErrLeadershipLost = errors.New("raft: leadership lost before entry committed")
	ErrStopped        = errors.New("raft: node stopped")
	ErrTimeout        = errors.New("raft: timed out waiting for commit")
)

type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Leader:
		return "leader"
	case Candidate:
		return "candidate"
	default:
		return "follower"
	}
}

// FSM is the replicated state machine. Apply is called exactly once per
// committed command entry, in log order, from a single goroutine.
type FSM interface {
	Apply(index uint64, data []byte) any
	Snapshot() ([]byte, error)
	Restore(data []byte) error
}

// Transport delivers RPCs to peers addressed by their configured address.
type Transport interface {
	RequestVote(ctx context.Context, addr string, req *pb.VoteRequest) (*pb.VoteResponse, error)
	AppendEntries(ctx context.Context, addr string, req *pb.AppendRequest) (*pb.AppendResponse, error)
	InstallSnapshot(ctx context.Context, addr string, req *pb.SnapshotRequest) (*pb.SnapshotResponse, error)
}

type Config struct {
	ID    string
	Peers map[string]string // voter id -> address, including this node

	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	RPCTimeout         time.Duration
	MaxBatch           int
	// Take a snapshot once this many entries have been applied since the
	// last one. Zero disables snapshotting.
	SnapshotThreshold uint64

	Storage   Storage
	Transport Transport
	FSM       FSM
	Logger    *slog.Logger

	// OnLeaderChange is invoked (from its own goroutine, in order) whenever
	// this node gains or loses leadership.
	OnLeaderChange func(isLeader bool, term uint64)
}

func (c *Config) defaults() {
	if c.HeartbeatInterval == 0 {
		c.HeartbeatInterval = 75 * time.Millisecond
	}
	if c.ElectionTimeoutMin == 0 {
		c.ElectionTimeoutMin = 450 * time.Millisecond
	}
	if c.ElectionTimeoutMax == 0 {
		c.ElectionTimeoutMax = 2 * c.ElectionTimeoutMin
	}
	if c.RPCTimeout == 0 {
		c.RPCTimeout = 400 * time.Millisecond
	}
	if c.MaxBatch == 0 {
		c.MaxBatch = 256
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

type future struct {
	term uint64
	ch   chan result
}

type result struct {
	val any
	err error
}

type Node struct {
	cfg Config
	log *slog.Logger

	mu          sync.Mutex
	role        Role
	term        uint64
	votedFor    string
	leaderID    string
	entries     []*pb.LogEntry // entries[0] is a sentinel at snapIndex/snapTerm
	commitIndex uint64
	lastApplied uint64

	nextIndex   map[string]uint64
	matchIndex  map[string]uint64
	lastContact map[string]time.Time
	notify      map[string]chan struct{}

	electionDeadline time.Time
	lastLeaderSeen   time.Time
	leaderSince      time.Time

	// durable is the highest log index the leader knows is on its own disk.
	// With group commit it trails lastIndex by at most one fsync.
	durable   uint64
	persistCh chan struct{}

	pendingSnap  []byte
	futures      map[uint64]*future
	applyCond    *sync.Cond
	stopped      bool
	stopCh       chan struct{}
	wg           sync.WaitGroup
	leaderEvents chan leaderEvent
	stats        Stats
	snapshotting bool
}

type leaderEvent struct {
	leader bool
	term   uint64
}

// Stats are cumulative counters exposed through Status for metrics.
type Stats struct {
	ElectionsStarted uint64 `json:"elections_started"`
	LeaderChanges    uint64 `json:"leader_changes"`
	SnapshotsTaken   uint64 `json:"snapshots_taken"`
	SnapshotsLoaded  uint64 `json:"snapshots_installed"`
}

// New restores persisted state and returns a node ready to Start.
func New(cfg Config) (*Node, error) {
	cfg.defaults()
	n := &Node{
		cfg:          cfg,
		log:          cfg.Logger.With("component", "raft", "id", cfg.ID),
		futures:      map[uint64]*future{},
		stopCh:       make(chan struct{}),
		persistCh:    make(chan struct{}, 1),
		leaderEvents: make(chan leaderEvent, 64),
	}
	n.applyCond = sync.NewCond(&n.mu)

	term, voted, err := cfg.Storage.LoadState()
	if err != nil {
		return nil, err
	}
	n.term, n.votedFor = term, voted

	snapIdx, snapTerm, snap, err := cfg.Storage.LoadSnapshot()
	if err != nil {
		return nil, err
	}
	n.entries = []*pb.LogEntry{{Index: snapIdx, Term: snapTerm}}
	if snap != nil {
		if err := cfg.FSM.Restore(snap); err != nil {
			return nil, err
		}
	}
	n.commitIndex, n.lastApplied = snapIdx, snapIdx

	stored, err := cfg.Storage.LoadLog()
	if err != nil {
		return nil, err
	}
	for _, e := range stored {
		// A crash between saving a snapshot and rewriting the log can leave
		// already-compacted entries behind; skip them.
		if e.Index <= snapIdx {
			continue
		}
		if e.Index != n.lastIndex()+1 {
			n.log.Warn("gap in stored log, discarding tail", "want", n.lastIndex()+1, "got", e.Index)
			break
		}
		n.entries = append(n.entries, e)
	}
	n.log.Info("restored", "term", n.term, "snapshot_index", snapIdx, "last_index", n.lastIndex())
	return n, nil
}

// Start launches the background goroutines.
func (n *Node) Start() {
	n.mu.Lock()
	n.resetElectionTimer()
	n.mu.Unlock()
	n.wg.Add(4)
	go n.persister()
	go n.ticker()
	go n.applier()
	go n.leaderNotifier()
}

// Stop halts the node. Pending Apply calls fail with ErrStopped.
func (n *Node) Stop() {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	close(n.stopCh)
	for idx, f := range n.futures {
		f.ch <- result{err: ErrStopped}
		delete(n.futures, idx)
	}
	n.applyCond.Broadcast()
	n.mu.Unlock()
	n.wg.Wait()
	_ = n.cfg.Storage.Close()
}

// ---------------------------------------------------------------- log helpers

func (n *Node) snapIndex() uint64 { return n.entries[0].Index }
func (n *Node) snapTerm() uint64  { return n.entries[0].Term }
func (n *Node) lastIndex() uint64 { return n.entries[len(n.entries)-1].Index }
func (n *Node) lastTerm() uint64  { return n.entries[len(n.entries)-1].Term }

// termAt returns the term of the entry at idx, or false if it has been
// compacted away or does not exist yet.
func (n *Node) termAt(idx uint64) (uint64, bool) {
	if idx < n.snapIndex() || idx > n.lastIndex() {
		return 0, false
	}
	return n.entries[idx-n.snapIndex()].Term, true
}

func (n *Node) entryAt(idx uint64) *pb.LogEntry { return n.entries[idx-n.snapIndex()] }

func (n *Node) quorum() int { return len(n.cfg.Peers)/2 + 1 }

func (n *Node) randomTimeout() time.Duration {
	lo, hi := n.cfg.ElectionTimeoutMin, n.cfg.ElectionTimeoutMax
	return lo + time.Duration(rand.Int64N(int64(hi-lo)+1))
}

func (n *Node) resetElectionTimer() { n.electionDeadline = time.Now().Add(n.randomTimeout()) }

func (n *Node) persistState() {
	if err := n.cfg.Storage.SaveState(n.term, n.votedFor); err != nil {
		// Losing the vote record could let us vote twice in a term, which
		// breaks election safety. Refuse to continue.
		panic("raft: persisting hard state: " + err.Error())
	}
}

// ------------------------------------------------------------- role changes

// becomeFollower must be called with mu held.
func (n *Node) becomeFollower(term uint64, leader string) {
	wasLeader := n.role == Leader
	if term > n.term {
		n.term = term
		n.votedFor = ""
		n.persistState()
	}
	n.role = Follower
	n.leaderID = leader
	if wasLeader {
		n.log.Info("stepping down", "term", n.term)
		n.failFutures(ErrLeadershipLost)
		n.emitLeader(false)
	}
}

func (n *Node) becomeLeader() {
	n.role = Leader
	n.leaderID = n.cfg.ID
	n.leaderSince = time.Now()
	n.stats.LeaderChanges++
	n.nextIndex = map[string]uint64{}
	n.matchIndex = map[string]uint64{}
	n.lastContact = map[string]time.Time{}
	n.notify = map[string]chan struct{}{}
	n.log.Info("became leader", "term", n.term, "last_index", n.lastIndex())

	// Everything we hold was fsynced when we received it as a follower.
	n.durable = n.lastIndex()

	// A no-op in the new term lets us commit (and therefore learn the commit
	// status of) everything inherited from previous leaders.
	n.appendLocal(&pb.LogEntry{Type: pb.EntryType_ENTRY_NOOP})

	term := n.term
	for id, addr := range n.cfg.Peers {
		if id == n.cfg.ID {
			continue
		}
		n.nextIndex[id] = n.lastIndex()
		n.matchIndex[id] = 0
		n.lastContact[id] = time.Now()
		ch := make(chan struct{}, 1)
		n.notify[id] = ch
		n.wg.Add(1)
		go n.replicate(id, addr, term, ch)
	}
	n.advanceCommit()
	n.emitLeader(true)
}

func (n *Node) emitLeader(leader bool) {
	select {
	case n.leaderEvents <- leaderEvent{leader: leader, term: n.term}:
	default:
		n.log.Warn("leader event dropped")
	}
}

func (n *Node) leaderNotifier() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case ev := <-n.leaderEvents:
			if n.cfg.OnLeaderChange != nil {
				n.cfg.OnLeaderChange(ev.leader, ev.term)
			}
		}
	}
}

func (n *Node) failFutures(err error) {
	for idx, f := range n.futures {
		f.ch <- result{err: err}
		delete(n.futures, idx)
	}
}

// ---------------------------------------------------------------- ticker

func (n *Node) ticker() {
	defer n.wg.Done()
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-t.C:
		}
		n.mu.Lock()
		now := time.Now()
		switch n.role {
		case Leader:
			n.checkQuorum(now)
		default:
			if now.After(n.electionDeadline) {
				n.resetElectionTimer()
				n.wg.Add(1)
				go n.runElection()
			}
		}
		n.mu.Unlock()
	}
}

// checkQuorum makes a leader that can no longer reach a majority step down,
// so clients on its side of a partition get redirected instead of writing
// into a log that can never commit.
func (n *Node) checkQuorum(now time.Time) {
	window := n.cfg.ElectionTimeoutMax
	if now.Sub(n.leaderSince) < window {
		return
	}
	alive := 1
	for _, t := range n.lastContact {
		if now.Sub(t) <= window {
			alive++
		}
	}
	if alive < n.quorum() {
		n.log.Warn("lost contact with quorum, stepping down", "alive", alive)
		n.becomeFollower(n.term, "")
		n.resetElectionTimer()
	}
}

// ---------------------------------------------------------------- elections

func (n *Node) runElection() {
	defer n.wg.Done()

	n.mu.Lock()
	if n.role == Leader || n.stopped {
		n.mu.Unlock()
		return
	}
	// Phase 1: pre-vote. Nothing is persisted and our term is unchanged.
	req := &pb.VoteRequest{
		Term:         n.term + 1,
		CandidateId:  n.cfg.ID,
		LastLogIndex: n.lastIndex(),
		LastLogTerm:  n.lastTerm(),
		PreVote:      true,
	}
	startTerm := n.term
	n.mu.Unlock()

	if !n.collectVotes(req) {
		return
	}
	n.log.Info("pre-vote succeeded", "next_term", req.Term)

	// Phase 2: the real election.
	n.mu.Lock()
	if n.term != startTerm || n.role == Leader || n.stopped {
		n.mu.Unlock()
		return
	}
	n.term++
	n.role = Candidate
	n.votedFor = n.cfg.ID
	n.leaderID = ""
	n.persistState()
	n.resetElectionTimer()
	n.stats.ElectionsStarted++
	req = &pb.VoteRequest{
		Term:         n.term,
		CandidateId:  n.cfg.ID,
		LastLogIndex: n.lastIndex(),
		LastLogTerm:  n.lastTerm(),
	}
	n.log.Info("starting election", "term", n.term)
	n.mu.Unlock()

	if !n.collectVotes(req) {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role == Candidate && n.term == req.Term && !n.stopped {
		n.becomeLeader()
	}
}

// collectVotes broadcasts req and reports whether a majority granted it.
// It returns as soon as the outcome is decided.
func (n *Node) collectVotes(req *pb.VoteRequest) bool {
	type vote struct {
		resp *pb.VoteResponse
		err  error
	}
	peers := 0
	ch := make(chan vote, len(n.cfg.Peers))
	for id, addr := range n.cfg.Peers {
		if id == n.cfg.ID {
			continue
		}
		peers++
		go func(addr string) {
			ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RPCTimeout)
			defer cancel()
			resp, err := n.cfg.Transport.RequestVote(ctx, addr, req)
			ch <- vote{resp, err}
		}(addr)
	}
	granted, need := 1, n.quorum()
	if granted >= need {
		return true
	}
	for i := 0; i < peers; i++ {
		v := <-ch
		if v.err != nil {
			continue
		}
		if v.resp.Term > req.Term {
			n.mu.Lock()
			if v.resp.Term > n.term {
				n.becomeFollower(v.resp.Term, "")
			}
			n.mu.Unlock()
			return false
		}
		if v.resp.Granted {
			granted++
			if granted >= need {
				return true
			}
		}
	}
	return false
}

// HandleRequestVote implements the RequestVote RPC receiver.
func (n *Node) HandleRequestVote(req *pb.VoteRequest) *pb.VoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	upToDate := req.LastLogTerm > n.lastTerm() ||
		(req.LastLogTerm == n.lastTerm() && req.LastLogIndex >= n.lastIndex())

	if req.PreVote {
		// Deny if we still hear from a healthy leader: the candidate is
		// probably the one that is cut off.
		leaderAlive := n.role == Leader ||
			(n.leaderID != "" && time.Since(n.lastLeaderSeen) < n.cfg.ElectionTimeoutMin)
		return &pb.VoteResponse{Term: n.term, Granted: req.Term > n.term && upToDate && !leaderAlive}
	}

	if req.Term < n.term {
		return &pb.VoteResponse{Term: n.term}
	}
	if req.Term > n.term {
		n.becomeFollower(req.Term, "")
	}
	if (n.votedFor == "" || n.votedFor == req.CandidateId) && upToDate {
		n.votedFor = req.CandidateId
		n.persistState()
		n.resetElectionTimer()
		return &pb.VoteResponse{Term: n.term, Granted: true}
	}
	return &pb.VoteResponse{Term: n.term}
}

// ---------------------------------------------------------------- replication

func (n *Node) appendLocal(e *pb.LogEntry) uint64 {
	e.Index = n.lastIndex() + 1
	e.Term = n.term
	// The leader may send entries to followers before they hit its own disk
	// (Raft thesis 10.2.1); it just can't count itself towards the quorum
	// until the persister has fsynced them. Concurrent proposals therefore
	// share one fsync: group commit.
	if err := n.cfg.Storage.AppendLogNoSync([]*pb.LogEntry{e}); err != nil {
		panic("raft: appending to log: " + err.Error())
	}
	n.entries = append(n.entries, e)
	select {
	case n.persistCh <- struct{}{}:
	default:
	}
	for _, ch := range n.notify {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	return e.Index
}

// replicate is the per-follower pipeline for one leadership term. It sends
// at most one RPC at a time, which keeps ordering trivial; batching makes up
// for the lack of pipelining.
func (n *Node) replicate(id, addr string, term uint64, notify chan struct{}) {
	defer n.wg.Done()
	hb := time.NewTicker(n.cfg.HeartbeatInterval)
	defer hb.Stop()
	for {
		more := n.sendOnce(id, addr, term)
		if more {
			select {
			case <-n.stopCh:
				return
			default:
				continue
			}
		}
		select {
		case <-n.stopCh:
			return
		case <-hb.C:
		case <-notify:
		}
		n.mu.Lock()
		still := n.role == Leader && n.term == term
		n.mu.Unlock()
		if !still {
			return
		}
	}
}

// sendOnce performs one AppendEntries or InstallSnapshot round. It returns
// true if the follower is still behind and another round should follow
// immediately.
func (n *Node) sendOnce(id, addr string, term uint64) bool {
	n.mu.Lock()
	if n.role != Leader || n.term != term || n.stopped {
		n.mu.Unlock()
		return false
	}
	next := n.nextIndex[id]
	if next <= n.snapIndex() {
		n.mu.Unlock()
		return n.sendSnapshot(id, addr, term)
	}
	prev := next - 1
	prevTerm, _ := n.termAt(prev)
	end := min(n.lastIndex(), prev+uint64(n.cfg.MaxBatch))
	var ents []*pb.LogEntry
	for i := next; i <= end; i++ {
		ents = append(ents, n.entryAt(i))
	}
	req := &pb.AppendRequest{
		Term:         term,
		LeaderId:     n.cfg.ID,
		PrevLogIndex: prev,
		PrevLogTerm:  prevTerm,
		Entries:      ents,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RPCTimeout)
	resp, err := n.cfg.Transport.AppendEntries(ctx, addr, req)
	cancel()
	if err != nil {
		return false
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if resp.Term > n.term {
		n.becomeFollower(resp.Term, "")
		n.resetElectionTimer()
		return false
	}
	if n.role != Leader || n.term != term {
		return false
	}
	n.lastContact[id] = time.Now()
	if resp.Success {
		match := prev + uint64(len(ents))
		if match > n.matchIndex[id] {
			n.matchIndex[id] = match
		}
		n.nextIndex[id] = n.matchIndex[id] + 1
		n.advanceCommit()
		return n.nextIndex[id] <= n.lastIndex()
	}
	// Rejected: jump back using the follower's hint (Raft section 5.3).
	newNext := resp.ConflictIndex
	if resp.ConflictTerm != 0 {
		for i := n.lastIndex(); i > n.snapIndex(); i-- {
			if t, _ := n.termAt(i); t == resp.ConflictTerm {
				newNext = i + 1
				break
			} else if t < resp.ConflictTerm {
				break
			}
		}
	}
	if newNext < 1 {
		newNext = 1
	}
	if newNext >= next {
		newNext = next - 1
	}
	n.nextIndex[id] = max(newNext, 1)
	return true
}

func (n *Node) sendSnapshot(id, addr string, term uint64) bool {
	n.mu.Lock()
	_, _, data, err := n.cfg.Storage.LoadSnapshot()
	req := &pb.SnapshotRequest{
		Term:              term,
		LeaderId:          n.cfg.ID,
		LastIncludedIndex: n.snapIndex(),
		LastIncludedTerm:  n.snapTerm(),
		Data:              data,
	}
	n.mu.Unlock()
	if err != nil {
		n.log.Error("loading snapshot to send", "err", err)
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*n.cfg.RPCTimeout)
	resp, err := n.cfg.Transport.InstallSnapshot(ctx, addr, req)
	cancel()
	if err != nil {
		return false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if resp.Term > n.term {
		n.becomeFollower(resp.Term, "")
		return false
	}
	if n.role != Leader || n.term != term {
		return false
	}
	n.lastContact[id] = time.Now()
	n.matchIndex[id] = max(n.matchIndex[id], req.LastIncludedIndex)
	n.nextIndex[id] = n.matchIndex[id] + 1
	n.log.Info("follower caught up via snapshot", "peer", id, "index", req.LastIncludedIndex)
	return n.nextIndex[id] <= n.lastIndex()
}

// advanceCommit moves commitIndex to the highest index replicated on a
// majority whose entry is from the current term (Figure 8 safety rule).
func (n *Node) persister() {
	defer n.wg.Done()
	for {
		select {
		case <-n.stopCh:
			return
		case <-n.persistCh:
		}
		n.mu.Lock()
		target := n.lastIndex()
		n.mu.Unlock()
		if err := n.cfg.Storage.Sync(); err != nil {
			panic("raft: fsync: " + err.Error())
		}
		n.mu.Lock()
		if target > n.durable {
			n.durable = target
		}
		if n.role == Leader {
			n.advanceCommit()
		}
		n.mu.Unlock()
	}
}

func (n *Node) advanceCommit() {
	matches := []uint64{min(n.lastIndex(), n.durable)}
	for _, m := range n.matchIndex {
		matches = append(matches, m)
	}
	sort.Slice(matches, func(i, j int) bool { return matches[i] > matches[j] })
	candidate := matches[n.quorum()-1]
	if candidate <= n.commitIndex {
		return
	}
	if t, ok := n.termAt(candidate); ok && t == n.term {
		n.commitIndex = candidate
		n.applyCond.Broadcast()
	}
}

// HandleAppendEntries implements the AppendEntries RPC receiver.
func (n *Node) HandleAppendEntries(req *pb.AppendRequest) *pb.AppendResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.term {
		return &pb.AppendResponse{Term: n.term, LastIndex: n.lastIndex()}
	}
	if req.Term > n.term || n.role != Follower {
		n.becomeFollower(req.Term, req.LeaderId)
	}
	n.leaderID = req.LeaderId
	n.lastLeaderSeen = time.Now()
	n.resetElectionTimer()

	prev, prevTerm, ents := req.PrevLogIndex, req.PrevLogTerm, req.Entries
	// Entries at or below our snapshot are committed and identical; skip
	// them and re-anchor on the snapshot.
	if prev < n.snapIndex() {
		skip := n.snapIndex() - prev
		if uint64(len(ents)) <= skip {
			ents = nil
		} else {
			ents = ents[skip:]
		}
		prev, prevTerm = n.snapIndex(), n.snapTerm()
	}

	if prev > n.lastIndex() {
		return &pb.AppendResponse{Term: n.term, ConflictIndex: n.lastIndex() + 1, LastIndex: n.lastIndex()}
	}
	if t, _ := n.termAt(prev); t != prevTerm {
		first := prev
		for first-1 > n.snapIndex() {
			if pt, _ := n.termAt(first - 1); pt != t {
				break
			}
			first--
		}
		return &pb.AppendResponse{Term: n.term, ConflictTerm: t, ConflictIndex: first, LastIndex: n.lastIndex()}
	}

	for i, e := range ents {
		if e.Index <= n.lastIndex() {
			if t, _ := n.termAt(e.Index); t == e.Term {
				continue
			}
			// Conflict: drop our divergent suffix. Committed entries can
			// never conflict, so this never touches applied state.
			n.entries = n.entries[:e.Index-n.snapIndex()]
			if err := n.cfg.Storage.RewriteLog(n.entries[1:]); err != nil {
				panic("raft: truncating log: " + err.Error())
			}
		}
		rest := ents[i:]
		if err := n.cfg.Storage.AppendLog(rest); err != nil {
			panic("raft: appending to log: " + err.Error())
		}
		n.entries = append(n.entries, rest...)
		break
	}

	if req.LeaderCommit > n.commitIndex {
		lastNew := prev + uint64(len(ents))
		n.commitIndex = min(req.LeaderCommit, lastNew)
		n.applyCond.Broadcast()
	}
	return &pb.AppendResponse{Term: n.term, Success: true, LastIndex: n.lastIndex()}
}

// HandleInstallSnapshot implements the InstallSnapshot RPC receiver.
func (n *Node) HandleInstallSnapshot(req *pb.SnapshotRequest) *pb.SnapshotResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	if req.Term < n.term {
		return &pb.SnapshotResponse{Term: n.term}
	}
	if req.Term > n.term || n.role != Follower {
		n.becomeFollower(req.Term, req.LeaderId)
	}
	n.leaderID = req.LeaderId
	n.lastLeaderSeen = time.Now()
	n.resetElectionTimer()

	if req.LastIncludedIndex <= n.lastApplied || req.LastIncludedIndex <= n.snapIndex() {
		return &pb.SnapshotResponse{Term: n.term}
	}
	if err := n.cfg.Storage.SaveSnapshot(req.LastIncludedIndex, req.LastIncludedTerm, req.Data); err != nil {
		n.log.Error("saving snapshot", "err", err)
		return &pb.SnapshotResponse{Term: n.term}
	}
	sentinel := &pb.LogEntry{Index: req.LastIncludedIndex, Term: req.LastIncludedTerm}
	if t, ok := n.termAt(req.LastIncludedIndex); ok && t == req.LastIncludedTerm {
		// We already have a matching prefix; keep whatever follows it.
		tail := n.entries[req.LastIncludedIndex-n.snapIndex()+1:]
		n.entries = append([]*pb.LogEntry{sentinel}, tail...)
	} else {
		n.entries = []*pb.LogEntry{sentinel}
	}
	if err := n.cfg.Storage.RewriteLog(n.entries[1:]); err != nil {
		panic("raft: rewriting log after snapshot: " + err.Error())
	}
	n.pendingSnap = req.Data
	n.commitIndex = max(n.commitIndex, req.LastIncludedIndex)
	n.stats.SnapshotsLoaded++
	n.applyCond.Broadcast()
	return &pb.SnapshotResponse{Term: n.term}
}

// ---------------------------------------------------------------- applying

func (n *Node) applier() {
	defer n.wg.Done()
	for {
		n.mu.Lock()
		for !n.stopped && n.pendingSnap == nil && n.lastApplied >= n.commitIndex {
			n.applyCond.Wait()
		}
		if n.stopped {
			n.mu.Unlock()
			return
		}
		if snap := n.pendingSnap; snap != nil {
			n.pendingSnap = nil
			n.lastApplied = n.snapIndex()
			n.mu.Unlock()
			if err := n.cfg.FSM.Restore(snap); err != nil {
				n.log.Error("restoring snapshot", "err", err)
			}
			continue
		}
		start := n.lastApplied + 1
		batch := make([]*pb.LogEntry, 0, n.commitIndex-n.lastApplied)
		for i := start; i <= n.commitIndex; i++ {
			batch = append(batch, n.entryAt(i))
		}
		n.lastApplied = n.commitIndex
		n.mu.Unlock()

		for _, e := range batch {
			var val any
			if e.Type == pb.EntryType_ENTRY_COMMAND {
				val = n.cfg.FSM.Apply(e.Index, e.Data)
			}
			n.mu.Lock()
			if f, ok := n.futures[e.Index]; ok {
				if f.term == e.Term {
					f.ch <- result{val: val}
				} else {
					f.ch <- result{err: ErrLeadershipLost}
				}
				delete(n.futures, e.Index)
			}
			n.mu.Unlock()
		}
		n.maybeSnapshot()
	}
}

func (n *Node) maybeSnapshot() {
	if n.cfg.SnapshotThreshold == 0 {
		return
	}
	n.mu.Lock()
	idx := n.lastApplied
	if idx-n.snapIndex() < n.cfg.SnapshotThreshold || n.snapshotting {
		n.mu.Unlock()
		return
	}
	term, ok := n.termAt(idx)
	n.snapshotting = true
	n.mu.Unlock()
	defer func() {
		n.mu.Lock()
		n.snapshotting = false
		n.mu.Unlock()
	}()
	if !ok {
		return
	}
	// Only the applier mutates the FSM, so this snapshot reflects exactly
	// the state as of idx.
	data, err := n.cfg.FSM.Snapshot()
	if err != nil {
		n.log.Error("taking snapshot", "err", err)
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if idx <= n.snapIndex() {
		return
	}
	if err := n.cfg.Storage.SaveSnapshot(idx, term, data); err != nil {
		n.log.Error("saving snapshot", "err", err)
		return
	}
	tail := n.entries[idx-n.snapIndex()+1:]
	n.entries = append([]*pb.LogEntry{{Index: idx, Term: term}}, tail...)
	if err := n.cfg.Storage.RewriteLog(n.entries[1:]); err != nil {
		panic("raft: compacting log: " + err.Error())
	}
	n.stats.SnapshotsTaken++
	n.log.Debug("snapshot taken", "index", idx, "remaining", len(tail))
}

// ---------------------------------------------------------------- public API

// Apply proposes a command and blocks until it is committed and applied,
// returning the FSM's result.
func (n *Node) Apply(ctx context.Context, data []byte) (any, error) {
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return nil, ErrStopped
	}
	if n.role != Leader {
		n.mu.Unlock()
		return nil, ErrNotLeader
	}
	idx := n.appendLocal(&pb.LogEntry{Type: pb.EntryType_ENTRY_COMMAND, Data: data})
	f := &future{term: n.term, ch: make(chan result, 1)}
	n.futures[idx] = f
	n.advanceCommit() // single-node clusters commit immediately
	n.mu.Unlock()

	select {
	case r := <-f.ch:
		return r.val, r.err
	case <-ctx.Done():
		return nil, ErrTimeout
	}
}

// Leader returns the current leader's id and address, if known.
func (n *Node) Leader() (id, addr string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID, n.cfg.Peers[n.leaderID]
}

func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role == Leader
}

type PeerStatus struct {
	ID          string `json:"id"`
	Address     string `json:"address"`
	MatchIndex  uint64 `json:"match_index"`
	NextIndex   uint64 `json:"next_index"`
	LastContact int64  `json:"last_contact_ms"` // ms ago, -1 if unknown
}

type Status struct {
	ID            string       `json:"id"`
	Role          string       `json:"role"`
	Term          uint64       `json:"term"`
	Leader        string       `json:"leader"`
	CommitIndex   uint64       `json:"commit_index"`
	LastApplied   uint64       `json:"last_applied"`
	LastIndex     uint64       `json:"last_index"`
	SnapshotIndex uint64       `json:"snapshot_index"`
	Voters        int          `json:"voters"`
	Peers         []PeerStatus `json:"peers,omitempty"`
	Stats         Stats        `json:"stats"`
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	s := Status{
		ID: n.cfg.ID, Role: n.role.String(), Term: n.term, Leader: n.leaderID,
		CommitIndex: n.commitIndex, LastApplied: n.lastApplied, LastIndex: n.lastIndex(),
		SnapshotIndex: n.snapIndex(), Voters: len(n.cfg.Peers), Stats: n.stats,
	}
	if n.role == Leader {
		now := time.Now()
		for id, addr := range n.cfg.Peers {
			if id == n.cfg.ID {
				continue
			}
			s.Peers = append(s.Peers, PeerStatus{
				ID: id, Address: addr, MatchIndex: n.matchIndex[id], NextIndex: n.nextIndex[id],
				LastContact: now.Sub(n.lastContact[id]).Milliseconds(),
			})
		}
		sort.Slice(s.Peers, func(i, j int) bool { return s.Peers[i].ID < s.Peers[j].ID })
	}
	return s
}
