package raft

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	pb "github.com/coolstack-coder/nebulaos/gen/nebula/v1"
)

// crashPoint is where in InstallSnapshot a follower process gets killed.
type crashPoint int

const (
	// The snapshot temp file is half written; the old snapshot is untouched.
	crashMidSnapshotWrite crashPoint = iota
	// The new snapshot is on disk but the log has not been rewritten yet.
	crashAfterSnapshotSave
	// Snapshot and log are both on disk; the FSM has not been restored yet.
	crashAfterLogRewrite
)

func (p crashPoint) String() string {
	switch p {
	case crashMidSnapshotWrite:
		return "mid_snapshot_write"
	case crashAfterSnapshotSave:
		return "after_snapshot_save"
	default:
		return "after_log_rewrite"
	}
}

// crashingStorage wraps a FileStorage and simulates a SIGKILL at a chosen
// point of a snapshot install. Writes up to the crash point reach the disk,
// every write after it is dropped as if the process had died, and onCrash
// cuts the node off so its in-flight reply is never delivered.
type crashingStorage struct {
	Storage
	dir     string
	at      crashPoint
	onCrash func(snapIndex uint64)

	mu      sync.Mutex
	dead    bool
	snapIdx uint64
}

func (s *crashingStorage) die() {
	s.dead = true
	s.onCrash(s.snapIdx)
}

func (s *crashingStorage) SaveSnapshot(index, term uint64, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dead {
		return nil
	}
	s.snapIdx = index
	switch s.at {
	case crashMidSnapshotWrite:
		torn := data[:len(data)/2]
		if err := os.WriteFile(filepath.Join(s.dir, "snapshot.bin.tmp"), torn, 0o644); err != nil {
			return err
		}
		s.die()
		return nil
	case crashAfterSnapshotSave:
		if err := s.Storage.SaveSnapshot(index, term, data); err != nil {
			return err
		}
		s.die()
		return nil
	}
	return s.Storage.SaveSnapshot(index, term, data)
}

func (s *crashingStorage) RewriteLog(entries []*pb.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dead {
		return nil
	}
	if err := s.Storage.RewriteLog(entries); err != nil {
		return err
	}
	if s.at == crashAfterLogRewrite && s.snapIdx != 0 {
		s.die()
	}
	return nil
}

func (s *crashingStorage) SaveState(term uint64, votedFor string) error {
	if s.isDead() {
		return nil
	}
	return s.Storage.SaveState(term, votedFor)
}

func (s *crashingStorage) AppendLog(entries []*pb.LogEntry) error {
	if s.isDead() {
		return nil
	}
	return s.Storage.AppendLog(entries)
}

func (s *crashingStorage) AppendLogNoSync(entries []*pb.LogEntry) error {
	if s.isDead() {
		return nil
	}
	return s.Storage.AppendLogNoSync(entries)
}

func (s *crashingStorage) isDead() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dead
}

// newFileCluster runs every node on a real FileStorage under root. Writes
// are not fsynced: a SIGKILL leaves the page cache intact, so fsync does not
// change what a killed process leaves behind.
func newFileCluster(t *testing.T, size int, snapshotThreshold uint64, root string) *cluster {
	return newClusterWith(t, size, snapshotThreshold, func(id string) Storage {
		s, err := NewFileStorage(filepath.Join(root, id), false)
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}

// TestFollowerKilledDuringSnapshotInstall kills a lagging follower at each
// point of InstallSnapshot, lets the leader compact past the snapshot it was
// installing, then restarts it from whatever reached the disk. The follower
// must come back and converge on exactly the leader's state.
func TestFollowerKilledDuringSnapshotInstall(t *testing.T) {
	for _, at := range []crashPoint{crashMidSnapshotWrite, crashAfterSnapshotSave, crashAfterLogRewrite} {
		t.Run(at.String(), func(t *testing.T) { testKilledDuringInstall(t, at) })
	}
}

func testKilledDuringInstall(t *testing.T, at crashPoint) {
	root := t.TempDir()
	c := newFileCluster(t, 3, 20, root)
	l := c.leader()
	var victim string
	for _, id := range c.ids {
		if id != l {
			victim = id
			break
		}
	}
	write := func(from, to int) {
		for i := from; i < to; i++ {
			if err := c.apply(l, fmt.Sprintf("v%d", i)); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Fall far enough behind that the leader has to send a snapshot.
	c.setDown(victim, true)
	write(0, 60)

	// Bring the follower back on storage that dies partway through the
	// install.
	c.crash(victim)
	crashed := make(chan uint64, 1)
	c.startWith(victim, &crashingStorage{
		Storage: c.open(victim),
		dir:     filepath.Join(root, victim),
		at:      at,
		onCrash: func(idx uint64) {
			c.setDown(victim, true)
			crashed <- idx
		},
	})
	c.setDown(victim, false)
	var installing uint64
	select {
	case installing = <-crashed:
	case <-time.After(5 * time.Second):
		t.Fatal("follower never started installing a snapshot")
	}
	c.crash(victim)

	// While it is down the leader moves on and compacts past the snapshot
	// the follower was installing, so the restart cannot just resume it.
	write(60, 120)
	if s := c.nodes[l].Status(); s.SnapshotIndex <= installing {
		t.Fatalf("leader did not compact past index %d (snapshot at %d)", installing, s.SnapshotIndex)
	}

	// Restart from disk while still cut off and check what survived: a
	// snapshot that was fully saved must be kept, a torn one ignored.
	c.start(victim)
	recovered := c.nodes[victim].Status().SnapshotIndex
	if at == crashMidSnapshotWrite && recovered >= installing {
		t.Fatalf("torn snapshot %d was loaded (recovered %d)", installing, recovered)
	}
	if at != crashMidSnapshotWrite && recovered != installing {
		t.Fatalf("saved snapshot %d was lost on restart (recovered %d)", installing, recovered)
	}
	c.checkPrefixes()

	c.setDown(victim, false)
	c.waitApplied(120, c.ids...)
	c.checkPrefixes()
	if c.nodes[victim].Status().Stats.SnapshotsLoaded == 0 {
		t.Fatal("expected the restarted follower to catch up via a snapshot")
	}
	if got, want := c.fsms[victim].history(), c.fsms[l].history(); len(got) != len(want) {
		t.Fatalf("follower applied %d commands, leader %d", len(got), len(want))
	}
}
