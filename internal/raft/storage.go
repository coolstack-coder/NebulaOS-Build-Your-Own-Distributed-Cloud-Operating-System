package raft

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	pb "github.com/coolstack-coder/nebulaos/gen/nebula/v1"
	"google.golang.org/protobuf/proto"
)

// Storage persists the three pieces of Raft state that must survive a crash:
// the hard state (term + vote), the log, and the latest snapshot.
//
// The log is an append-only file of length-prefixed, CRC-checked protobuf
// records. Appends are the hot path and only ever fsync the tail; rewrites
// (conflict truncation, compaction) go through a temp file + rename so a
// crash mid-rewrite leaves the previous file intact.
type Storage interface {
	LoadState() (term uint64, votedFor string, err error)
	SaveState(term uint64, votedFor string) error
	LoadLog() ([]*pb.LogEntry, error)
	AppendLog(entries []*pb.LogEntry) error
	// AppendLogNoSync writes entries without waiting for the disk; a later
	// Sync makes them durable. The leader uses this for group commit.
	AppendLogNoSync(entries []*pb.LogEntry) error
	Sync() error
	RewriteLog(entries []*pb.LogEntry) error
	LoadSnapshot() (index, term uint64, data []byte, err error)
	SaveSnapshot(index, term uint64, data []byte) error
	Close() error
}

type hardState struct {
	Term     uint64 `json:"term"`
	VotedFor string `json:"voted_for"`
}

// FileStorage is the on-disk Storage implementation.
type FileStorage struct {
	dir   string
	sync  bool
	crcTb *crc32.Table

	mu   sync.Mutex // guards logF, which RewriteLog swaps
	logF *os.File
}

// NewFileStorage opens (or creates) Raft storage under dir. When sync is
// false writes are not fsynced, which is only appropriate for tests and
// benchmarks that do not simulate power loss.
func NewFileStorage(dir string, sync bool) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &FileStorage{dir: dir, sync: sync, crcTb: crc32.MakeTable(crc32.Castagnoli)}
	f, err := os.OpenFile(s.path("raft.log"), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	s.logF = f
	return s, nil
}

func (s *FileStorage) path(name string) string { return filepath.Join(s.dir, name) }

func (s *FileStorage) LoadState() (uint64, string, error) {
	b, err := os.ReadFile(s.path("state.json"))
	if errors.Is(err, os.ErrNotExist) {
		return 0, "", nil
	}
	if err != nil {
		return 0, "", err
	}
	var hs hardState
	if err := json.Unmarshal(b, &hs); err != nil {
		return 0, "", fmt.Errorf("corrupt raft state: %w", err)
	}
	return hs.Term, hs.VotedFor, nil
}

func (s *FileStorage) SaveState(term uint64, votedFor string) error {
	b, _ := json.Marshal(hardState{Term: term, VotedFor: votedFor})
	return s.writeAtomic("state.json", b)
}

func (s *FileStorage) LoadLog() ([]*pb.LogEntry, error) {
	if _, err := s.logF.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	r := bufio.NewReader(s.logF)
	var (
		out  []*pb.LogEntry
		good int64
		hdr  [8]byte
	)
	for {
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			break // clean EOF or a torn header at the tail
		}
		n := binary.LittleEndian.Uint32(hdr[0:4])
		sum := binary.LittleEndian.Uint32(hdr[4:8])
		buf := make([]byte, n)
		if _, err := io.ReadFull(r, buf); err != nil {
			break
		}
		if crc32.Checksum(buf, s.crcTb) != sum {
			break
		}
		e := &pb.LogEntry{}
		if err := proto.Unmarshal(buf, e); err != nil {
			break
		}
		out = append(out, e)
		good += int64(8 + n)
	}
	// Drop any torn write left over from a crash so later appends start at a
	// record boundary.
	if st, err := s.logF.Stat(); err == nil && st.Size() > good {
		if err := s.logF.Truncate(good); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (s *FileStorage) encode(entries []*pb.LogEntry) ([]byte, error) {
	var out []byte
	for _, e := range entries {
		b, err := proto.Marshal(e)
		if err != nil {
			return nil, err
		}
		var hdr [8]byte
		binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(b)))
		binary.LittleEndian.PutUint32(hdr[4:8], crc32.Checksum(b, s.crcTb))
		out = append(out, hdr[:]...)
		out = append(out, b...)
	}
	return out, nil
}

func (s *FileStorage) AppendLog(entries []*pb.LogEntry) error {
	if err := s.AppendLogNoSync(entries); err != nil {
		return err
	}
	return s.Sync()
}

func (s *FileStorage) AppendLogNoSync(entries []*pb.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}
	b, err := s.encode(entries)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err = s.logF.Write(b)
	return err
}

// Sync flushes everything written so far. It deliberately does not hold the
// lock across the fsync so appends can continue while the disk catches up.
func (s *FileStorage) Sync() error {
	if !s.sync {
		return nil
	}
	s.mu.Lock()
	f := s.logF
	s.mu.Unlock()
	err := f.Sync()
	if errors.Is(err, os.ErrClosed) {
		// RewriteLog replaced the file meanwhile; it fsyncs the new one,
		// which holds every entry we were asked to make durable.
		return nil
	}
	return err
}

func (s *FileStorage) RewriteLog(entries []*pb.LogEntry) error {
	b, err := s.encode(entries)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.writeAtomic("raft.log", b); err != nil {
		return err
	}
	_ = s.logF.Close()
	f, err := os.OpenFile(s.path("raft.log"), os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	s.logF = f
	return nil
}

func (s *FileStorage) LoadSnapshot() (uint64, uint64, []byte, error) {
	b, err := os.ReadFile(s.path("snapshot.bin"))
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil, nil
	}
	if err != nil {
		return 0, 0, nil, err
	}
	if len(b) < 20 {
		return 0, 0, nil, errors.New("snapshot file truncated")
	}
	idx := binary.LittleEndian.Uint64(b[0:8])
	term := binary.LittleEndian.Uint64(b[8:16])
	sum := binary.LittleEndian.Uint32(b[16:20])
	data := b[20:]
	if crc32.Checksum(data, s.crcTb) != sum {
		return 0, 0, nil, errors.New("snapshot checksum mismatch")
	}
	return idx, term, data, nil
}

func (s *FileStorage) SaveSnapshot(index, term uint64, data []byte) error {
	b := make([]byte, 20+len(data))
	binary.LittleEndian.PutUint64(b[0:8], index)
	binary.LittleEndian.PutUint64(b[8:16], term)
	binary.LittleEndian.PutUint32(b[16:20], crc32.Checksum(data, s.crcTb))
	copy(b[20:], data)
	return s.writeAtomic("snapshot.bin", b)
}

func (s *FileStorage) writeAtomic(name string, b []byte) error {
	tmp := s.path(name + ".tmp")
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if s.sync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path(name)); err != nil {
		return err
	}
	if !s.sync {
		return nil
	}
	// The rename is only durable once the directory is synced. Without this
	// a power loss after compaction could keep the rewritten log but lose
	// the snapshot it starts from.
	return syncDir(s.dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}

func (s *FileStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.logF.Close()
}

// MemoryStorage keeps everything in RAM. Used by tests, where it also lets a
// "crashed" node restart with exactly the state it had persisted.
type MemoryStorage struct {
	term     uint64
	votedFor string
	log      []*pb.LogEntry
	snapIdx  uint64
	snapTerm uint64
	snap     []byte
}

func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func (m *MemoryStorage) LoadState() (uint64, string, error) { return m.term, m.votedFor, nil }
func (m *MemoryStorage) SaveState(t uint64, v string) error {
	m.term, m.votedFor = t, v
	return nil
}
func (m *MemoryStorage) LoadLog() ([]*pb.LogEntry, error) {
	return append([]*pb.LogEntry(nil), m.log...), nil
}
func (m *MemoryStorage) AppendLog(e []*pb.LogEntry) error {
	m.log = append(m.log, e...)
	return nil
}
func (m *MemoryStorage) AppendLogNoSync(e []*pb.LogEntry) error { return m.AppendLog(e) }
func (m *MemoryStorage) Sync() error                            { return nil }
func (m *MemoryStorage) RewriteLog(e []*pb.LogEntry) error {
	m.log = append([]*pb.LogEntry(nil), e...)
	return nil
}
func (m *MemoryStorage) LoadSnapshot() (uint64, uint64, []byte, error) {
	return m.snapIdx, m.snapTerm, m.snap, nil
}
func (m *MemoryStorage) SaveSnapshot(i, t uint64, d []byte) error {
	m.snapIdx, m.snapTerm, m.snap = i, t, append([]byte(nil), d...)
	return nil
}
func (m *MemoryStorage) Close() error { return nil }
