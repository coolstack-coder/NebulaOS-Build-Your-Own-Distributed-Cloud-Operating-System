package raft

import (
	"os"
	"path/filepath"
	"testing"

	pb "github.com/coolstack-coder/nebulaos/gen/nebula/v1"
)

func entries(from, to uint64, term uint64) []*pb.LogEntry {
	var out []*pb.LogEntry
	for i := from; i <= to; i++ {
		out = append(out, &pb.LogEntry{Index: i, Term: term, Data: []byte{byte(i)}})
	}
	return out
}

func TestFileStorageSurvivesTornWrite(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileStorage(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AppendLog(entries(1, 10, 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveState(3, "n2"); err != nil {
		t.Fatal(err)
	}
	s.Close()

	// Simulate a crash halfway through writing the next record.
	f, _ := os.OpenFile(filepath.Join(dir, "raft.log"), os.O_APPEND|os.O_WRONLY, 0)
	f.Write([]byte{42, 0, 0, 0, 1, 2, 3})
	f.Close()

	s, err = NewFileStorage(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.LoadLog()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 10 || got[9].Index != 10 {
		t.Fatalf("expected 10 intact entries, got %d", len(got))
	}
	// Appends after recovery must land on a clean record boundary.
	if err := s.AppendLog(entries(11, 12, 2)); err != nil {
		t.Fatal(err)
	}
	got, _ = s.LoadLog()
	if len(got) != 12 || got[11].Term != 2 {
		t.Fatalf("append after torn write broke the log: %d entries", len(got))
	}
	term, vote, _ := s.LoadState()
	if term != 3 || vote != "n2" {
		t.Fatalf("hard state lost: %d %q", term, vote)
	}
}

func TestFileStorageRewriteAndSnapshot(t *testing.T) {
	s, err := NewFileStorage(t.TempDir(), true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	_ = s.AppendLog(entries(1, 20, 1))
	if err := s.RewriteLog(entries(15, 20, 1)); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendLogNoSync(entries(21, 21, 2)); err != nil {
		t.Fatal(err)
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	got, _ := s.LoadLog()
	if len(got) != 7 || got[0].Index != 15 || got[6].Index != 21 {
		t.Fatalf("unexpected log after rewrite: %d entries", len(got))
	}
	if err := s.SaveSnapshot(14, 1, []byte("state")); err != nil {
		t.Fatal(err)
	}
	idx, term, data, err := s.LoadSnapshot()
	if err != nil || idx != 14 || term != 1 || string(data) != "state" {
		t.Fatalf("snapshot round trip: %d %d %q %v", idx, term, data, err)
	}
}
