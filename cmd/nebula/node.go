package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	goruntime "runtime"
	"strings"
	"syscall"
	"time"

	"github.com/coolstack-coder/nebulaos/internal/node"
	"github.com/coolstack-coder/nebulaos/internal/state"
	"github.com/coolstack-coder/nebulaos/web"
)

func parseKV(s string) (map[string]string, error) {
	out := map[string]string{}
	if strings.TrimSpace(s) == "" {
		return out, nil
	}
	for _, part := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || k == "" {
			return nil, fmt.Errorf("expected key=value, got %q", part)
		}
		out[k] = v
	}
	return out, nil
}

func runNode(args []string) error {
	fs := flag.NewFlagSet("node", flag.ExitOnError)
	var (
		id        = fs.String("id", "", "unique node id (required)")
		grpcAddr  = fs.String("grpc", "127.0.0.1:7001", "gRPC listen address (Raft + cluster API)")
		apiAddr   = fs.String("api", "127.0.0.1:8001", "HTTP API / dashboard listen address")
		advGRPC   = fs.String("advertise-grpc", "", "gRPC address peers should use (default: --grpc)")
		advAPI    = fs.String("advertise-api", "", "HTTP address peers should use (default: --api)")
		peers     = fs.String("peers", "", "control-plane voters as id=host:port,... (include this node to make it a voter)")
		join      = fs.String("join", "", "worker-only: comma-separated server gRPC addresses")
		dataDir   = fs.String("data", "", "data directory (default ./data/<id>)")
		rt        = fs.String("runtime", "sim", "task runtime: sim | process | docker | containerd")
		isolate   = fs.Bool("isolate", false, "process runtime: run tasks in new Linux namespaces (root only)")
		cpu       = fs.Int64("cpu", int64(goruntime.NumCPU())*1000, "schedulable CPU in millicores")
		mem       = fs.Int64("mem", 8192, "schedulable memory in MB")
		labels    = fs.String("labels", "", "node labels as k=v,...")
		hb        = fs.Duration("heartbeat", 500*time.Millisecond, "agent heartbeat interval")
		timeout   = fs.Duration("node-timeout", 3*time.Second, "declare a node down after this long without heartbeats")
		election  = fs.Duration("election-timeout", 450*time.Millisecond, "minimum Raft election timeout (max is 2x)")
		snapEvery = fs.Uint64("snapshot-every", 2048, "compact the Raft log every N applied entries")
		fsync     = fs.Bool("fsync", true, "fsync the Raft log on every append")
		logLevel  = fs.String("log", "info", "log level: debug | info | warn | error")
	)
	fs.Parse(args)
	if *id == "" {
		return fmt.Errorf("--id is required")
	}
	peerMap, err := parseKV(*peers)
	if err != nil {
		return fmt.Errorf("--peers: %w", err)
	}
	labelMap, err := parseKV(*labels)
	if err != nil {
		return fmt.Errorf("--labels: %w", err)
	}
	var joinList []string
	for _, j := range strings.Split(*join, ",") {
		if j = strings.TrimSpace(j); j != "" {
			joinList = append(joinList, j)
		}
	}
	if _, voter := peerMap[*id]; !voter && len(joinList) == 0 {
		if len(peerMap) == 0 {
			// Zero-config single node cluster.
			peerMap[*id] = *grpcAddr
			if *advGRPC != "" {
				peerMap[*id] = *advGRPC
			}
		} else {
			for _, a := range peerMap {
				joinList = append(joinList, a)
			}
		}
	}
	if *dataDir == "" {
		*dataDir = "data/" + *id
	}

	var lvl slog.Level
	_ = lvl.UnmarshalText([]byte(*logLevel))
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))

	n, err := node.New(node.Config{
		ID: *id, DataDir: *dataDir, GRPCAddr: *grpcAddr, APIAddr: *apiAddr,
		AdvertiseGRPC: *advGRPC, AdvertiseAPI: *advAPI,
		Peers: peerMap, Join: joinList,
		Capacity: state.Resources{CPU: *cpu, Memory: *mem}, Labels: labelMap,
		Runtime: *rt, Isolate: *isolate,
		HeartbeatInterval: *hb, NodeTimeout: *timeout, ElectionTimeout: *election,
		SnapshotThreshold: *snapEvery, Fsync: *fsync, Logger: logger,
		UI: web.Handler(),
	})
	if err != nil {
		return err
	}
	if err := n.Start(); err != nil {
		return err
	}
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	logger.Info("shutting down")
	n.Stop()
	return nil
}
