package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/coolstack-coder/nebulaos/internal/demo"
	"github.com/coolstack-coder/nebulaos/internal/state"
	"github.com/coolstack-coder/nebulaos/web"
)

func demoConfig(fs *flag.FlagSet) *demo.Config {
	c := &demo.Config{}
	fs.IntVar(&c.Nodes, "nodes", 5, "number of nodes")
	fs.IntVar(&c.GRPCBase, "grpc-base", 7700, "node i gets gRPC port grpc-base+i")
	fs.IntVar(&c.APIBase, "api-base", 8700, "node i gets HTTP port api-base+i")
	fs.StringVar(&c.Runtime, "runtime", "sim", "task runtime: sim | docker | process")
	fs.StringVar(&c.DataDir, "data", ".nebula/demo", "data directory (wiped on start)")
	fs.Int64Var(&c.CPU, "cpu", 4000, "millicores per node")
	fs.Int64Var(&c.Memory, "mem", 8192, "MB per node")
	fs.DurationVar(&c.NodeTimeout, "node-timeout", 2*time.Second, "failure detection timeout")
	fs.DurationVar(&c.Heartbeat, "heartbeat", 300*time.Millisecond, "agent heartbeat interval")
	fs.DurationVar(&c.Election, "election-timeout", 500*time.Millisecond, "min Raft election timeout")
	fs.BoolVar(&c.Fsync, "fsync", true, "fsync Raft writes")
	return c
}

// sampleServices is the workload the demo deploys with --seed. Images are
// real so the same seed works with --runtime docker.
func sampleServices() []state.ServiceSpec {
	return []state.ServiceSpec{
		{Name: "web", Image: "nginx:1.27-alpine", Replicas: 4, Port: 80,
			Resources: state.Resources{CPU: 300, Memory: 256}, SimDemand: 700,
			Autoscale: &state.Autoscale{Min: 3, Max: 14, TargetCPU: 60}},
		{Name: "api", Image: "traefik/whoami:v1.10", Replicas: 3, Port: 80,
			Resources: state.Resources{CPU: 400, Memory: 384}, SimDemand: 600},
		{Name: "cache", Image: "redis:7-alpine", Replicas: 2,
			Resources: state.Resources{CPU: 250, Memory: 512}, SimDemand: 150},
		{Name: "worker", Image: "busybox:1.36", Command: []string{"sleep", "86400"}, Replicas: 3,
			Resources: state.Resources{CPU: 500, Memory: 256}, SimDemand: 900},
	}
}

func waitForLeader(sup *demo.Supervisor, d time.Duration) (string, error) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if l := sup.Leader(); l != "" {
			return l, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return "", fmt.Errorf("no leader after %s", d)
}

func postJSON(url string, v any) error {
	b, _ := json.Marshal(v)
	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s: %s", url, resp.Status)
	}
	return nil
}

func runDemo(args []string) error {
	fs := flag.NewFlagSet("demo", flag.ExitOnError)
	cfg := demoConfig(fs)
	port := fs.Int("port", 9000, "dashboard port")
	seed := fs.Bool("seed", true, "deploy sample services once a leader is elected")
	fs.Parse(args)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg.Logger = logger
	sup, err := demo.New(*cfg)
	if err != nil {
		return err
	}
	lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", *port))
	if err != nil {
		return err
	}
	if err := sup.Start(); err != nil {
		return err
	}
	srv := &http.Server{Handler: sup.Handler(web.Handler()), ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(lis) }()

	leader, err := waitForLeader(sup, 15*time.Second)
	if err != nil {
		sup.Shutdown()
		return err
	}
	logger.Info("cluster up", "leader", leader, "nodes", cfg.Nodes)
	if *seed {
		// Give every agent a heartbeat or two to register before deploying,
		// so the first placement sees the whole cluster.
		time.Sleep(1500 * time.Millisecond)
		for _, s := range sampleServices() {
			api, _ := sup.API()
			if err := postJSON("http://"+api+"/api/services", s); err != nil {
				logger.Warn("seeding", "service", s.Name, "err", err)
			}
		}
	}
	fmt.Printf("\n  NebulaOS demo running — %d nodes, runtime=%s\n  Dashboard:  http://127.0.0.1:%d\n  CLI:        nebula ctl --addr http://127.0.0.1:%d status\n\n",
		cfg.Nodes, cfg.Runtime, *port, *port)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	fmt.Println("shutting down cluster...")
	_ = srv.Close()
	sup.Shutdown()
	return nil
}
