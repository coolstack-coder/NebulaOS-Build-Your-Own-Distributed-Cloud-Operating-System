package main

import (
	"flag"
	"fmt"

	"github.com/coolstack-coder/nebulaos/internal/bench"
)

func runBench(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	cfg := demoConfig(fs)
	runs := fs.Int("runs", 5, "repetitions per failure scenario")
	replicas := fs.Int("replicas", 20, "replicas in the benchmark service")
	out := fs.String("out", "", "write a markdown report (and a .json next to it)")
	fs.Parse(args)
	if cfg.DataDir == ".nebula/demo" {
		cfg.DataDir = ".nebula/bench"
	}
	if cfg.GRPCBase == 7700 {
		cfg.GRPCBase, cfg.APIBase = 7800, 8800 // don't collide with a running demo
	}
	rep, err := bench.Run(bench.Config{
		Demo: *cfg, Runs: *runs, Replicas: *replicas,
		Logf: func(f string, a ...any) { fmt.Printf(f+"\n", a...) },
	})
	if err != nil {
		return err
	}
	fmt.Println()
	fmt.Println(rep.Markdown())
	if *out != "" {
		return rep.WriteFiles(*out)
	}
	return nil
}
