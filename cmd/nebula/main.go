// Command nebula is the single NebulaOS binary.
//
//	nebula node   run a control-plane member and/or worker
//	nebula ctl    talk to a cluster's HTTP API
//	nebula demo   launch a local multi-node cluster with the dashboard
//	nebula bench  measure scheduling and failure recovery
package main

import (
	"fmt"
	"os"
)

const usage = `NebulaOS — a self-healing distributed cloud orchestrator

Usage:
  nebula node  [flags]   run a node (see: nebula node -h)
  nebula ctl   <command> manage a cluster (see: nebula ctl help)
  nebula demo  [flags]   run a local N-node cluster + dashboard
  nebula bench [flags]   benchmark scheduling and failure recovery
  nebula version
`

var version = "0.1.0"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	args := os.Args[2:]
	var err error
	switch os.Args[1] {
	case "node":
		err = runNode(args)
	case "ctl":
		err = runCtl(args)
	case "demo":
		err = runDemo(args)
	case "bench":
		err = runBench(args)
	case "version", "--version", "-v":
		fmt.Println("nebula", version)
	case "help", "-h", "--help":
		fmt.Print(usage)
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", os.Args[1], usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
