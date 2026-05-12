// Command dropline is a single-binary network diagnostic combining ICMP path
// tracing with a UDP loss test against the same target. See spec for details.
package main

import (
	"fmt"
	"os"
)

const usage = `dropline — concurrent path tracing and packet-loss tester

Usage:
  dropline serve [--listen :5301] [--max-sessions 4]
                 [--max-rate-bps 1G] [--allow-reverse-stream]
  dropline trace TARGET [--rate 10M] [--duration 60s] [--packet-size 1200]
                        [--mtr-interval 1s] [--max-hops 30]
                        [--tui | --report | --json] [--save FILE]
                        [--reverse-trace auto|on|off]
                        [--reverse-stream auto|on|off]
                        [--tcp-corroborate auto|on|off]
                        [--tcp-corroborate-rate 100K]
                        [--tcp-hop-probe auto|on|off]
                        [--tcp-hop-probe-port 443]
  dropline service install [--listen :5301] [--max-sessions 4]
                           [--max-rate-bps 1G] [--allow-reverse-stream]
  dropline service uninstall
  dropline service run     (invoked by the Windows SCM; not for direct use)
  dropline version
`

var version = "0.0.0-dev"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	switch os.Args[1] {
	case "serve":
		runServe(os.Args[2:])
	case "trace":
		runTrace(os.Args[2:])
	case "service":
		runService(os.Args[2:])
	case "version", "--version", "-v":
		fmt.Println(version)
	case "help", "--help", "-h":
		fmt.Print(usage)
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: %s\n\n", os.Args[1])
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
}
