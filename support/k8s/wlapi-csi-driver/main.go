package main

import (
	"flag"
	"fmt"
	"os"
)

var (
	nodeIDFlag         = flag.String("node-id", "", "Kubernetes Node ID. If unset, the node ID is obtained from the environment (i.e., -node-id-env)")
	nodeIDEnvFlag      = flag.String("node-id-env", "MY_NODE_ID", "Envvar from which to obtain the node ID. Overriden by -node-id.")
	csiSocketPathFlag  = flag.String("csi-socket-path", "/csi/csi.sock", "Path to the CSI socket")
	wlAPISocketDirFlag = flag.String("wlapi-socket-dir", "", "Path to the Workload API socket directory")
)

func main() {
	flag.Parse()

	config := Config{
		NodeID:         getNodeIDFromFlags(),
		WorkloadAPIDir: *wlAPISocketDirFlag,
		CSISocketPath:  *csiSocketPathFlag,
	}
	if err := Run(config); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}

func getNodeIDFromFlags() string {
	nodeID := os.Getenv(*nodeIDEnvFlag)
	if *nodeIDFlag != "" {
		nodeID = *nodeIDFlag
	}
	return nodeID
}
