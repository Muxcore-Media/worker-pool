package workerpool

import (
	"math/rand"

	"github.com/Muxcore-Media/core/pkg/contracts"
)

// selectNode picks the best node for a task from the available cluster members.
//
// Selection rules:
//  1. A node must have labels matching every capability the task requires.
//     For example, a task with Capabilities=["gpu"] must land on a node whose
//     Labels map contains a non-empty value for the "gpu" key.
//  2. If no capabilities are required, every node is eligible.
//  3. Among eligible nodes one is chosen at random. (Metrics-based selection
//     such as least-loaded-first can be added in a future iteration.)
//
// Returns nil when no capable node is found.
func (m *Module) selectNode(task contracts.WorkerTask, nodes []contracts.NodeInfo) *contracts.NodeInfo {
	var capable []contracts.NodeInfo
	for _, node := range nodes {
		if nodeSupportsCapabilities(node, task.Capabilities) {
			capable = append(capable, node)
		}
	}
	if len(capable) == 0 {
		return nil
	}
	return &capable[rand.Intn(len(capable))]
}

// nodeSupportsCapabilities checks whether every capability in the required set
// is present as a non-empty label on the node. When no capabilities are
// required every node qualifies.
func nodeSupportsCapabilities(node contracts.NodeInfo, capabilities []string) bool {
	if len(capabilities) == 0 {
		return true
	}
	for _, cap := range capabilities {
		val, ok := node.Labels[cap]
		if !ok || val == "" {
			return false
		}
	}
	return true
}
