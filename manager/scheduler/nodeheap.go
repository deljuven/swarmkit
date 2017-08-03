package scheduler

import (
	"math/big"

	"github.com/docker/swarmkit/api"
)

type nodeMaxHeap struct {
	nodes    []NodeInfo
	lessFunc func(*NodeInfo, *NodeInfo) bool
	length   int
}

func (h nodeMaxHeap) Len() int {
	return h.length
}

func (h nodeMaxHeap) Swap(i, j int) {
	h.nodes[i], h.nodes[j] = h.nodes[j], h.nodes[i]
}

func (h nodeMaxHeap) Less(i, j int) bool {
	// reversed to make a max-heap
	return h.lessFunc(&h.nodes[j], &h.nodes[i])
}

func (h *nodeMaxHeap) Push(x interface{}) {
	h.nodes = append(h.nodes, x.(NodeInfo))
	h.length++
}

func (h *nodeMaxHeap) Pop() interface{} {
	h.length--
	// return value is never used
	return nil
}

type resourceType int32

const (
	cpu    resourceType = 0
	memory resourceType = 64
)

type drfResource struct {
	amount       int64
	resourceType resourceType
}

type drfNode struct {
	nodeID            string
	taskID            string
	serviceID         string
	key               string
	dominantReserved  drfResource
	dominantAvailable drfResource
}

func getMaxDrfResource(nodeInfo NodeInfo, task *api.Task) (taskReserved, nodeAvailable drfResource) {
	reservations := taskReservations(task.Spec)
	available := nodeInfo.AvailableResources
	available.NanoCPUs += reservations.NanoCPUs
	available.MemoryBytes += reservations.MemoryBytes
	if big.NewRat(reservations.MemoryBytes, available.MemoryBytes).Cmp(big.NewRat(reservations.NanoCPUs, available.NanoCPUs)) > 0 {
		taskReserved = drfResource{reservations.MemoryBytes, memory}
		nodeAvailable = drfResource{available.MemoryBytes, memory}
	} else {
		taskReserved = drfResource{reservations.NanoCPUs, cpu}
		nodeAvailable = drfResource{available.NanoCPUs, cpu}
	}
	return
}

func getDrfResource(nodeInfo NodeInfo, task *api.Task) (taskReserved, nodeAvailable drfResource) {
	reservations := taskReservations(task.Spec)
	available := nodeInfo.AvailableResources
	if big.NewRat(reservations.MemoryBytes, available.MemoryBytes).Cmp(big.NewRat(reservations.NanoCPUs, available.NanoCPUs)) > 0 {
		taskReserved = drfResource{reservations.MemoryBytes, memory}
		nodeAvailable = drfResource{available.MemoryBytes, memory}
	} else {
		taskReserved = drfResource{reservations.NanoCPUs, cpu}
		nodeAvailable = drfResource{available.NanoCPUs, cpu}
	}
	return
}

func newDRFNode(node NodeInfo, serviceID string, task *api.Task) *drfNode {
	drfNode := &drfNode{}
	drfNode.nodeID = node.ID
	drfNode.taskID = task.ID
	drfNode.serviceID = serviceID
	switch SupportFlag {
	case ServiceBased:
		drfNode.key = serviceID
	case ImageBased:
		fallthrough
	case RootfsBased:
		drfNode.key = task.Spec.GetContainer().Image
	}
	drfNode.dominantReserved, drfNode.dominantAvailable = getDrfResource(node, task)
	return drfNode
}

func newMaxDRFNode(node NodeInfo, serviceID string, task *api.Task) *drfNode {
	drfNode := &drfNode{}
	drfNode.nodeID = node.ID
	drfNode.taskID = task.ID
	drfNode.serviceID = serviceID
	switch SupportFlag {
	case ServiceBased:
		drfNode.key = serviceID
	case ImageBased:
		fallthrough
	case RootfsBased:
		drfNode.key = task.Spec.GetContainer().Image
	}
	drfNode.dominantReserved, drfNode.dominantAvailable = getMaxDrfResource(node, task)
	return drfNode
}

// build a min heap for drf algorithm, which is based on max-top fairness
type nodeDRFHeap struct {
	nodes           []drfNode
	toAllocReplicas *map[string]int
	serviceReplicas *map[string]map[string]int
	// coherence factor mapping, mapping from service to node or image to node or rootfs to node
	coherenceMapping *map[string]map[string]int
	// coherence key mapping, mapping from service to service or image or fs chain
	factorKeyMapping *map[string][]string
	drfLess          func(drfNode, drfNode, nodeDRFHeap) bool
}
