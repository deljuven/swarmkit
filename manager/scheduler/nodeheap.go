package scheduler

import "github.com/docker/swarmkit/api"
import (
	"math/big"
	"strings"
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
	serviceKey        string
	dominantReserved  drfResource
	dominantAvailable drfResource
}

func getDrfResource(nodeInfo NodeInfo, task api.Task) (taskReserved, nodeAvailable drfResource) {
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

func newDRFNode(node NodeInfo, serviceKey string, task api.Task) *drfNode {
	drfNode := &drfNode{}
	drfNode.nodeID = node.ID
	drfNode.taskID = task.ID
	drfNode.serviceKey = serviceKey
	drfNode.dominantReserved, drfNode.dominantAvailable = getDrfResource(node, task)
	return drfNode
}

func newDRFNodes(node NodeInfo, serviceKey string, tasks map[string]*api.Task) []drfNode {
	nodes := make([]drfNode, len(tasks))
	taskList := make([]*api.Task, 0)
	for _, t := range tasks {
		taskList = append(taskList, t)
	}
	for index := range nodes {
		nodes[index] = drfNode{}
		nodes[index].nodeID = node.ID
		nodes[index].taskID = taskList[index].ID
		nodes[index].serviceKey = serviceKey
		nodes[index].dominantReserved, nodes[index].dominantAvailable = getDrfResource(node, *taskList[index])
	}
	return nodes
}

// build a min heap for drf algorithm, which is based on max-min fairness
type nodeDRFHeap struct {
	nodes           []drfNode
	toAllocReplicas *map[string]int
	// coherence factor mapping, mapping from service to node or image to node or rootfs to node
	coherenceMapping *map[string]map[string]int
	// coherence key mapping, mapping from service to service or image or fs chain
	factorKeyMapping *map[string][]string
}

func (h nodeDRFHeap) Len() int {
	return len(h.nodes)
}

func (h nodeDRFHeap) Swap(i, j int) {
	h.nodes[i], h.nodes[j] = h.nodes[j], h.nodes[i]
}

func (h nodeDRFHeap) Less(i, j int) bool {
	// reversed to make a drf-heap
	drfLess := func(ni, nj *drfNode) bool {
		if h.toAllocReplicas != nil {
			toReplicas := *h.toAllocReplicas
			if toReplicas[ni.serviceKey] != toReplicas[nj.serviceKey] {
				return toReplicas[ni.serviceKey] > toReplicas[nj.serviceKey]
			}
		}

		if h.coherenceMapping != nil {
			coherencesMapping := *h.coherenceMapping
			factorKeysMapping := *h.factorKeyMapping
			//coherenceI, coherenceJ := 0,0
			factorKeysI, okI := factorKeysMapping[ni.serviceKey]
			factorKeysJ, okJ := factorKeysMapping[nj.serviceKey]
			getFactor := func(keys []string, nodeId string) int {
				final := 0
				for _, key := range keys {
					if factor, ok := coherencesMapping[key]; ok {
						if value, ok := factor[nodeId]; ok {
							final += value
						} else {
							break
						}
					} else {
						break
					}
				}
				return final
			}
			if okI && okJ {
				coherenceI, coherenceJ := getFactor(factorKeysI, ni.nodeID), getFactor(factorKeysJ, nj.nodeID)
				if coherenceI != coherenceJ {
					return coherenceI > coherenceJ
				}
			} else if okI {
				return true
			} else if okJ {
				return false
			}
		}

		// drf compare
		reservedI, availableI := ni.dominantReserved, ni.dominantAvailable
		reservedJ, availableJ := nj.dominantReserved, nj.dominantAvailable
		cmp := big.NewRat(reservedI.amount, availableI.amount).Cmp(big.NewRat(reservedJ.amount, availableJ.amount))
		if cmp < 0 {
			return true
		} else if cmp > 0 {
			return false
		}

		leftI, typeI := availableI.amount-reservedI.amount, availableI.resourceType
		leftJ, typeJ := availableJ.amount-reservedJ.amount, availableJ.resourceType
		// drf resource with same type, choose the least left amount; otherwise, choose cpu type
		if typeI == typeJ {
			if leftI == leftJ {
				return strings.Compare(ni.nodeID, nj.nodeID) < 0
			}
			return leftI < leftJ
		} else if typeI == cpu {
			return true
		} else {
			return false
		}
	}
	return drfLess(&h.nodes[i], &h.nodes[j])
}

func (h *nodeDRFHeap) Push(x interface{}) {
	h.nodes = append(h.nodes, x.(drfNode))
}

func (h *nodeDRFHeap) Pop() interface{} {
	length := len(h.nodes) - 1
	finest := h.nodes[length]
	h.nodes = h.nodes[:length]
	return finest
}

// Prepare used for initiation in test
func (h *nodeDRFHeap) Prepare(nodes []NodeInfo, tasks []api.Task, meetsConstraints func(*NodeInfo) bool) {
	//size := len(tasks)
	h.nodes = make([]drfNode, 0)
	for _, node := range nodes {
		for _, task := range tasks {
			if meetsConstraints(&node) {
				h.nodes = append(h.nodes, *newDRFNode(node, getTaskGroupKey(&task), task))
			}
		}
	}
	allocTmp := make(map[string]int)
	h.toAllocReplicas = &allocTmp
	coherenceTmp := make(map[string]map[string]int)
	h.coherenceMapping = &coherenceTmp
	serviceTmp := make(map[string][]string)
	h.factorKeyMapping = &serviceTmp

}
