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

type ResourceType int32

const (
	CPU    ResourceType = 0
	MEMORY ResourceType = 64
)

type DRFResource struct {
	amount       int64
	resourceType ResourceType
}

type DRFNode struct {
	nodeId            string
	taskId            string
	serviceKey        string
	dominantReserved  DRFResource
	dominantAvailable DRFResource
}

func drfResource(nodeInfo NodeInfo, task api.Task) (taskReserved, nodeAvailable DRFResource) {
	reservations := taskReservations(task.Spec)
	available := nodeInfo.AvailableResources
	if big.NewRat(reservations.MemoryBytes, available.MemoryBytes).Cmp(big.NewRat(reservations.NanoCPUs, available.NanoCPUs)) > 0 {
		taskReserved = DRFResource{reservations.MemoryBytes, MEMORY}
		nodeAvailable = DRFResource{available.MemoryBytes, MEMORY}
	} else {
		taskReserved = DRFResource{reservations.NanoCPUs, CPU}
		nodeAvailable = DRFResource{available.NanoCPUs, CPU}
	}
	return
}

func newDRFNode(node NodeInfo, serviceKey string, task api.Task) *DRFNode {
	drfNode := &DRFNode{}
	drfNode.nodeId = node.ID
	drfNode.taskId = task.ID
	drfNode.serviceKey = serviceKey
	drfNode.dominantReserved, drfNode.dominantAvailable = drfResource(node, task)
	return drfNode
}

func newDRFNodes(node NodeInfo, serviceKey string, tasks map[string]*api.Task) []DRFNode {
	nodes := make([]DRFNode, len(tasks))
	taskList := make([]*api.Task, 0)
	for _, t := range tasks {
		taskList = append(taskList, t)
	}
	for index := range nodes {
		nodes[index] = DRFNode{}
		nodes[index].nodeId = node.ID
		nodes[index].taskId = taskList[index].ID
		nodes[index].serviceKey = serviceKey
		nodes[index].dominantReserved, nodes[index].dominantAvailable = drfResource(node, *taskList[index])
	}
	return nodes
}

func (node *DRFNode) Available() DRFResource {
	return node.dominantAvailable
}

func (node *DRFNode) Reserved() DRFResource {
	return node.dominantReserved
}

// build a min heap for drf algorithm, which is based on max-min fairness
type nodeDRFHeap struct {
	nodes           []DRFNode
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
	drfLess := func(ni, nj *DRFNode) bool {
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
				coherenceI, coherenceJ := getFactor(factorKeysI, ni.nodeId), getFactor(factorKeysJ, nj.nodeId)
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
				return strings.Compare(ni.nodeId, nj.nodeId) < 0
			}
			return leftI < leftJ
		} else if typeI == CPU {
			return true
		} else {
			return false
		}
	}
	return drfLess(&h.nodes[i], &h.nodes[j])
}

func (h *nodeDRFHeap) Push(x interface{}) {
	h.nodes = append(h.nodes, x.(DRFNode))
}

func (h *nodeDRFHeap) Pop() interface{} {
	length := len(h.nodes) - 1
	finest := h.nodes[length]
	h.nodes = h.nodes[:length]
	return finest
}

// for test
func (h *nodeDRFHeap) Prepare(nodes []NodeInfo, tasks []api.Task, meetsConstraints func(*NodeInfo) bool) {
	//size := len(tasks)
	h.nodes = make([]DRFNode, 0)
	for _, node := range nodes {
		for _, task := range tasks {
			if meetsConstraints(&node) {
				h.nodes = append(h.nodes, *newDRFNode(node, GetTaskGroupKey(&task), task))
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
