package scheduler

import (
	"testing"

	"fmt"
	"github.com/docker/swarmkit/api"
	"github.com/stretchr/testify/assert"
	"math/big"
	"strings"
	"time"
)

func TestDRFHeap(t *testing.T) {
	node1 := &api.Node{ID: "node1"}
	node2 := &api.Node{ID: "node2"}

	task1 := &api.Task{
		ID: "task1",
		Spec: api.TaskSpec{
			Runtime: &api.TaskSpec_Container{
				Container: &api.ContainerSpec{
					Image: "alpine",
				},
			},
			Resources: &api.ResourceRequirements{
				Reservations: &api.Resources{
					NanoCPUs:    1e9,
					MemoryBytes: 3e7,
				},
			},
		},
	}

	task2 := &api.Task{
		ID: "task2",
		Spec: api.TaskSpec{
			Runtime: &api.TaskSpec_Container{
				Container: &api.ContainerSpec{
					Image: "alpine",
				},
			},
			Resources: &api.ResourceRequirements{
				Reservations: &api.Resources{
					NanoCPUs:    2e9,
					MemoryBytes: 1e7,
				},
			},
		},
	}

	task3 := &api.Task{
		ID: "task3",
		Spec: api.TaskSpec{
			Runtime: &api.TaskSpec_Container{
				Container: &api.ContainerSpec{
					Image: "alpine",
				},
			},
			Resources: &api.ResourceRequirements{
				Reservations: &api.Resources{
					NanoCPUs:    2e9,
					MemoryBytes: 2e7,
				},
			},
		},
	}

	tasks := []*api.Task{task1, task2, task3}

	// nodeInfo has no tasks
	nodeInfo1 := newNodeInfo(node1, nil, api.Resources{
		NanoCPUs:    8e9,
		MemoryBytes: 4e7,
	})
	nodeInfo2 := newNodeInfo(node2, nil, api.Resources{
		NanoCPUs:    2e9,
		MemoryBytes: 4e7,
	})

	nodes := []NodeInfo{nodeInfo1, nodeInfo2}

	var drfHeap nodeDRFHeap
	drfHeap.Prepare(nodes, tasks, func(node *NodeInfo) bool {
		return true
	})

	//heap.Init(&drfHeap)
	//
	//finest := heap.Pop(&drfHeap)
	drfHeap.drfLess = func(ni, nj drfNode, h *nodeDRFHeap) bool {
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
	finest := drfHeap.top()

	expected := newDRFNode(nodeInfo1, task2.ServiceID, task2)
	assert.Equal(t, *expected, finest, "should be equal")
}

func TestHugeDRFHeap(t *testing.T) {
	nodeSize := 30
	taskSize := 300
	nodes := make([]*api.Node, nodeSize)
	nodeInfos := make(map[string]*NodeInfo)
	for index := range nodes {
		nodes[index] = &api.Node{ID: fmt.Sprintf("node%v", index)}
		n := newNodeInfo(nodes[index], nil, api.Resources{
			NanoCPUs:    8e9,
			MemoryBytes: 4e7,
		})
		nodeInfos[nodes[index].ID] = &n
	}

	tasks := make(map[string]*api.Task)
	for index := 0; index < taskSize; index++ {
		id := fmt.Sprintf("task%v", index)
		tasks[id] = &api.Task{
			ID: id,
			Spec: api.TaskSpec{
				Runtime: &api.TaskSpec_Container{
					Container: &api.ContainerSpec{
						Image: "alpine",
					},
				},
				Resources: &api.ResourceRequirements{
					Reservations: &api.Resources{
						NanoCPUs:    4e8,
						MemoryBytes: 2e6,
					},
				},
			},
		}
	}

	meets := func(node *NodeInfo, task *api.Task) bool {
		available, reserved := node.AvailableResources, task.Spec.Resources.Reservations
		if available.NanoCPUs < reserved.NanoCPUs || available.MemoryBytes < reserved.MemoryBytes {
			return false
		}
		return true
	}

	drfHeap := &nodeDRFHeap{}
	drfHeap.drfLess = func(ni, nj drfNode, h *nodeDRFHeap) bool {
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
	count := 0
	start := time.Now()
	var maxInit, minInit time.Duration = 0, time.Minute
	for {
		pre := time.Now()
		drfHeap.nodes = make([]drfNode, 0)
		for _, task := range tasks {
			// filter nodes
			for _, node := range nodeInfos {
				if meets(node, task) {
					drfHeap.nodes = append(drfHeap.nodes, *newDRFNode(*node, task.ServiceID, task))
				}
			}
		}

		if drfHeap.Len() == 0 {
			fin := time.Now()
			t.Logf("current is %v, time passed %v. counts %v, max inits costs %v, min inits costs %v", fin, fin.Sub(start), count, maxInit, minInit)
			return
		}

		//heap.Init(drfHeap)
		past := time.Now()
		tmp := past.Sub(pre)
		if tmp > maxInit {
			maxInit = tmp
		}
		if tmp < minInit {
			minInit = tmp
		}
		//pop, ok := heap.Pop(drfHeap).(drfNode)
		pop := drfHeap.top()
		count++
		//t.Logf("consuming node %v with task %v", pop.nodeID, pop.taskID)
		reserved := tasks[pop.taskID].Spec.Resources.Reservations
		nodeInfos[pop.nodeID].AvailableResources.NanoCPUs -= reserved.NanoCPUs
		nodeInfos[pop.nodeID].AvailableResources.MemoryBytes -= reserved.MemoryBytes
		//t.Logf("after consuming, resource are %v", nodeInfos[pop.nodeID].AvailableResources)
		delete(tasks, pop.taskID)
	}

}
