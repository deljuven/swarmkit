package scheduler

import (
	"testing"

	"container/heap"
	"github.com/docker/swarmkit/api"
	"github.com/stretchr/testify/assert"
)

func TestDRFHeap(t *testing.T) {
	node1 := &api.Node{ID: "node1"}
	node2 := &api.Node{ID: "node2"}

	task1 := api.Task{
		ID: "task1",
		Spec: api.TaskSpec{
			Resources: &api.ResourceRequirements{
				Reservations: &api.Resources{
					NanoCPUs:    1e9,
					MemoryBytes: 3e7,
				},
			},
		},
	}

	task2 := api.Task{
		ID: "task2",
		Spec: api.TaskSpec{
			Resources: &api.ResourceRequirements{
				Reservations: &api.Resources{
					NanoCPUs:    2e9,
					MemoryBytes: 1e7,
				},
			},
		},
	}

	task3 := api.Task{
		ID: "task3",
		Spec: api.TaskSpec{
			Resources: &api.ResourceRequirements{
				Reservations: &api.Resources{
					NanoCPUs:    2e9,
					MemoryBytes: 2e7,
				},
			},
		},
	}

	tasks := []api.Task{task1, task2, task3}

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

	heap.Init(&drfHeap)

	finest := heap.Pop(&drfHeap)

	expected := newDRFNode(nodeInfo1, task2.ServiceID, task2)
	assert.Equal(t, *expected, finest, "should be equal")

}
