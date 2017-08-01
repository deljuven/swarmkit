package scheduler

import (
	"testing"

	"container/heap"
	"fmt"
	"github.com/docker/swarmkit/api"
	"math/big"
	"strings"
	"time"
)

func TestDRFHeap(t *testing.T) {
	nodeSize := 4
	ns := make([]*api.Node, nodeSize)
	for index := range ns {
		ns[index] = &api.Node{ID: fmt.Sprintf("node%v", index+1)}
	}

	tSize := 6
	sSize := 2
	taskList := make([]*api.Task, tSize)
	for index := range taskList {
		taskList[index] = &api.Task{
			Spec: api.TaskSpec{
				Runtime: &api.TaskSpec_Container{
					Container: &api.ContainerSpec{
						Image: "alpine",
					},
				},
				Resources: &api.ResourceRequirements{
					Reservations: &api.Resources{
						NanoCPUs:    1e9,
						MemoryBytes: 2e7,
					},
				},
				Placement: &api.Placement{
					Preferences: []*api.PlacementPreference{
						{
							Preference: &api.PlacementPreference_Image{
								Image: &api.ImageDependency{
									ReplicaDescriptor: 2,
								},
							},
						},
					},
				},
			},
		}
	}
	sIds := make([]string, sSize)
	for index := range sIds {
		sIds[index] = fmt.Sprintf("service%v", index+1)
	}
	tIds := make([]string, tSize)
	for index := range tIds {
		tIds[index] = fmt.Sprintf("task%v", index+1)
	}

	tasks := make(map[string]*api.Task)
	for index, t := range taskList {
		t.ID = tIds[index]
		t.ServiceID = sIds[index%sSize]
		tasks[t.ID] = t
	}

	// nodeInfo has no tasks
	nodes := make(map[string]NodeInfo)
	for _, n := range ns {
		nodes[n.ID] = newNodeInfo(n, nil, api.Resources{
			NanoCPUs:    1e10,
			MemoryBytes: 1e8,
		})
	}

	taskGroups := make(map[string]map[string]*api.Task)
	for _, t := range tasks {
		if _, ok := taskGroups[t.ServiceID]; !ok {
			taskGroups[t.ServiceID] = make(map[string]*api.Task)
		}
		taskGroups[t.ServiceID][t.ID] = t
	}

	toAllocReplicas := make(map[string]int)
	for _, s := range sIds {
		toAllocReplicas[s] = 2
	}

	serviceReplicas := make(map[string]map[string]int)
	mapping := make(map[string]map[string]int)

	var drfHeap nodeDRFHeap
	drfHeap.toAllocReplicas = &toAllocReplicas
	drfHeap.serviceReplicas = &serviceReplicas
	drfHeap.coherenceMapping = &mapping
	drfHeap.drfLess = func(ni, nj drfNode, h *nodeDRFHeap) bool {
		// replica compare, services with less replicas first
		toReplicas := *h.toAllocReplicas
		if toReplicas != nil {
			if toReplicas[ni.serviceID] != toReplicas[nj.serviceID] {
				return toReplicas[ni.serviceID] > toReplicas[nj.serviceID]
			}
		}

		// node compare, if replica is filled, node without same service first
		replicas := *h.serviceReplicas
		_, okI := replicas[ni.serviceID][ni.nodeID]
		_, okJ := replicas[nj.serviceID][nj.nodeID]
		if toReplicas[ni.serviceID] > 0 {
			if okI && !okJ {
				return false
			} else if !okI && okJ {
				return true
			}
		}

		mapping := *h.coherenceMapping
		if _, exists := mapping[ni.serviceID]; !exists {
			mapping[ni.serviceID] = make(map[string]int)
		}
		if _, exists := mapping[nj.serviceID]; !exists {
			mapping[nj.serviceID] = make(map[string]int)
		}
		_, okI = mapping[ni.serviceID][ni.nodeID]
		_, okJ = mapping[nj.serviceID][nj.nodeID]
		if okI && !okJ {
			return true
		} else if !okI && okJ {
			return false
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

	result := make(map[string]string)
	for {
		// init drf heap
		drfHeap.nodes = make([]drfNode, 0)
		var fittest *drfNode

		for _, taskGroup := range taskGroups {
			//filter tasks
			var t *api.Task
			for _, t = range taskGroup {
				//specs[servSpec] = t.Spec
				break
			}

			if t == nil {
				continue
			}

			if _, ok := tasks[t.ID]; !ok {
				continue
			}

			// filter nodes
			for _, node := range nodes {
				if node.AvailableResources.MemoryBytes >= t.Spec.Resources.Reservations.MemoryBytes &&
					node.AvailableResources.NanoCPUs >= t.Spec.Resources.Reservations.NanoCPUs {
					n := newDRFNode(node, t.ServiceID, t)
					if fittest == nil || drfHeap.drfLess(*n, *fittest, &drfHeap) {
						fittest = n
					}
				}
				//drfHeap.nodes = append(drfHeap.nodes, newDRFNodes(node, t.ServiceID, taskGroup)...)
			}
		}

		if len(tasks) == 0 || fittest == nil {
			break
		}

		node := nodes[fittest.nodeID]
		task := tasks[fittest.taskID]
		result[fittest.taskID] = fittest.nodeID
		toAllocReplicas[fittest.serviceID]--
		if _, ok := serviceReplicas[fittest.serviceID]; !ok {
			serviceReplicas[fittest.serviceID] = make(map[string]int)
		}
		node.AvailableResources.MemoryBytes -= task.Spec.Resources.Reservations.MemoryBytes
		node.AvailableResources.NanoCPUs -= task.Spec.Resources.Reservations.NanoCPUs
		nodes[fittest.nodeID] = node
		serviceReplicas[fittest.serviceID][fittest.nodeID]++
		mapping[fittest.serviceID][fittest.nodeID]++
		old := taskGroups[fittest.serviceID]
		delete(old, fittest.taskID)
		delete(tasks, fittest.taskID)
		if len(old) == 0 {
			delete(taskGroups, fittest.serviceID)
		}

		fmt.Printf("node %v with task %v for service %v, reserved %v, avail %v\n",
			fittest.nodeID, fittest.taskID, fittest.serviceID, fittest.dominantReserved, fittest.dominantAvailable)
		//expected := newDRFNode(nodeInfo1, task2.ServiceID, task2)
		//assert.Equal(t, *expected, fittest, "should be equal")
	}
}

func TestHugeDRFHeap(t *testing.T) {
	nodeSize := 100
	taskSize := 1000
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
		var min *drfNode
		for _, task := range tasks {
			// filter nodes
			for _, node := range nodeInfos {
				if meets(node, task) {
					n := newDRFNode(*node, task.ServiceID, task)
					if min == nil {
						min = n
					} else if drfHeap.drfLess(*min, *n, drfHeap) {
						min = n
					}
					//drfHeap.nodes = append(drfHeap.nodes, *newDRFNode(*node, task.ServiceID, task))
				}
			}
			if min != nil {
				break
			}
		}

		if min == nil || len(tasks) == 0 {
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
		//pop := drfHeap.top()
		count++
		//t.Logf("consuming node %v with task %v", pop.nodeID, pop.taskID)
		reserved := tasks[min.taskID].Spec.Resources.Reservations
		nodeInfos[min.nodeID].AvailableResources.NanoCPUs -= reserved.NanoCPUs
		nodeInfos[min.nodeID].AvailableResources.MemoryBytes -= reserved.MemoryBytes
		//t.Logf("after consuming, resource are %v", nodeInfos[pop.nodeID].AvailableResources)
		delete(tasks, min.taskID)
	}

}

func TestHugeMaxHeap(t *testing.T) {
	nodeSize := 100
	taskSize := 1000
	nodes := make([]*api.Node, nodeSize)
	nodeInfos := make(map[string]*NodeInfo)
	serviceID := "service1"
	for index := range nodes {
		nodes[index] = &api.Node{ID: fmt.Sprintf("node%v", index)}
		n := newNodeInfo(nodes[index], nil, api.Resources{
			NanoCPUs:    8e9,
			MemoryBytes: 4e7,
		})
		nodeInfos[nodes[index].ID] = &n
	}

	tasks := make(map[string]*api.Task)
	var task *api.Task
	for index := 0; index < taskSize; index++ {
		id := fmt.Sprintf("task%v", index)
		tasks[id] = &api.Task{
			ID:        id,
			ServiceID: serviceID,
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
		if task == nil {
			task = tasks[id]
		}
	}

	meets := func(node *NodeInfo, task *api.Task) bool {
		available, reserved := node.AvailableResources, task.Spec.Resources.Reservations
		if available.NanoCPUs < reserved.NanoCPUs || available.MemoryBytes < reserved.MemoryBytes {
			return false
		}
		return true
	}

	now := time.Now()
	nodeHeap := &nodeMaxHeap{}
	nodeHeap.lessFunc = func(a *NodeInfo, b *NodeInfo) bool {
		// If either node has at least maxFailures recent failures,
		// that's the deciding factor.
		// If either node has at least maxFailures recent failures,
		// that's the deciding factor.
		recentFailuresA := a.countRecentFailures(now, serviceID)
		recentFailuresB := b.countRecentFailures(now, serviceID)

		if recentFailuresA >= maxFailures || recentFailuresB >= maxFailures {
			if recentFailuresA > recentFailuresB {
				return false
			}
			if recentFailuresB > recentFailuresA {
				return true
			}
		}

		tasksByServiceA := a.ActiveTasksCountByService[serviceID]
		tasksByServiceB := b.ActiveTasksCountByService[serviceID]

		if tasksByServiceA < tasksByServiceB {
			return true
		}
		if tasksByServiceA > tasksByServiceB {
			return false
		}

		// Total number of tasks breaks ties.
		return a.ActiveTasksCount < b.ActiveTasksCount
	}
	for _, node := range nodeInfos {
		if meets(node, task) {
			heap.Push(nodeHeap, *node)
		}
	}

	count := 0
	start := time.Now()
	heap.Init(nodeHeap)
	var maxInit, minInit time.Duration = 0, time.Minute
	nodeIter := 0
	for _, task := range tasks {
		node := nodeHeap.nodes[nodeIter%nodeSize]
		nodeIter++
		node.addTask(task)
		//t.Logf("consuming node %v with task %v", pop.nodeID, pop.taskID)
		//t.Logf("after consuming, resource are %v", nodeInfos[pop.nodeID].AvailableResources)
	}
	fin := time.Now()
	t.Logf("current is %v, time passed %v. counts %v, max inits costs %v, min inits costs %v", fin, fin.Sub(start), count, maxInit, minInit)
	return

}
