package scheduler

import (
	"testing"

	"container/heap"
	"fmt"
	"github.com/docker/docker/pkg/testutil/assert"
	"github.com/docker/swarmkit/api"
	"math/big"
	"strings"
	"time"
)

func initWorkset(nodeSize, taskSize, serviceSize int) (nodes map[string]NodeInfo, tasks map[string]*api.Task, services []string) {
	tasks = make(map[string]*api.Task)
	nodes = make(map[string]NodeInfo)
	ns := make([]*api.Node, nodeSize)
	for index := range ns {
		ns[index] = &api.Node{ID: fmt.Sprintf("node%v", index+1)}
	}

	taskList := make([]*api.Task, taskSize)
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
	services = make([]string, serviceSize)
	for index := range services {
		services[index] = fmt.Sprintf("service%v", index+1)
	}
	tIds := make([]string, taskSize)
	for index := range tIds {
		tIds[index] = fmt.Sprintf("task%v", index+1)
	}

	for index, t := range taskList {
		t.ID = tIds[index]
		t.ServiceID = services[index%serviceSize]
		tasks[t.ID] = t
	}
	can := true
	for _, n := range ns {
		if can {
			nodes[n.ID] = newNodeInfo(n, nil, api.Resources{
				NanoCPUs:    1e10,
				MemoryBytes: 1.9e8,
			})
			can = false
		} else {
			nodes[n.ID] = newNodeInfo(n, nil, api.Resources{
				NanoCPUs:    1e10,
				MemoryBytes: 9e7,
			})
		}
	}
	return
}

func TestDRFHeap(t *testing.T) {
	nodeSize, tSize, sSize := 4, 10, 3
	nodes, tasks, services := initWorkset(nodeSize, tSize, sSize)

	tasks["task7"].ServiceID = "service3"
	tasks["task8"].ServiceID = "service3"
	tasks["task10"].ServiceID = "service3"

	taskGroups := make(map[string]map[string]*api.Task)
	for _, t := range tasks {
		if _, ok := taskGroups[t.ServiceID]; !ok {
			taskGroups[t.ServiceID] = make(map[string]*api.Task)
		}
		taskGroups[t.ServiceID][t.ID] = t
	}

	toAllocReplicas := make(map[string]int)
	for _, s := range services {
		toAllocReplicas[s] = 2
	}

	serviceReplicas := make(map[string]map[string]int)
	mapping := make(map[string]map[string]int)

	var drfHeap nodeDRFHeap
	drfHeap.toAllocReplicas = &toAllocReplicas
	drfHeap.serviceReplicas = &serviceReplicas
	drfHeap.coherenceMapping = &mapping
	drfHeap.drfLess = func(ni, nj drfNode, h nodeDRFHeap) bool {
		// replica compare, services with less replicas first
		toReplicas := *h.toAllocReplicas
		if toReplicas != nil {
			if toReplicas[ni.serviceID] != toReplicas[nj.serviceID] {
				return toReplicas[ni.serviceID] > toReplicas[nj.serviceID]
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
			} else {
				if okI && !okJ {
					return true
				} else if !okI && okJ {
					return false
				}
			}
		}

		mapping := *h.coherenceMapping
		if _, exists := mapping[ni.serviceID]; !exists {
			mapping[ni.serviceID] = make(map[string]int)
		}
		if _, exists := mapping[nj.serviceID]; !exists {
			mapping[nj.serviceID] = make(map[string]int)
		}
		okI, okJ := false, false
		okI2, okJ2 := false, false
		if ni.serviceID == "service3" {
			_, okI = mapping["service1"][ni.nodeID]
		}
		if nj.serviceID == "service3" {
			_, okJ = mapping["service1"][nj.nodeID]
		}
		if ni.serviceID == "service1" {
			_, okI2 = mapping["service3"][ni.nodeID]
		}
		if nj.serviceID == "service1" {
			_, okJ2 = mapping["service3"][nj.nodeID]
		}
		_, sI := mapping[ni.serviceID][ni.nodeID]
		_, sJ := mapping[nj.serviceID][nj.nodeID]
		okI = okI || sI || okI2
		okJ = okJ || sJ || okJ2
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
					if fittest == nil || drfHeap.drfLess(*n, *fittest, drfHeap) {
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

func initRunningWorkset() (tasks map[string]*api.Task, services []string, nodes map[string]NodeInfo) {
	nodeSize, taskSize, serviceSize := 4, 10, 3
	tasks = make(map[string]*api.Task)
	nodes = make(map[string]NodeInfo)
	nds := make(map[string]*NodeInfo)
	ns := make([]*api.Node, nodeSize)
	for index := range ns {
		ns[index] = &api.Node{ID: fmt.Sprintf("node%v", index+1)}
	}

	taskList := make([]*api.Task, taskSize)
	for index := range taskList {
		taskList[index] = &api.Task{
			Slot: uint64(index + 1),
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
	services = make([]string, serviceSize)
	for index := range services {
		services[index] = fmt.Sprintf("service%v", index+1)
	}
	tIds := make([]string, taskSize)
	for index := range tIds {
		tIds[index] = fmt.Sprintf("task%v", index+1)
	}

	can := true
	for _, n := range ns {
		if can {
			newNode := newNodeInfo(n, make(map[string]*api.Task), api.Resources{
				NanoCPUs:    1e10,
				MemoryBytes: 1.9e8,
			})
			nds[n.ID] = &newNode
			can = false
		} else {
			newNode := newNodeInfo(n, make(map[string]*api.Task), api.Resources{
				NanoCPUs:    1e10,
				MemoryBytes: 9e7,
			})
			nds[n.ID] = &newNode
		}
	}

	for index, t := range taskList {
		t.ID = tIds[index]
		if index > (2 * serviceSize) {
			t.ServiceID = services[0]
			t.NodeID = ns[0].ID
		} else {
			t.ServiceID = services[index%serviceSize]
			t.NodeID = ns[index%nodeSize].ID
		}
		nds[t.NodeID].Tasks[t.ID] = t
		nds[t.NodeID].AvailableResources.NanoCPUs -= t.Spec.Resources.Reservations.NanoCPUs
		nds[t.NodeID].AvailableResources.MemoryBytes -= t.Spec.Resources.Reservations.MemoryBytes
		tasks[t.ID] = t
	}

	for ID, n := range nds {
		nodes[ID] = *n
	}

	return
}

func scaleDown(serviceID string, required uint64, serviceReplicas map[string]map[string]int, replicas map[string]int, nodeSet map[string]NodeInfo) (slots map[uint64]struct{}) {
	removeCandidates := make(map[uint64]struct{})
	slots = make(map[uint64]struct{})

	counter := int(required)
	servReplicas := serviceReplicas[serviceID]
	replica := len(servReplicas)
	serviceReplica := replicas[serviceID]
	slotsMap := make(map[uint64]int)
	scaleCandidates := make(map[string]*api.Task)
	instancesNodes := make(map[string]int)
	replicaNodes := make(map[string]NodeInfo)
	for nodeID := range servReplicas {
		if node, ok := nodeSet[nodeID]; ok {
			replicaNodes[nodeID] = node
			instancesNodes[nodeID] = servReplicas[nodeID]
			for taskID, task := range node.Tasks {
				if task.ServiceID == serviceID {
					slotsMap[task.Slot]++
					scaleCandidates[taskID] = task
				}
			}
		}
	}
	initDrfMaxHeap := func() *nodeDRFHeap {
		drfHeap := &nodeDRFHeap{}
		drfHeap.drfLess = func(nj, ni drfNode, h nodeDRFHeap) bool {
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
		return drfHeap
	}
	drfHeap := initDrfMaxHeap()
	for {
		if counter == len(removeCandidates) {
			break
		}
		// init drf heap
		drfHeap.nodes = make([]drfNode, 0)
		var fittest *drfNode
		for _, task := range scaleCandidates {
			// filter nodes
			n := newMaxDRFNode(replicaNodes[task.NodeID], task.ServiceID, task)
			if fittest == nil || drfHeap.drfLess(*n, *fittest, *drfHeap) {
				fittest = n
			}
		}

		if fittest == nil {
			break
		}

		if instancesNodes[fittest.nodeID] == 1 {
			if replica == serviceReplica {
				slot := scaleCandidates[fittest.taskID].Slot
				slotsMap[slot]--
				if slotsMap[slot] == 0 {
					delete(slotsMap, slot)
				}
				delete(scaleCandidates, fittest.taskID)
				continue
			} else {
				replica--
			}
		}

		nodeID, taskID := fittest.nodeID, fittest.taskID
		slot := scaleCandidates[taskID].Slot
		removeCandidates[slot] = struct{}{}
		delete(slotsMap, slot)
		delete(scaleCandidates, taskID)
		instancesNodes[nodeID]--
	}

	if len(removeCandidates) == 0 {
		return nil
	}
	for slot := range removeCandidates {
		slots[slot] = struct{}{}
	}
	return
}

func TestScaleDown(t *testing.T) {
	_, _, nodes := initRunningWorkset()
	//for _, n := range nodes {
	//	srv := make(map[string]map[string]int)
	//	for i, t := range n.Tasks {
	//		if _, ok := srv[t.ServiceID]; !ok {
	//			srv[t.ServiceID] = make(map[string]int)
	//		}
	//		srv[t.ServiceID][i] = 1
	//	}
	//	fmt.Printf("tasks %v \n", srv)
	//	fmt.Printf("node %v \n", n.ID)
	//}

	serviceReplicas := make(map[string]map[string]int)
	replicas := make(map[string]int)

	for _, n := range nodes {
		for _, t := range n.Tasks {
			if _, ok := serviceReplicas[t.ServiceID]; !ok {
				serviceReplicas[t.ServiceID] = make(map[string]int)
			}
			serviceReplicas[t.ServiceID][t.NodeID]++
			if _, ok := replicas[t.ServiceID]; !ok {
				replicas[t.ServiceID] = int(t.Spec.Placement.Preferences[0].GetImage().ReplicaDescriptor)
			}
		}
	}
	slots := scaleDown("service1", 3, serviceReplicas, replicas, nodes)
	assert.Equal(t, len(slots), 3)
	slots = scaleDown("service1", 4, serviceReplicas, replicas, nodes)
	assert.Equal(t, len(slots), 4)
	slots = scaleDown("service1", 5, serviceReplicas, replicas, nodes)
	assert.Equal(t, len(slots), 4)
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
	drfHeap.drfLess = func(ni, nj drfNode, h nodeDRFHeap) bool {
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
					} else if drfHeap.drfLess(*min, *n, *drfHeap) {
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
