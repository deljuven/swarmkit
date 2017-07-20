package scheduler

import (
	"time"

	"github.com/docker/swarmkit/api"
	"github.com/docker/swarmkit/log"
	"github.com/docker/swarmkit/manager/state"
	"github.com/docker/swarmkit/manager/state/store"
	"github.com/docker/swarmkit/protobuf/ptypes"
	"golang.org/x/net/context"
	"container/heap"
)

const (
	// monitorFailures is the lookback period for counting failures of
	// a task to determine if a node is faulty for a particular service.
	monitorFailures = 5 * time.Minute

	// maxFailures is the number of failures within monitorFailures that
	// triggers downweighting of a node in the sorting function.
	maxFailures = 5
)

type schedulingDecision struct {
	old *api.Task
	new *api.Task
}

type Strategy int64

const (
	None Strategy = 0

	SpreadOver Strategy = 64

	ImageBase Strategy = 128

	// flag for rootfs-base(0) or image-based(1) or service-based(2)
	ROOTSF_BASED  = 0
	IMAGE_BASED   = 1
	SERVICE_BASED = 2
	SUPPORT_FLAG  = ROOTSF_BASED
)

// Scheduler assigns tasks to nodes.
type Scheduler struct {
	store           *store.MemoryStore
	unassignedTasks map[string]*api.Task
	// preassignedTasks already have NodeID, need resource validation
	preassignedTasks map[string]*api.Task
	nodeSet          nodeSet
	allTasks         map[string]*api.Task
	pipeline         *Pipeline

	// stopChan signals to the state machine to stop running
	stopChan chan struct{}
	// doneChan is closed when the state machine terminates
	doneChan chan struct{}

	// to alloc task replica mapping to nodes
	toAllocReplicas map[string]int
	// change when task updated, created and deleted
	serviceMapping map[string]map[string]int

	// rootfs to node mapping
	rootfsMapping map[string]map[string]int
	// image to node mapping
	imageMapping map[string]map[string]int

	// mapping from service to service or img or rootfs
	factorKeys map[string][]string
}

// New creates a new scheduler.
func New(store *store.MemoryStore) *Scheduler {
	return &Scheduler{
		store:            store,
		unassignedTasks:  make(map[string]*api.Task),
		preassignedTasks: make(map[string]*api.Task),
		allTasks:         make(map[string]*api.Task),
		stopChan:         make(chan struct{}),
		doneChan:         make(chan struct{}),
		pipeline:         NewPipeline(),
		toAllocReplicas:  make(map[string]int),
		serviceMapping:   make(map[string]map[string]int),
		rootfsMapping:    make(map[string]map[string]int),
		imageMapping:     make(map[string]map[string]int),
		factorKeys:       make(map[string][]string),
	}
}

func (s *Scheduler) setupTasksList(tx store.ReadTx) error {
	tasks, err := store.FindTasks(tx, store.All)
	if err != nil {
		return err
	}

	tasksByNode := make(map[string]map[string]*api.Task)
	for _, t := range tasks {
		// Ignore all tasks that have not reached PENDING
		// state and tasks that no longer consume resources.
		if t.Status.State < api.TaskStatePending || t.Status.State > api.TaskStateRunning {
			continue
		}

		s.allTasks[t.ID] = t
		if t.NodeID == "" {
			s.enqueue(t)
			continue
		}
		// preassigned tasks need to validate resource requirement on corresponding node
		if t.Status.State == api.TaskStatePending {
			s.preassignedTasks[t.ID] = t
			continue
		}

		if tasksByNode[t.NodeID] == nil {
			tasksByNode[t.NodeID] = make(map[string]*api.Task)
		}
		tasksByNode[t.NodeID][t.ID] = t
	}

	if err := s.buildNodeSet(tx, tasksByNode); err != nil {
		return err
	}

	return nil
}

// Run is the scheduler event loop.
func (s *Scheduler) Run(ctx context.Context) error {
	defer close(s.doneChan)

	updates, cancel, err := store.ViewAndWatch(s.store, s.setupTasksList)
	if err != nil {
		log.G(ctx).WithError(err).Errorf("snapshot store update failed")
		return err
	}
	defer cancel()

	// Validate resource for tasks from preassigned tasks
	// do this before other tasks because preassigned tasks like
	// global service should start before other tasks
	s.processPreassignedTasks(ctx)

	// Queue all unassigned tasks before processing changes.
	s.tick(ctx)

	const (
		// commitDebounceGap is the amount of time to wait between
		// commit events to debounce them.
		commitDebounceGap = 50 * time.Millisecond
		// maxLatency is a time limit on the debouncing.
		maxLatency = time.Second
	)
	var (
		debouncingStarted     time.Time
		commitDebounceTimer   *time.Timer
		commitDebounceTimeout <-chan time.Time
	)

	pendingChanges := 0

	schedule := func() {
		if len(s.preassignedTasks) > 0 {
			s.processPreassignedTasks(ctx)
		}
		if pendingChanges > 0 {
			s.tick(ctx)
			pendingChanges = 0
		}
	}

	// Watch for changes.
	for {
		select {
		case event := <-updates:
			switch v := event.(type) {
			case state.EventCreateTask:
				pendingChanges += s.createTask(ctx, v.Task)
			case state.EventUpdateTask:
				pendingChanges += s.updateTask(ctx, v.Task)
			case state.EventDeleteTask:
				s.deleteTask(ctx, v.Task)
			case state.EventCreateNode:
				s.createOrUpdateNode(v.Node)
				pendingChanges++
			case state.EventUpdateNode:
				s.createOrUpdateNode(v.Node)
				pendingChanges++
			case state.EventDeleteNode:
				s.nodeSet.remove(v.Node.ID)
			case state.EventCommit:
				if commitDebounceTimer != nil {
					if time.Since(debouncingStarted) > maxLatency {
						commitDebounceTimer.Stop()
						commitDebounceTimer = nil
						commitDebounceTimeout = nil
						schedule()
					} else {
						commitDebounceTimer.Reset(commitDebounceGap)
					}
				} else {
					commitDebounceTimer = time.NewTimer(commitDebounceGap)
					commitDebounceTimeout = commitDebounceTimer.C
					debouncingStarted = time.Now()
				}
			}
		case <-commitDebounceTimeout:
			schedule()
			commitDebounceTimer = nil
			commitDebounceTimeout = nil
		case <-s.stopChan:
			return nil
		}
	}
}

// Stop causes the scheduler event loop to stop running.
func (s *Scheduler) Stop() {
	close(s.stopChan)
	<-s.doneChan
}

// enqueue queues a task for scheduling.
func (s *Scheduler) enqueue(t *api.Task) {
	s.unassignedTasks[t.ID] = t
}

func (s *Scheduler) createTask(ctx context.Context, t *api.Task) int {
	// Ignore all tasks that have not reached PENDING
	// state, and tasks that no longer consume resources.
	if t.Status.State < api.TaskStatePending || t.Status.State > api.TaskStateRunning {
		return 0
	}

	s.allTasks[t.ID] = t
	if t.NodeID == "" {
		// unassigned task
		s.enqueue(t)
		return 1
	}

	if t.Status.State == api.TaskStatePending {
		s.preassignedTasks[t.ID] = t
		// preassigned tasks do not contribute to running tasks count
		return 0
	}

	nodeInfo, err := s.nodeSet.nodeInfo(t.NodeID)
	if err == nil && nodeInfo.addTask(t) {
		s.nodeSet.updateNode(nodeInfo)
	}

	s.updateRunningServReplicas(nodeInfo, t.ServiceID)

	return 0
}

func (s *Scheduler) updateTask(ctx context.Context, t *api.Task) int {
	// Ignore all tasks that have not reached PENDING
	// state.
	if t.Status.State < api.TaskStatePending {
		return 0
	}

	oldTask := s.allTasks[t.ID]

	// Ignore all tasks that have not reached ALLOCATED
	// state, and tasks that no longer consume resources.
	if t.Status.State > api.TaskStateRunning {
		if oldTask == nil {
			return 1
		}
		s.deleteTask(ctx, oldTask)
		if t.Status.State != oldTask.Status.State &&
			(t.Status.State == api.TaskStateFailed || t.Status.State == api.TaskStateRejected) {
			nodeInfo, err := s.nodeSet.nodeInfo(t.NodeID)
			if err == nil {
				nodeInfo.taskFailed(ctx, t.ServiceID)
				s.nodeSet.updateNode(nodeInfo)
			}
		}
		return 1
	}

	if t.NodeID == "" {
		// unassigned task
		if oldTask != nil {
			s.deleteTask(ctx, oldTask)
		}
		s.allTasks[t.ID] = t
		s.enqueue(t)
		return 1
	}

	if t.Status.State == api.TaskStatePending {
		if oldTask != nil {
			s.deleteTask(ctx, oldTask)
		}
		s.allTasks[t.ID] = t
		s.preassignedTasks[t.ID] = t
		// preassigned tasks do not contribute to running tasks count
		return 0
	}

	s.allTasks[t.ID] = t
	nodeInfo, err := s.nodeSet.nodeInfo(t.NodeID)
	if err == nil && nodeInfo.addTask(t) {
		s.nodeSet.updateNode(nodeInfo)
	}

	s.updateRunningServReplicas(nodeInfo, t.ServiceID)

	return 0
}

func (s *Scheduler) deleteTask(ctx context.Context, t *api.Task) {
	delete(s.allTasks, t.ID)
	delete(s.preassignedTasks, t.ID)
	nodeInfo, err := s.nodeSet.nodeInfo(t.NodeID)
	if err == nil && nodeInfo.removeTask(t) {
		s.nodeSet.updateNode(nodeInfo)
	}
	s.updateRunningServReplicas(nodeInfo, t.ServiceID)
}

func (s *Scheduler) createOrUpdateNode(n *api.Node) {
	nodeInfo, _ := s.nodeSet.nodeInfo(n.ID)
	var resources api.Resources
	if n.Description != nil && n.Description.Resources != nil {
		resources = *n.Description.Resources
		// reconcile resources by looping over all tasks in this node
		for _, task := range nodeInfo.Tasks {
			reservations := taskReservations(task.Spec)
			resources.MemoryBytes -= reservations.MemoryBytes
			resources.NanoCPUs -= reservations.NanoCPUs
		}
	}
	nodeInfo.Node = n
	nodeInfo.AvailableResources = resources
	s.nodeSet.addOrUpdateNode(nodeInfo)
}

func (s *Scheduler) processPreassignedTasks(ctx context.Context) {
	schedulingDecisions := make(map[string]schedulingDecision, len(s.preassignedTasks))
	for _, t := range s.preassignedTasks {
		newT := s.taskFitNode(ctx, t, t.NodeID)
		if newT == nil {
			continue
		}
		schedulingDecisions[t.ID] = schedulingDecision{old: t, new: newT}
	}

	successful, failed := s.applySchedulingDecisions(ctx, schedulingDecisions)

	for _, decision := range successful {
		if decision.new.Status.State == api.TaskStateAssigned {
			delete(s.preassignedTasks, decision.old.ID)
		}
	}
	for _, decision := range failed {
		s.allTasks[decision.old.ID] = decision.old
		nodeInfo, err := s.nodeSet.nodeInfo(decision.new.NodeID)
		if err == nil && nodeInfo.removeTask(decision.new) {
			s.nodeSet.updateNode(nodeInfo)
		}
		s.updateRunningServReplicas(nodeInfo, decision.new.ServiceID)
	}
}

// tick attempts to schedule the queue.
func (s *Scheduler) tick(ctx context.Context) {
	tasksByCommonSpec := make(map[string]map[string]*api.Task)
	schedulingDecisions := make(map[string]schedulingDecision, len(s.unassignedTasks))
	tasksForImageBySpec := make(map[string]map[string]*api.Task)
	taskGroupKeys := make(map[string]string)
	tasks := make(map[string]*api.Task)

	for taskID, t := range s.unassignedTasks {
		if t == nil || t.NodeID != "" {
			// task deleted or already assigned
			delete(s.unassignedTasks, taskID)
			continue
		}
		tasks[taskID] = t

		// Group common tasks with common specs by marshalling the spec
		// into taskKey and using it as a map key.
		// TODO(aaronl): Once specs are versioned, this will allow a
		// much more efficient fast path.
		taskGroupKey := getTaskGroupKey(t)
		taskGroupKeys[t.ID] = taskGroupKey

		var prefs []*api.PlacementPreference
		if t.Spec.Placement != nil {
			prefs = t.Spec.Placement.Preferences
		}

		strategy := None
		for _, pref := range prefs {
			img := pref.GetImage()
			if img != nil {
				strategy = ImageBase
				s.toAllocReplicas[t.ServiceID] = int(img.ReplicaDescriptor)
			}
		}

		if strategy == ImageBase {
			if tasksForImageBySpec[taskGroupKey] == nil {
				tasksForImageBySpec[taskGroupKey] = make(map[string]*api.Task)
			}
			tasksForImageBySpec[taskGroupKey][t.ID] = t
		} else {
			if tasksByCommonSpec[taskGroupKey] == nil {
				tasksByCommonSpec[taskGroupKey] = make(map[string]*api.Task)
			}
			tasksByCommonSpec[taskGroupKey][taskID] = t
		}
		delete(s.unassignedTasks, taskID)
	}

	// first do service with image base strategy,then spread over strategy
	// for image base, use drf to select suitable nodes, drf return the best fit node per time
	// so for n tasks, there should be n drf calls
	s.scheduleImageBaseTasks(ctx, tasksForImageBySpec, tasks, taskGroupKeys, schedulingDecisions)

	for _, taskGroup := range tasksByCommonSpec {
		s.scheduleTaskGroup(ctx, taskGroup, schedulingDecisions)
	}

	_, failed := s.applySchedulingDecisions(ctx, schedulingDecisions)
	for _, decision := range failed {
		s.allTasks[decision.old.ID] = decision.old

		nodeInfo, err := s.nodeSet.nodeInfo(decision.new.NodeID)
		if err == nil && nodeInfo.removeTask(decision.new) {
			s.nodeSet.updateNode(nodeInfo)
		}
		s.updateRunningServReplicas(nodeInfo, decision.new.ServiceID)

		// enqueue task for next scheduling attempt
		s.enqueue(decision.old)
	}
}

func getTaskGroupKey(t *api.Task) (taskGroupKey string) {
	fieldsToMarshal := api.Task{
		ServiceID: t.ServiceID,
		Spec:      t.Spec,
	}
	marshalled, err := fieldsToMarshal.Marshal()
	if err != nil {
		panic(err)
	}
	taskGroupKey = string(marshalled)
	return
}

func (s *Scheduler) updateFactorKeys(serviceId string, factors []string) {
	if _, ok := s.factorKeys[serviceId]; ok {
		if factors == nil {
			delete(s.factorKeys, serviceId)
			return
		}
	}
	s.factorKeys[serviceId] = factors
}

// update service-node mapping after nodeinfo updating task status
func (s *Scheduler) updateRunningServReplicas(nodeInfo NodeInfo, serviceId string) {
	_, ok := s.serviceMapping[serviceId]
	if nodeInfo.ActiveTasksCountByService[serviceId] > 0 {
		if !ok {
			s.serviceMapping[serviceId] = make(map[string]int)
		}
		if _, ok := s.serviceMapping[serviceId][nodeInfo.ID]; !ok {
			s.serviceMapping[serviceId][nodeInfo.ID] = 1
		}
		if SUPPORT_FLAG == SERVICE_BASED {
			s.updateFactorKeys(serviceId, []string{serviceId})
		}
	} else {
		if ok {
			delete(s.serviceMapping[serviceId], nodeInfo.ID)
			if SUPPORT_FLAG == SERVICE_BASED {
				s.updateFactorKeys(serviceId, nil)
			}
		}
	}
	s.toAllocReplicas[serviceId] = len(s.serviceMapping[serviceId])
}

// TODO image base alloc, should consider dead tasks assignments in preassigned
// serviceReplicas is used for image-based service, indicating number of image replica needed to be scheduled
func (s *Scheduler) scheduleImageBaseTasks(ctx context.Context, taskGroups map[string]map[string]*api.Task, tasks map[string]*api.Task, taskGroupKeys map[string]string, schedulingDecisions map[string]schedulingDecision) int {
	//tasksByImage := make([]*api.Task, 0)
	// cause drf change resource usage during calculation, use nodes to copy the nodeset.nodes
	nodes := make(map[string]NodeInfo)
	for _, nd := range s.nodeSet.nodes {
		nodes[nd.ID] = nd
		for _, taskGroup := range taskGroups {
			var t *api.Task
			for _, t = range taskGroup {
				if _, ok := nd.ActiveTasksCountByService[t.ServiceID]; ok {
					// a mapping from image-based service to nodeslot counting
					// TODO later use image-node mapping, nodeImages , to replace this constraint
					// use service-node mapping at this time
					if _, ok := s.serviceMapping[t.ServiceID]; !ok {
						s.serviceMapping[t.ServiceID] = make(map[string]int)
					}
					s.serviceMapping[t.ServiceID][nd.ID] = 1
				}
				break
			}
		}
	}

	for serviceID := range s.toAllocReplicas {
		if servReplica, ok := s.serviceMapping[serviceID]; ok {
			s.toAllocReplicas[serviceID] -= len(servReplica)
		}
		if s.toAllocReplicas[serviceID] < 0 {
			s.toAllocReplicas[serviceID] = 0
		}
	}

	specs := make(map[string]api.TaskSpec)
	taskScheduled := 0

	drfHeap := nodeDRFHeap{}
	drfHeap.toAllocReplicas = &s.toAllocReplicas
	drfHeap.factorKeyMapping = &s.factorKeys
	switch SUPPORT_FLAG {
	case ROOTSF_BASED:
		drfHeap.coherenceMapping = &s.rootfsMapping
	case IMAGE_BASED:
		drfHeap.coherenceMapping = &s.imageMapping
	case SERVICE_BASED:
		drfHeap.coherenceMapping = &s.serviceMapping
	}
	for {
		// init drf heap
		drfHeap.nodes = make([]DRFNode, 0)
		for servSpec, taskGroup := range taskGroups {
			//filter tasks
			var t *api.Task
			for _, t = range taskGroup {
				specs[t.ServiceID] = t.Spec
				break
			}

			if t == nil {
				log.G(ctx).Warnf("no task for taskGroup %v", servSpec)
				continue
			}

			s.pipeline.SetTask(t)

			// filter nodes
			for _, node := range nodes {
				if s.pipeline.Process(&node) {
					drfHeap.nodes = append(drfHeap.nodes, newDRFNodes(node, taskGroup)...)
				}
			}
		}

		if len(tasks) == 0 || drfHeap.Len() == 0 {
			return taskScheduled
		}

		// get drf result and apply it
		heap.Init(&drfHeap)
		fittest, ok := heap.Pop(&drfHeap).(DRFNode)
		if !ok {
			log.G(ctx).Errorf("drf heap failed")
			break
		}

		nodeID, taskID := fittest.nodeId, fittest.taskId
		// Skip tasks which were already scheduled because they ended
		// up in two groups at once.
		if _, exists := schedulingDecisions[taskID]; exists {
			continue
		}

		old := tasks[taskID]
		// update task and node
		log.G(ctx).WithField("task.id", taskID).Debugf("image-based assigning to node %s", nodeID)
		newT := *old
		newT.NodeID = nodeID
		newT.Status = api.TaskStatus{
			State:     api.TaskStateAssigned,
			Timestamp: ptypes.MustTimestampProto(time.Now()),
			Message:   "scheduler assigned task to node",
		}
		s.allTasks[taskID] = &newT

		nodeInfo, err := s.nodeSet.nodeInfo(nodeID)
		if err == nil && nodeInfo.addTask(&newT) {
			s.nodeSet.updateNode(nodeInfo)
			nodes[nodeID] = nodeInfo
		}

		s.updateRunningServReplicas(nodeInfo, old.ServiceID)

		schedulingDecisions[taskID] = schedulingDecision{old: old, new: &newT}
		taskScheduled++

		// remove scheduled task from tasks to rebuild the heap for next iteration
		delete(tasks, taskID)
		oldKey := taskGroupKeys[taskID]
		oldGroup, ok := taskGroups[oldKey]
		if ok {
			if len(oldGroup) == 0 {
				delete(taskGroups, oldKey)
			} else {
				delete(oldGroup, taskID)
			}
		}
	}

	return taskScheduled
}

func (s *Scheduler) applySchedulingDecisions(ctx context.Context, schedulingDecisions map[string]schedulingDecision) (successful, failed []schedulingDecision) {
	if len(schedulingDecisions) == 0 {
		return
	}

	successful = make([]schedulingDecision, 0, len(schedulingDecisions))

	// Apply changes to master store
	applied, err := s.store.Batch(func(batch *store.Batch) error {
		for len(schedulingDecisions) > 0 {
			err := batch.Update(func(tx store.Tx) error {
				// Update exactly one task inside this Update
				// callback.
				for taskID, decision := range schedulingDecisions {
					delete(schedulingDecisions, taskID)

					t := store.GetTask(tx, taskID)
					if t == nil {
						// Task no longer exists
						nodeInfo, err := s.nodeSet.nodeInfo(decision.new.NodeID)
						if err == nil && nodeInfo.removeTask(decision.new) {
							s.nodeSet.updateNode(nodeInfo)
						}
						s.updateRunningServReplicas(nodeInfo, decision.new.ServiceID)
						delete(s.allTasks, decision.old.ID)

						continue
					}

					if t.Status.State == decision.new.Status.State && t.Status.Message == decision.new.Status.Message {
						// No changes, ignore
						continue
					}

					if t.Status.State >= api.TaskStateAssigned {
						nodeInfo, err := s.nodeSet.nodeInfo(decision.new.NodeID)
						if err != nil {
							failed = append(failed, decision)
							continue
						}
						node := store.GetNode(tx, decision.new.NodeID)
						if node == nil || node.Meta.Version != nodeInfo.Meta.Version {
							// node is out of date
							failed = append(failed, decision)
							continue
						}
					}

					if err := store.UpdateTask(tx, decision.new); err != nil {
						log.G(ctx).Debugf("scheduler failed to update task %s; will retry", taskID)
						failed = append(failed, decision)
						continue
					}
					successful = append(successful, decision)
					return nil
				}
				return nil
			})
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		log.G(ctx).WithError(err).Error("scheduler tick transaction failed")
		failed = append(failed, successful[applied:]...)
		successful = successful[:applied]
	}
	return
}

// taskFitNode checks if a node has enough resources to accommodate a task.
func (s *Scheduler) taskFitNode(ctx context.Context, t *api.Task, nodeID string) *api.Task {
	nodeInfo, err := s.nodeSet.nodeInfo(nodeID)
	if err != nil {
		// node does not exist in set (it may have been deleted)
		return nil
	}
	newT := *t
	s.pipeline.SetTask(t)
	if !s.pipeline.Process(&nodeInfo) {
		// this node cannot accommodate this task
		newT.Status.Timestamp = ptypes.MustTimestampProto(time.Now())
		newT.Status.Message = s.pipeline.Explain()
		s.allTasks[t.ID] = &newT

		return &newT
	}
	newT.Status = api.TaskStatus{
		State:     api.TaskStateAssigned,
		Timestamp: ptypes.MustTimestampProto(time.Now()),
		Message:   "scheduler confirmed task can run on preassigned node",
	}
	s.allTasks[t.ID] = &newT

	if nodeInfo.addTask(&newT) {
		s.nodeSet.updateNode(nodeInfo)
	}

	s.updateRunningServReplicas(nodeInfo, t.ServiceID)
	return &newT
}

// scheduleTaskGroup schedules a batch of tasks that are part of the same
// service and share the same version of the spec.
func (s *Scheduler) scheduleTaskGroup(ctx context.Context, taskGroup map[string]*api.Task, schedulingDecisions map[string]schedulingDecision) {
	// Pick at task at random from taskGroup to use for constraint
	// evaluation. It doesn't matter which one we pick because all the
	// tasks in the group are equal in terms of the fields the constraint
	// filters consider.
	var t *api.Task
	for _, t = range taskGroup {
		break
	}

	s.pipeline.SetTask(t)

	now := time.Now()

	nodeLess := func(a *NodeInfo, b *NodeInfo) bool {
		// If either node has at least maxFailures recent failures,
		// that's the deciding factor.
		recentFailuresA := a.countRecentFailures(now, t.ServiceID)
		recentFailuresB := b.countRecentFailures(now, t.ServiceID)

		if recentFailuresA >= maxFailures || recentFailuresB >= maxFailures {
			if recentFailuresA > recentFailuresB {
				return false
			}
			if recentFailuresB > recentFailuresA {
				return true
			}
		}

		tasksByServiceA := a.ActiveTasksCountByService[t.ServiceID]
		tasksByServiceB := b.ActiveTasksCountByService[t.ServiceID]

		if tasksByServiceA < tasksByServiceB {
			return true
		}
		if tasksByServiceA > tasksByServiceB {
			return false
		}

		// Total number of tasks breaks ties.
		return a.ActiveTasksCount < b.ActiveTasksCount
	}

	var prefs []*api.PlacementPreference
	if t.Spec.Placement != nil {
		prefs = t.Spec.Placement.Preferences
	}

	tree := s.nodeSet.tree(t.ServiceID, prefs, len(taskGroup), s.pipeline.Process, nodeLess)
	// meaning ? afaik the subtree is just a filter chain for spread, no sub branch, cause each task group having the same service spec, leading to same sub tree
	s.scheduleNTasksOnSubtree(ctx, len(taskGroup), taskGroup, &tree, schedulingDecisions, nodeLess)
	if len(taskGroup) != 0 {
		s.noSuitableNode(ctx, taskGroup, schedulingDecisions)
	}
}

func (s *Scheduler) scheduleNTasksOnSubtree(ctx context.Context, n int, taskGroup map[string]*api.Task, tree *decisionTree, schedulingDecisions map[string]schedulingDecision, nodeLess func(a *NodeInfo, b *NodeInfo) bool) int {
	if tree.next == nil {
		nodes := tree.orderedNodes(s.pipeline.Process, nodeLess)
		if len(nodes) == 0 {
			return 0
		}

		return s.scheduleNTasksOnNodes(ctx, n, taskGroup, nodes, schedulingDecisions, nodeLess)
	}

	// Walk the tree and figure out how the tasks should be split at each
	// level.
	tasksScheduled := 0
	tasksInUsableBranches := tree.tasks
	var noRoom map[*decisionTree]struct{}

	// Try to make branches even until either all branches are
	// full, or all tasks have been scheduled.
	for tasksScheduled != n && len(noRoom) != len(tree.next) {
		desiredTasksPerBranch := (tasksInUsableBranches + n - tasksScheduled) / (len(tree.next) - len(noRoom))
		remainder := (tasksInUsableBranches + n - tasksScheduled) % (len(tree.next) - len(noRoom))

		for _, subtree := range tree.next {
			if noRoom != nil {
				if _, ok := noRoom[subtree]; ok {
					continue
				}
			}
			subtreeTasks := subtree.tasks
			if subtreeTasks < desiredTasksPerBranch || (subtreeTasks == desiredTasksPerBranch && remainder > 0) {
				tasksToAssign := desiredTasksPerBranch - subtreeTasks
				if remainder > 0 {
					tasksToAssign++
				}
				res := s.scheduleNTasksOnSubtree(ctx, tasksToAssign, taskGroup, subtree, schedulingDecisions, nodeLess)
				if res < tasksToAssign {
					if noRoom == nil {
						noRoom = make(map[*decisionTree]struct{})
					}
					noRoom[subtree] = struct{}{}
					tasksInUsableBranches -= subtreeTasks
				} else if remainder > 0 {
					remainder--
				}
				tasksScheduled += res
			}
		}
	}

	return tasksScheduled
}

func (s *Scheduler) scheduleNTasksOnNodes(ctx context.Context, n int, taskGroup map[string]*api.Task, nodes []NodeInfo, schedulingDecisions map[string]schedulingDecision, nodeLess func(a *NodeInfo, b *NodeInfo) bool) int {
	tasksScheduled := 0
	failedConstraints := make(map[int]bool) // key is index in nodes slice
	nodeIter := 0
	nodeCount := len(nodes)
	for taskID, t := range taskGroup {
		// Skip tasks which were already scheduled because they ended
		// up in two groups at once.
		if _, exists := schedulingDecisions[taskID]; exists {
			continue
		}

		node := &nodes[nodeIter%nodeCount]

		log.G(ctx).WithField("task.id", t.ID).Debugf("assigning to node %s", node.ID)
		newT := *t
		newT.NodeID = node.ID
		newT.Status = api.TaskStatus{
			State:     api.TaskStateAssigned,
			Timestamp: ptypes.MustTimestampProto(time.Now()),
			Message:   "scheduler assigned task to node",
		}
		s.allTasks[t.ID] = &newT

		nodeInfo, err := s.nodeSet.nodeInfo(node.ID)
		if err == nil && nodeInfo.addTask(&newT) {
			s.nodeSet.updateNode(nodeInfo)
			nodes[nodeIter%nodeCount] = nodeInfo
		}

		s.updateRunningServReplicas(nodeInfo, t.ServiceID)

		schedulingDecisions[taskID] = schedulingDecision{old: t, new: &newT}
		delete(taskGroup, taskID)
		tasksScheduled++
		if tasksScheduled == n {
			return tasksScheduled
		}

		if nodeIter+1 < nodeCount {
			// First pass fills the nodes until they have the same
			// number of tasks from this service.
			nextNode := nodes[(nodeIter+1)%nodeCount]
			if nodeLess(&nextNode, &nodeInfo) {
				nodeIter++
			}
		} else {
			// In later passes, we just assign one task at a time
			// to each node that still meets the constraints.
			nodeIter++
		}

		origNodeIter := nodeIter
		for failedConstraints[nodeIter%nodeCount] || !s.pipeline.Process(&nodes[nodeIter%nodeCount]) {
			failedConstraints[nodeIter%nodeCount] = true
			nodeIter++
			if nodeIter-origNodeIter == nodeCount {
				// None of the nodes meet the constraints anymore.
				return tasksScheduled
			}
		}
	}

	return tasksScheduled
}

func (s *Scheduler) noSuitableNode(ctx context.Context, taskGroup map[string]*api.Task, schedulingDecisions map[string]schedulingDecision) {
	explanation := s.pipeline.Explain()
	for _, t := range taskGroup {
		log.G(ctx).WithField("task.id", t.ID).Debug("no suitable node available for task")

		newT := *t
		newT.Status.Timestamp = ptypes.MustTimestampProto(time.Now())
		if explanation != "" {
			newT.Status.Message = "no suitable node (" + explanation + ")"
		} else {
			newT.Status.Message = "no suitable node"
		}
		s.allTasks[t.ID] = &newT
		schedulingDecisions[t.ID] = schedulingDecision{old: t, new: &newT}

		s.enqueue(&newT)
	}
}

func (s *Scheduler) buildNodeSet(tx store.ReadTx, tasksByNode map[string]map[string]*api.Task) error {
	nodes, err := store.FindNodes(tx, store.All)
	if err != nil {
		return err
	}

	s.nodeSet.alloc(len(nodes))

	for _, n := range nodes {
		var resources api.Resources
		if n.Description != nil && n.Description.Resources != nil {
			resources = *n.Description.Resources
		}
		s.nodeSet.addOrUpdateNode(newNodeInfo(n, tasksByNode[n.ID], resources))
	}

	return nil
}
