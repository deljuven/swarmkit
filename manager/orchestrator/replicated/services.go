package replicated

import (
	"sort"

	"github.com/docker/go-events"
	"github.com/docker/swarmkit/api"
	"github.com/docker/swarmkit/log"
	"github.com/docker/swarmkit/manager/orchestrator"
	"github.com/docker/swarmkit/manager/scheduler"
	"github.com/docker/swarmkit/manager/state"
	"github.com/docker/swarmkit/manager/state/store"
	"golang.org/x/net/context"
)

// This file provices service-level orchestration. It observes changes to
// services and creates and destroys tasks as necessary to match the service
// specifications. This is different from task-level orchestration, which
// responds to changes in individual tasks (or nodes which run them).

func (r *Orchestrator) initCluster(readTx store.ReadTx) error {
	clusters, err := store.FindClusters(readTx, store.ByName("default"))
	if err != nil {
		return err
	}

	if len(clusters) != 1 {
		// we'll just pick it when it is created.
		return nil
	}

	r.cluster = clusters[0]
	return nil
}

func (r *Orchestrator) initServices(ctx context.Context, readTx store.ReadTx) error {
	services, err := store.FindServices(readTx, store.All)
	if err != nil {
		return err
	}
	imgs := make(map[string]struct{})
	for _, s := range services {
		if orchestrator.IsReplicatedService(s) {
			r.reconcileServices[s.ID] = s
			ctnr := s.Spec.Task.GetContainer()
			if _, ok := imgs[ctnr.Image]; !ok {
				imgs[ctnr.Image] = struct{}{}
				go func() {
					err := r.SyncRootFSMapping(ctx, ctnr.Image, ctnr.PullOptions.RegistryAuth)
					if err != nil {
						log.G(ctx).Errorf("failed to get image layer info for : %v", err)
					}
				}()
			}
		}
	}
	return nil
}

func (r *Orchestrator) handleServiceEvent(ctx context.Context, event events.Event) {
	switch v := event.(type) {
	case state.EventDeleteService:
		if !orchestrator.IsReplicatedService(v.Service) {
			return
		}
		orchestrator.DeleteServiceTasks(ctx, r.store, v.Service)
		r.restarts.ClearServiceHistory(v.Service.ID)
		delete(r.reconcileServices, v.Service.ID)
	case state.EventCreateService:
		if !orchestrator.IsReplicatedService(v.Service) {
			return
		}
		r.reconcileServices[v.Service.ID] = v.Service
		ctnr := v.Service.Spec.Task.GetContainer()
		err := r.SyncRootFSMapping(ctx, ctnr.Image, ctnr.PullOptions.RegistryAuth)
		if err != nil {
			log.G(ctx).Errorf("failed to get image layer info for : %v", err)
		}
	case state.EventUpdateService:
		if !orchestrator.IsReplicatedService(v.Service) {
			return
		}
		r.reconcileServices[v.Service.ID] = v.Service
		ctnr := v.Service.Spec.Task.GetContainer()
		err := r.SyncRootFSMapping(ctx, ctnr.Image, ctnr.PullOptions.RegistryAuth)
		if err != nil {
			log.G(ctx).Errorf("failed to get image layer info for : %v", err)
		}
	}
}

func (r *Orchestrator) tickServices(ctx context.Context) {
	if len(r.reconcileServices) > 0 {
		for _, s := range r.reconcileServices {
			r.reconcile(ctx, s)
		}
		r.reconcileServices = make(map[string]*api.Service)
	}
}

func (r *Orchestrator) resolveService(ctx context.Context, task *api.Task) *api.Service {
	if task.ServiceID == "" {
		return nil
	}
	var service *api.Service
	r.store.View(func(tx store.ReadTx) {
		service = store.GetService(tx, task.ServiceID)
	})
	return service
}

func (r *Orchestrator) reconcile(ctx context.Context, service *api.Service) {
	runningSlots, deadSlots, err := orchestrator.GetRunnableAndDeadSlots(r.store, service.ID)
	if err != nil {
		log.G(ctx).WithError(err).Errorf("reconcile failed finding tasks")
		return
	}

	numSlots := len(runningSlots)

	slotsSlice := make([]orchestrator.Slot, 0, numSlots)
	for _, slot := range runningSlots {
		slotsSlice = append(slotsSlice, slot)
	}

	deploy := service.Spec.GetMode().(*api.ServiceSpec_Replicated)
	specifiedSlots := deploy.Replicated.Replicas

	switch {
	case specifiedSlots > uint64(numSlots):
		log.G(ctx).Debugf("Service %s was scaled up from %d to %d instances", service.ID, numSlots, specifiedSlots)
		// Update all current tasks then add missing tasks
		r.updater.Update(ctx, r.cluster, service, slotsSlice)
		_, err = r.store.Batch(func(batch *store.Batch) error {
			r.addTasks(ctx, batch, service, runningSlots, deadSlots, specifiedSlots-uint64(numSlots))
			r.deleteTasksMap(ctx, batch, deadSlots)
			return nil
		})
		if err != nil {
			log.G(ctx).WithError(err).Errorf("reconcile batch failed")
		}

	case specifiedSlots < uint64(numSlots):
		// Update up to N tasks then remove the extra
		log.G(ctx).Debugf("Service %s was scaled down from %d to %d instances", service.ID, numSlots, specifiedSlots)

		// Preferentially remove tasks on the nodes that have the most
		// copies of this service, to leave a more balanced result.

		// First sort tasks such that tasks which are currently running
		// (in terms of observed state) appear before non-running tasks.
		// This will cause us to prefer to remove non-running tasks, all
		// other things being equal in terms of node balance.

		sort.Sort(slotsByRunningState(slotsSlice))

		// get the service strategy
		strategy := scheduler.None
		if service.Spec.Task.Placement != nil {
			prefs := service.Spec.Task.Placement.Preferences
			for _, pref := range prefs {
				img := pref.GetImage()
				if img != nil {
					strategy = scheduler.ImageBase
				}
				break
			}
		}

		if strategy == scheduler.ImageBase {
			// scale down with image-based, simply reduce slots which not change replica distribution
			// TODO(deljuven) a more complicated policy should be taken later
			go r.getScaleDownPolicy(ctx, service, specifiedSlots)
		} else {
			sortedSlots := make([]orchestrator.Slot, 0, numSlots)
			slotsByNode := make(map[string]int)
			slotsWithIndices := make(slotsByIndex, 0, numSlots)
			for _, slot := range slotsSlice {
				if len(slot) == 1 && slot[0].NodeID != "" {
					slotsByNode[slot[0].NodeID]++
					slotsWithIndices = append(slotsWithIndices, slotWithIndex{slot: slot, index: slotsByNode[slot[0].NodeID]})
				} else {
					slotsWithIndices = append(slotsWithIndices, slotWithIndex{slot: slot, index: -1})
				}
			}

			sort.Sort(slotsWithIndices)

			for _, slot := range slotsWithIndices {
				sortedSlots = append(sortedSlots, slot.slot)
			}

			// Assign each task an index that counts it as the nth copy of
			// of the service on its node (1, 2, 3, ...), and sort the
			// tasks by this counter value.
			r.updater.Update(ctx, r.cluster, service, sortedSlots[:specifiedSlots])
			_, err = r.store.Batch(func(batch *store.Batch) error {
				r.deleteTasksMap(ctx, batch, deadSlots)
				r.deleteTasks(ctx, batch, sortedSlots[specifiedSlots:])
				return nil
			})
			if err != nil {
				log.G(ctx).WithError(err).Errorf("reconcile batch failed")
			}
		}

	case specifiedSlots == uint64(numSlots):
		_, err = r.store.Batch(func(batch *store.Batch) error {
			r.deleteTasksMap(ctx, batch, deadSlots)
			return nil
		})
		if err != nil {
			log.G(ctx).WithError(err).Errorf("reconcile batch failed")
		}
		// Simple update, no scaling - update all tasks.
		r.updater.Update(ctx, r.cluster, service, slotsSlice)
	}
}

func (r *Orchestrator) addTasks(ctx context.Context, batch *store.Batch, service *api.Service, runningSlots map[uint64]orchestrator.Slot, deadSlots map[uint64]orchestrator.Slot, count uint64) {
	slot := uint64(0)
	for i := uint64(0); i < count; i++ {
		// Find a slot number that is missing a running task
		for {
			slot++
			if _, ok := runningSlots[slot]; !ok {
				break
			}
		}

		delete(deadSlots, slot)
		err := batch.Update(func(tx store.Tx) error {
			return store.CreateTask(tx, orchestrator.NewTask(r.cluster, service, slot, ""))
		})
		if err != nil {
			log.G(ctx).Errorf("Failed to create task: %v", err)
		}
	}
}

func (r *Orchestrator) deleteTasks(ctx context.Context, batch *store.Batch, slots []orchestrator.Slot) {
	for _, slot := range slots {
		for _, t := range slot {
			r.deleteTask(ctx, batch, t)
		}
	}
}

func (r *Orchestrator) deleteTasksMap(ctx context.Context, batch *store.Batch, slots map[uint64]orchestrator.Slot) {
	for _, slot := range slots {
		for _, t := range slot {
			r.deleteTask(ctx, batch, t)
		}
	}
}

func (r *Orchestrator) deleteTask(ctx context.Context, batch *store.Batch, t *api.Task) {
	err := batch.Update(func(tx store.Tx) error {
		return store.DeleteTask(tx, t.ID)
	})
	if err != nil {
		log.G(ctx).WithError(err).Errorf("deleting task %s failed", t.ID)
	}
}

// IsRelatedService returns true if the service should be governed by this orchestrator
func (r *Orchestrator) IsRelatedService(service *api.Service) bool {
	return orchestrator.IsReplicatedService(service)
}

// IsRelatedService returns true if the service should be governed by this orchestrator
func (r *Orchestrator) getScaleDownPolicy(ctx context.Context, service *api.Service, required uint64) {
	if r.scaleDownReq == nil {
		log.G(ctx).Info("(*Orchestrator).getScaleDownPolicy is no longer running for chan closed")
		return
	}
	select {
	case r.scaleDownReq <- &scheduler.ScaleDownReq{
		Service:  service,
		Required: required,
	}:
	case <-ctx.Done():
		log.G(ctx).Errorf("(*Orchestrator).HandleScaleDownResp is no longer running for %v", ctx.Err())
		return
	}
}

// HandleScaleDownResp handles scale down policy calculated by scheduler
func (r *Orchestrator) HandleScaleDownResp(ctx context.Context) {
	for {
		if r.scaleDownResp == nil {
			log.G(ctx).Info("(*Orchestrator).HandleScaleDownResp is no longer running for chan closed")
			return
		}
		select {
		case resp, ok := <-r.scaleDownResp:
			if !ok {
				log.G(ctx).Error("(*Orchestrator).HandleScaleDownResp is no longer running for chan is closed")
				return
			}
			service, slots, required := resp.Service, resp.Slots, resp.Required
			if slots == nil || len(slots) == 0 {
				log.G(ctx).Infof("(*Orchestrator).HandleScaleDownResp will not scale down %v for no suitable slots", service.ID)
				return
			}
			if int(required) != len(slots) {
				log.G(ctx).Infof("(*Orchestrator).HandleScaleDownResp scale down %v not completely", service.ID)
			}

			runningSlots, deadSlots, err := orchestrator.GetRunnableAndDeadSlots(r.store, service.ID)
			if err != nil {
				log.G(ctx).WithError(err).Errorf("reconcile failed finding tasks")
				break
			}

			slotsSlice := make([]orchestrator.Slot, 0)
			for index, slot := range runningSlots {
				if _, ok := slots[index]; ok {
					slotsSlice = append(slotsSlice, slot)
				}
			}

			// Assign each task an index that counts it as the nth copy of
			// of the service on its node (1, 2, 3, ...), and sort the
			// tasks by this counter value.
			r.updater.Update(ctx, r.cluster, service, slotsSlice)
			_, err = r.store.Batch(func(batch *store.Batch) error {
				r.deleteTasksMap(ctx, batch, deadSlots)
				r.deleteTasks(ctx, batch, slotsSlice)
				return nil
			})
			if err != nil {
				log.G(ctx).WithError(err).Errorf("(*Orchestrator).HandleScaleDownResp batch update failed")
			}
		case <-ctx.Done():
			log.G(ctx).Errorf("(*Orchestrator).HandleScaleDownResp is no longer running for %v", ctx.Err())
			return
		}
	}
}
