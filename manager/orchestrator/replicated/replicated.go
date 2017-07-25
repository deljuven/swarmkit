package replicated

import (
	"github.com/docker/swarmkit/api"
	"github.com/docker/swarmkit/manager/orchestrator/restart"
	"github.com/docker/swarmkit/manager/orchestrator/update"
	"github.com/docker/swarmkit/manager/scheduler"
	"github.com/docker/swarmkit/manager/state"
	"github.com/docker/swarmkit/manager/state/store"
	"golang.org/x/net/context"
)

// An Orchestrator runs a reconciliation loop to create and destroy
// tasks as necessary for the replicated services.
type Orchestrator struct {
	store *store.MemoryStore

	reconcileServices map[string]*api.Service
	restartTasks      map[string]struct{}

	// stopChan signals to the state machine to stop running.
	stopChan chan struct{}
	// doneChan is closed when the state machine terminates.
	doneChan chan struct{}

	updater  *update.Supervisor
	restarts *restart.Supervisor

	cluster *api.Cluster // local cluster instance

	imageQueryReq chan *scheduler.RootfsQueryReq

	scaleDownReq  chan *scheduler.ScaleDownReq
	scaleDownResp chan *scheduler.ScaleDownResp
}

// NewReplicatedOrchestrator creates a new replicated Orchestrator.
func NewReplicatedOrchestrator(store *store.MemoryStore) *Orchestrator {
	restartSupervisor := restart.NewSupervisor(store)
	updater := update.NewSupervisor(store, restartSupervisor)
	return &Orchestrator{
		store:             store,
		stopChan:          make(chan struct{}),
		doneChan:          make(chan struct{}),
		reconcileServices: make(map[string]*api.Service),
		restartTasks:      make(map[string]struct{}),
		updater:           updater,
		restarts:          restartSupervisor,
	}
}

// ImageQueryPrepare inits the query chan shared with agent
func (r *Orchestrator) ImageQueryPrepare(imageQueryReq chan *scheduler.RootfsQueryReq) {
	if scheduler.SupportFlag != scheduler.RootfsBased {
		return
	}
	r.imageQueryReq = imageQueryReq
}

// InitScaleChan inits the scale chan shared with orchestrator
func (r *Orchestrator) InitScaleChan(scaleDownReq chan *scheduler.ScaleDownReq, scaleDownResp chan *scheduler.ScaleDownResp) {
	r.scaleDownReq = scaleDownReq
	r.scaleDownResp = scaleDownResp
}

// SyncRootFSMapping is used to query registry for specified image's rootfs
func (r *Orchestrator) SyncRootFSMapping(ctx context.Context, image string, encodedAuth string) error {
	if scheduler.SupportFlag != scheduler.RootfsBased {
		return nil
	}
	select {
	case r.imageQueryReq <- &scheduler.RootfsQueryReq{
		Image:       image,
		EncodedAuth: encodedAuth,
	}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run contains the orchestrator event loop. It runs until Stop is called.
func (r *Orchestrator) Run(ctx context.Context) error {
	defer close(r.doneChan)

	// Watch changes to services and tasks
	queue := r.store.WatchQueue()
	watcher, cancel := queue.Watch()
	defer cancel()

	// Balance existing services and drain initial tasks attached to invalid
	// nodes
	var err error
	r.store.View(func(readTx store.ReadTx) {
		if err = r.initTasks(ctx, readTx); err != nil {
			return
		}

		if err = r.initServices(ctx, readTx); err != nil {
			return
		}

		if err = r.initCluster(readTx); err != nil {
			return
		}
	})
	if err != nil {
		return err
	}

	go r.HandleScaleDownResp(ctx)

	r.tick(ctx)

	for {
		select {
		case event := <-watcher:
			// TODO(stevvooe): Use ctx to limit running time of operation.
			r.handleTaskEvent(ctx, event)
			r.handleServiceEvent(ctx, event)
			switch v := event.(type) {
			case state.EventCommit:
				r.tick(ctx)
			case state.EventUpdateCluster:
				r.cluster = v.Cluster
			}
		case <-r.stopChan:
			return nil
		}
	}
}

// Stop stops the orchestrator.
func (r *Orchestrator) Stop() {
	close(r.stopChan)
	<-r.doneChan
	r.updater.CancelAll()
	r.restarts.CancelAll()
}

func (r *Orchestrator) tick(ctx context.Context) {
	// tickTasks must be called first, so we respond to task-level changes
	// before performing service reconciliation.
	r.tickTasks(ctx)
	r.tickServices(ctx)
}
