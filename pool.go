package workerpool

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Muxcore-Media/core/pkg/contracts"
	"github.com/google/uuid"
)

// Event types published and consumed by the worker pool.
const (
	EventWorkerTaskAssigned  = "worker.task.assigned"
	EventWorkerTaskStarted   = "worker.task.started"
	EventWorkerTaskCompleted = "worker.task.completed"
	EventWorkerTaskFailed    = "worker.task.failed"
	EventWorkerTaskHeartbeat = "worker.task.heartbeat"
)

const (
	staleTimeout      = 30 * time.Second
	staleCheckInterval = 15 * time.Second
)

// Compile-time interface checks.
var _ contracts.Module = (*Module)(nil)
var _ contracts.WorkerPool = (*Module)(nil)

// Module implements a distributed worker pool with cluster-aware scheduling
// and automatic failover. It satisfies both contracts.Module and
// contracts.WorkerPool.
type Module struct {
	mu     sync.RWMutex
	ledger map[string]contracts.WorkerTask
	deps   contracts.ModuleDeps

	ctx    context.Context
	cancel context.CancelFunc
}

// NewModule creates a new worker pool module.
func NewModule(deps contracts.ModuleDeps) *Module {
	return &Module{
		ledger: make(map[string]contracts.WorkerTask),
		deps:   deps,
	}
}

func init() {
	contracts.Register(func(deps contracts.ModuleDeps) contracts.Module {
		return NewModule(deps)
	})
}

// Info returns the module's metadata.
func (m *Module) Info() contracts.ModuleInfo {
	return contracts.ModuleInfo{
		ID:          "worker-pool",
		Name:        "Worker Pool",
		Version:     "1.0.0",
		Kinds:       []contracts.ModuleKind{contracts.ModuleKindScheduler},
		Description: "Distributed worker pool with cluster-aware scheduling and automatic failover",
		Author:      "Muxcore-Media",
		Capabilities: []string{"worker.pool", "worker.failover"},
	}
}

// Init prepares the module for startup.
func (m *Module) Init(ctx context.Context) error { return nil }

// Start subscribes to cluster and worker events and begins failover monitoring.
func (m *Module) Start(ctx context.Context) error {
	m.ctx, m.cancel = context.WithCancel(ctx)

	// Subscribe to cluster events for failover.
	if err := m.deps.EventBus.Subscribe(ctx, contracts.EventClusterNodeLeft, m.onNodeLeft); err != nil {
		return fmt.Errorf("subscribe to %s: %w", contracts.EventClusterNodeLeft, err)
	}

	// Subscribe to task lifecycle events for status tracking.
	if err := m.deps.EventBus.Subscribe(ctx, EventWorkerTaskStarted, m.onTaskStarted); err != nil {
		return fmt.Errorf("subscribe to %s: %w", EventWorkerTaskStarted, err)
	}
	if err := m.deps.EventBus.Subscribe(ctx, EventWorkerTaskCompleted, m.onTaskCompleted); err != nil {
		return fmt.Errorf("subscribe to %s: %w", EventWorkerTaskCompleted, err)
	}
	if err := m.deps.EventBus.Subscribe(ctx, EventWorkerTaskFailed, m.onTaskFailed); err != nil {
		return fmt.Errorf("subscribe to %s: %w", EventWorkerTaskFailed, err)
	}

	// Subscribe to heartbeats to detect stalled tasks.
	if err := m.deps.EventBus.Subscribe(ctx, EventWorkerTaskHeartbeat, m.onHeartbeat); err != nil {
		return fmt.Errorf("subscribe to %s: %w", EventWorkerTaskHeartbeat, err)
	}

	// Start background stale-task checker.
	go m.staleTaskLoop()

	slog.Info("worker pool started")
	return nil
}

// Stop gracefully shuts down the module and unsubscribes from all events.
func (m *Module) Stop(ctx context.Context) error {
	if m.cancel != nil {
		m.cancel()
	}

	m.deps.EventBus.Unsubscribe(ctx, contracts.EventClusterNodeLeft, m.onNodeLeft)
	m.deps.EventBus.Unsubscribe(ctx, EventWorkerTaskStarted, m.onTaskStarted)
	m.deps.EventBus.Unsubscribe(ctx, EventWorkerTaskCompleted, m.onTaskCompleted)
	m.deps.EventBus.Unsubscribe(ctx, EventWorkerTaskFailed, m.onTaskFailed)
	m.deps.EventBus.Unsubscribe(ctx, EventWorkerTaskHeartbeat, m.onHeartbeat)

	slog.Info("worker pool stopped")
	return nil
}

// Health returns nil while the module is operational.
func (m *Module) Health(ctx context.Context) error { return nil }

// ---------------------------------------------------------------------------
// WorkerPool interface
// ---------------------------------------------------------------------------

// Submit queues a task for execution, assigns it to a capable cluster node,
// and publishes a worker.task.assigned event.
func (m *Module) Submit(ctx context.Context, task contracts.WorkerTask) (string, error) {
	if task.ID == "" {
		task.ID = uuid.New().String()
	}

	nodes := m.deps.Cluster.Members()
	node := m.selectNode(task, nodes)
	if node == nil {
		return task.ID, fmt.Errorf(
			"no capable node available for task %s (capabilities: %v)",
			task.ID, task.Capabilities,
		)
	}

	m.mu.Lock()
	task.AssignedNode = node.ID
	task.Status = contracts.WorkerTaskStatusAssigned
	task.LastHeartbeat = time.Now()
	m.ledger[task.ID] = task
	m.mu.Unlock()

	payload, _ := json.Marshal(assignedPayload{
		TaskID: task.ID,
		Type:   task.Type,
		NodeID: node.ID,
	})
	m.deps.EventBus.Publish(ctx, contracts.Event{
		ID:        uuid.New().String(),
		Type:      EventWorkerTaskAssigned,
		Source:    "worker-pool",
		Payload:   payload,
		Timestamp: time.Now(),
	})

	return task.ID, nil
}

// Status returns the current state of a task from the in-memory ledger.
func (m *Module) Status(ctx context.Context, taskID string) (contracts.WorkerTask, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	task, ok := m.ledger[taskID]
	if !ok {
		return contracts.WorkerTask{}, fmt.Errorf("task %s not found", taskID)
	}
	return task, nil
}

// Cancel stops a pending or assigned task by marking it cancelled.
func (m *Module) Cancel(ctx context.Context, taskID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	task, ok := m.ledger[taskID]
	if !ok {
		return fmt.Errorf("task %s not found", taskID)
	}

	if isTerminal(task.Status) {
		return fmt.Errorf("task %s is already in terminal state %s", taskID, task.Status)
	}

	task.Status = contracts.WorkerTaskStatusCancelled
	task.CompletedAt = time.Now()
	m.ledger[taskID] = task
	return nil
}

// List returns tasks from the ledger, optionally filtered by status, type,
// and/or assigned node.
func (m *Module) List(ctx context.Context, filter *contracts.WorkerTaskFilter) ([]contracts.WorkerTask, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []contracts.WorkerTask
	for _, task := range m.ledger {
		if filter != nil {
			if filter.Status != "" && task.Status != filter.Status {
				continue
			}
			if filter.Type != "" && task.Type != filter.Type {
				continue
			}
			if filter.AssignedNode != "" && task.AssignedNode != filter.AssignedNode {
				continue
			}
		}
		result = append(result, task)
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// Event handlers
// ---------------------------------------------------------------------------

// onNodeLeft handles cluster.node.left events by redistributing tasks that
// were assigned to the departed node.
func (m *Module) onNodeLeft(ctx context.Context, event contracts.Event) error {
	var payload contracts.NodeLeftPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal node left payload: %w", err)
	}

	slog.Warn("node left cluster, triggering failover", "node_id", payload.NodeID)
	m.handleNodeLeft(payload.NodeID)
	return nil
}

// onTaskStarted transitions a task from assigned to running.
func (m *Module) onTaskStarted(ctx context.Context, event contracts.Event) error {
	var payload struct {
		TaskID string `json:"task_id"`
		NodeID string `json:"node_id"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal task started payload: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if task, ok := m.ledger[payload.TaskID]; ok {
		task.Status = contracts.WorkerTaskStatusRunning
		task.StartedAt = time.Now()
		task.LastHeartbeat = time.Now()
		m.ledger[payload.TaskID] = task
	}
	return nil
}

// onTaskCompleted transitions a task to completed.
func (m *Module) onTaskCompleted(ctx context.Context, event contracts.Event) error {
	var payload struct {
		TaskID string `json:"task_id"`
		Result string `json:"result"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal task completed payload: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if task, ok := m.ledger[payload.TaskID]; ok {
		task.Status = contracts.WorkerTaskStatusCompleted
		task.CompletedAt = time.Now()
		m.ledger[payload.TaskID] = task
	}
	return nil
}

// onTaskFailed transitions a task to failed with the reported error.
func (m *Module) onTaskFailed(ctx context.Context, event contracts.Event) error {
	var payload struct {
		TaskID string `json:"task_id"`
		Error  string `json:"error"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal task failed payload: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if task, ok := m.ledger[payload.TaskID]; ok {
		task.Status = contracts.WorkerTaskStatusFailed
		task.Error = payload.Error
		task.CompletedAt = time.Now()
		m.ledger[payload.TaskID] = task
	}
	return nil
}

// onHeartbeat records the heartbeat timestamp on a task so the stale-task
// checker can detect stalls.
func (m *Module) onHeartbeat(ctx context.Context, event contracts.Event) error {
	var payload struct {
		TaskID string `json:"task_id"`
	}
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("unmarshal heartbeat payload: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if task, ok := m.ledger[payload.TaskID]; ok {
		task.LastHeartbeat = time.Now()
		m.ledger[payload.TaskID] = task
	}
	return nil
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// staleTaskLoop periodically scans for stalled tasks.
func (m *Module) staleTaskLoop() {
	ticker := time.NewTicker(staleCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.checkStaleTasks()
		}
	}
}

// isTerminal returns true for statuses that cannot be transitioned away from.
func isTerminal(s contracts.WorkerTaskStatus) bool {
	return s == contracts.WorkerTaskStatusCompleted ||
		s == contracts.WorkerTaskStatusFailed ||
		s == contracts.WorkerTaskStatusCancelled
}

// ---------------------------------------------------------------------------
// JSON payload structs
// ---------------------------------------------------------------------------

type assignedPayload struct {
	TaskID string `json:"task_id"`
	Type   string `json:"type"`
	NodeID string `json:"node_id"`
}
