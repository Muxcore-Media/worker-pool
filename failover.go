package workerpool

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/Muxcore-Media/core/pkg/contracts"
	"github.com/google/uuid"
)

// handleNodeLeft redistributes all active tasks that were assigned to the
// given node.
func (m *Module) handleNodeLeft(nodeID string) {
	// Collect affected tasks while holding the lock briefly, then
	// redistribute without the lock to avoid deadlock with Submit.
	var affected []contracts.WorkerTask

	m.mu.Lock()
	for id, task := range m.ledger {
		if task.AssignedNode == nodeID &&
			(task.Status == contracts.WorkerTaskStatusAssigned ||
				task.Status == contracts.WorkerTaskStatusRunning) {
			affected = append(affected, m.ledger[id])
		}
	}
	m.mu.Unlock()

	for _, task := range affected {
		slog.Info("redistributing task after node departure",
			"task_id", task.ID,
			"node_id", nodeID,
			"retry", task.RetryCount+1,
		)
		m.redistribute(task)
	}
}

// checkStaleTasks scans the ledger for tasks whose LastHeartbeat is older
// than staleTimeout and redistributes them.
func (m *Module) checkStaleTasks() {
	var stale []contracts.WorkerTask

	m.mu.Lock()
	now := time.Now()
	for id, task := range m.ledger {
		if (task.Status == contracts.WorkerTaskStatusAssigned ||
			task.Status == contracts.WorkerTaskStatusRunning) &&
			now.Sub(task.LastHeartbeat) > staleTimeout {
			stale = append(stale, m.ledger[id])
		}
	}
	m.mu.Unlock()

	for _, task := range stale {
		slog.Warn("task stalled, redistributing",
			"task_id", task.ID,
			"node_id", task.AssignedNode,
			"retry", task.RetryCount+1,
		)
		m.redistribute(task)
	}
}

// redistribute attempts to re-assign a task. If the retry budget is exhausted
// the task is marked as failed permanently.
func (m *Module) redistribute(task contracts.WorkerTask) {
	task.RetryCount++

	if task.MaxRetries <= 0 {
		task.MaxRetries = 3
	}

	if task.RetryCount >= task.MaxRetries {
		m.failTask(task, "max retries exceeded")
		slog.Error("task failed after max retries",
			"task_id", task.ID,
			"retries", task.RetryCount,
		)
		return
	}

	// Reset for re-submission.
	task.Status = contracts.WorkerTaskStatusPending
	task.AssignedNode = ""

	if _, err := m.Submit(context.Background(), task); err != nil {
		m.failTask(task, err.Error())
		slog.Error("task redistribution failed",
			"task_id", task.ID,
			"error", err,
		)
	}
}

// failTask marks a task as failed in the ledger and publishes a
// worker.task.failed event.
func (m *Module) failTask(task contracts.WorkerTask, reason string) {
	m.mu.Lock()
	task.Status = contracts.WorkerTaskStatusFailed
	task.Error = reason
	task.CompletedAt = time.Now()
	m.ledger[task.ID] = task
	m.mu.Unlock()

	payload, _ := json.Marshal(map[string]string{
		"task_id": task.ID,
		"error":   reason,
	})
	m.deps.EventBus.Publish(context.Background(), contracts.Event{
		ID:        uuid.New().String(),
		Type:      EventWorkerTaskFailed,
		Source:    "worker-pool",
		Payload:   payload,
		Timestamp: time.Now(),
	})
}
