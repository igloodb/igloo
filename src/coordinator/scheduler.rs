use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

// Assuming these paths are correct after previous steps
use crate::coordinator::planner::{QueryStage, ExecutionPlan}; // Added ExecutionPlan for QueryStage field
use crate::api::worker::TaskDefinition;
use crate::api::worker::worker_client::WorkerClient; // For gRPC client
use tonic::transport::Endpoint; // For connecting
use tonic::Request; // For wrapping the TaskDefinition

#[derive(Debug, Clone)]
pub struct WorkerInfo {
    pub id: String,
    pub address: String, // e.g., "http://localhost:50051"
    // pub last_heartbeat: std::time::Instant, // For future liveness checks
    // pub available_task_slots: usize, // For more advanced scheduling
}

pub struct Scheduler {
    workers: Arc<Mutex<Vec<WorkerInfo>>>,
    // next_worker_index: Mutex<usize>, // For more advanced round-robin or other strategies
}

impl Scheduler {
    pub fn new() -> Self {
        Scheduler {
            workers: Arc::new(Mutex::new(Vec::new())),
            // next_worker_index: Mutex::new(0),
        }
    }

    pub fn register_worker(&self, id: String, address: String) {
        let mut workers_lock = self.workers.lock().unwrap_or_else(|e| {
            log::error!("Failed to lock workers mutex for registration: {}", e);
            e.into_inner()
        });
        if !workers_lock.iter().any(|w| w.id == id) {
            log::info!("Registering new worker: {} at {}", id, address);
            workers_lock.push(WorkerInfo { id, address });
        } else {
            log::warn!("Attempted to register already existing worker: {}", id);
        }
    }

    // Basic worker selection for now.
    // In a real scenario, this would involve more complex logic,
    // potentially considering worker load, data locality, etc.
    // For this subtask, cycling through workers is sufficient.
    // This internal method is not directly used by schedule_stages in this version,
    // as schedule_stages implements its own cycling.
    #[allow(dead_code)]
    fn select_worker_round_robin(&self, current_index: &mut usize) -> Option<WorkerInfo> {
        let workers_lock = self.workers.lock().unwrap_or_else(|e| {
            log::error!("Failed to lock workers mutex for selection: {}", e);
            e.into_inner()
        });
        if workers_lock.is_empty() {
            return None;
        }
        let worker_info = workers_lock.get(*current_index).cloned();
        *current_index = (*current_index + 1) % workers_lock.len();
        worker_info
    }

    pub async fn schedule_stages(&self, stages: Vec<QueryStage>) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        let mut dispatched_task_ids = Vec::new();
        let workers_guard = self.workers.lock().map_err(|e| format!("Failed to lock workers mutex for scheduling: {}", e))?;

        if workers_guard.is_empty() {
            log::error!("No workers available to schedule stages.");
            return Err("No workers available".into());
        }

        let workers_snapshot: Vec<WorkerInfo> = workers_guard.clone();
        drop(workers_guard); // Release the lock early

        let mut worker_iter = workers_snapshot.iter().cycle();

        for stage in stages {
            log::info!("Processing stage: {}", stage.stage_id);
            let num_partitions = 1; // Placeholder

            for partition_id in 0..num_partitions {
                let task_id = format!("task_{}_stage_{}_part_{}", Uuid::new_v4(), stage.stage_id, partition_id);

                let worker_info = match worker_iter.next() {
                    Some(w) => w.clone(),
                    None => {
                        log::error!("Failed to select a worker for task {} (logic error or empty worker list)", task_id);
                        return Err("Failed to select worker due to empty worker list or logic error".into());
                    }
                };

                log::info!("Assigning task {} (stage {}, partition {}) to worker {}", task_id, stage.stage_id, partition_id, worker_info.id);

                let plan_bytes: Vec<u8> = Vec::new(); // Placeholder for serialized stage.plan

                let task_def = TaskDefinition {
                    task_id: task_id.clone(),
                    plan: plan_bytes,
                    partition_id: partition_id as u32,
                };

                log::info!("Attempting to dispatch TaskDefinition: {:?} to worker {} at {}", task_def, worker_info.id, worker_info.address);

                // Create a gRPC client and call execute_task
                match Endpoint::from_shared(worker_info.address.clone())?.timeout(std::time::Duration::from_secs(5)).connect().await {
                    Ok(channel) => {
                        let mut client = WorkerClient::new(channel);
                        let request = Request::new(task_def.clone()); // Clone task_def as it's used in error logging

                        match client.execute_task(request).await {
                            Ok(response) => {
                                let task_result = response.into_inner();
                                log::info!("Received TaskResult from worker {}: {:?}", worker_info.id, task_result);
                                if !task_result.success {
                                    log::warn!("Task {} failed on worker {}: {}", task_result.task_id, worker_info.id, task_result.message);
                                    // Optional: could collect failures or retry
                                }
                                dispatched_task_ids.push(task_id.clone()); // Push task_id on successful or failed processing by worker
                            }
                            Err(status) => {
                                log::error!("Error dispatching task {} to worker {}: {}", task_def.task_id, worker_info.id, status);
                                // Not returning error here, just logging. Task ID is not added to dispatched_task_ids for RPC errors.
                                // Depending on policy, might want to return Err(status.into()) or retry.
                            }
                        }
                    }
                    Err(transport_error) => {
                         log::error!("Failed to connect to worker {} at {}: {}", worker_info.id, worker_info.address, transport_error);
                        // Not returning error here. Task ID is not added.
                        // Depending on policy, might want to return Err(transport_error.into()).
                    }
                }
            }
        }
        Ok(dispatched_task_ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use crate::coordinator::planner::{StageInputExec, DummyLeafExec}; // Assuming these are pub or re-exported

    // Helper to create a dummy QueryStage
    fn create_dummy_stage(stage_id: usize, plan_name: &str, dependencies: Vec<usize>) -> QueryStage {
        let plan = Arc::new(DummyLeafExec::new(plan_name)) as Arc<dyn ExecutionPlan>;
        QueryStage {
            stage_id,
            plan,
            dependencies,
        }
    }

    #[tokio::test]
    async fn test_register_worker() {
        let scheduler = Scheduler::new();
        scheduler.register_worker("worker1".to_string(), "addr1".to_string());

        let workers = scheduler.workers.lock().unwrap();
        assert_eq!(workers.len(), 1);
        assert_eq!(workers[0].id, "worker1");
        assert_eq!(workers[0].address, "addr1");

        scheduler.register_worker("worker1".to_string(), "addr1_new".to_string());
        let workers_after_dup = scheduler.workers.lock().unwrap();
        assert_eq!(workers_after_dup.len(), 1);
        assert_eq!(workers_after_dup[0].address, "addr1");
    }

    #[tokio::test]
    async fn test_schedule_stages_no_workers() {
        let scheduler = Scheduler::new();
        let stage1 = create_dummy_stage(0, "plan0", vec![]);
        let stages_to_schedule = vec![stage1];

        let result = scheduler.schedule_stages(stages_to_schedule).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "No workers available".to_string());
    }

    #[tokio::test]
    async fn test_schedule_single_stage_single_worker() {
        let scheduler = Scheduler::new();
        scheduler.register_worker("worker1".to_string(), "addr1".to_string());

        let stage1 = create_dummy_stage(0, "plan0", vec![]);
        let stages_to_schedule = vec![stage1];

        let result = scheduler.schedule_stages(stages_to_schedule).await;
        assert!(result.is_ok());
        let dispatched_tasks = result.unwrap();
        assert_eq!(dispatched_tasks.len(), 1); // 1 stage * 1 partition = 1 task
        assert!(dispatched_tasks[0].contains("_stage_0_part_0"));
    }

    #[tokio::test]
    async fn test_schedule_multiple_stages_multiple_workers_cycling() {
        let scheduler = Scheduler::new();
        scheduler.register_worker("worker1".to_string(), "addr1".to_string());
        scheduler.register_worker("worker2".to_string(), "addr2".to_string());

        let stage1 = create_dummy_stage(0, "plan_s0", vec![]);
        let stage2 = create_dummy_stage(1, "plan_s1", vec![0]); // Depends on stage 0
        // Assuming num_partitions = 1 for each stage as per current schedule_stages logic
        let stages_to_schedule = vec![stage1, stage2];

        // The schedule_stages function currently logs which worker gets which task.
        // We are checking that tasks are generated. The actual dispatch logic (gRPC call)
        // is not part of this subtask, so we can't verify worker assignment beyond logs.
        // However, with cycling, tasks for stage1 and stage2 should be created.

        let result = scheduler.schedule_stages(stages_to_schedule).await;
        assert!(result.is_ok(), "Scheduling failed: {:?}", result.err());
        let dispatched_tasks = result.unwrap();

        assert_eq!(dispatched_tasks.len(), 2); // 2 stages * 1 partition/stage = 2 tasks
        assert!(dispatched_tasks.iter().any(|id| id.contains("_stage_0_part_0")));
        assert!(dispatched_tasks.iter().any(|id| id.contains("_stage_1_part_0")));

        // To verify round-robin, one would typically need to inspect logs or have the
        // scheduler return assignments. For now, success means tasks were generated.
    }

    #[tokio::test]
    async fn test_schedule_stages_dependencies_not_enforced_here() {
        // This test just confirms tasks are created even if dependencies are not strictly
        // checked by this version of schedule_stages. The main goal is task generation.
        let scheduler = Scheduler::new();
        scheduler.register_worker("worker1".to_string(), "addr1".to_string());

        let stage1 = create_dummy_stage(1, "plan_s1", vec![0]); // Depends on non-existent stage 0
        let stage0 = create_dummy_stage(0, "plan_s0", vec![]);

        // Order of stages matters if scheduler respects it, but current one iterates as given.
        let stages_to_schedule = vec![stage1, stage0];
        let result = scheduler.schedule_stages(stages_to_schedule).await;
        assert!(result.is_ok());
        let dispatched_tasks = result.unwrap();
        assert_eq!(dispatched_tasks.len(), 2);
    }
}
