// src/compute/scheduler.rs
use crate::compute::api::{ComputeService, TaskDefinition, TaskFuture};
use crate::compute::errors::ComputeError;
use crate::compute::registry::{FunctionRegistry, RegisteredFunction};
use async_trait::async_trait;
use log::{debug, error, info, warn};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use uuid::Uuid;

// Internal representation of a task for the scheduler queue
struct ScheduledTask {
    task_def: TaskDefinition,
    result_sender: oneshot::Sender<Result<Vec<u8>, ComputeError>>,
}

pub struct LocalTaskScheduler {
    registry: Arc<Mutex<FunctionRegistry>>, // Registry needs to be mutable for registration
    task_sender: mpsc::Sender<ScheduledTask>,
    // Workers are spawned and run in the background, scheduler holds sender.
    // num_workers: usize, // Store if needed for management, otherwise just for spawning
}

impl LocalTaskScheduler {
    pub fn new(num_workers: usize, queue_depth: usize) -> Self {
        let registry = Arc::new(Mutex::new(FunctionRegistry::new()));
        let (task_sender, task_receiver_raw) = mpsc::channel::<ScheduledTask>(queue_depth);

        let task_receiver = Arc::new(Mutex::new(task_receiver_raw));

        for i in 0..num_workers {
            let worker_id = format!("worker-{}", i);
            info!("Spawning {}", worker_id);
            let receiver_clone = Arc::clone(&task_receiver);
            let registry_clone = Arc::clone(&registry); // Clone Arc for the worker

            tokio::spawn(async move {
                loop {
                    let scheduled_task_option: Option<ScheduledTask> = {
                        let mut receiver_guard = receiver_clone.lock().await;
                        // Correctly await the recv() call
                        receiver_guard.recv().await
                    };
                    // Check if the channel has been closed
                    let scheduled_task = match scheduled_task_option {
                        Some(task) => task,
                        None => {
                            // Channel closed, worker should exit.
                            info!("Task channel closed for {}, shutting down.", worker_id);
                            break;
                        }
                    };

                    debug!(
                        "{} received task: {}",
                        worker_id, scheduled_task.task_def.task_id
                    );
                    let task_def = scheduled_task.task_def;
                    let result_sender = scheduled_task.result_sender;

                    // Lock registry for reading
                    let registry_guard = registry_clone.lock().await;
                    let func_result = match registry_guard.get(&task_def.function_id) {
                        Some(func) => {
                            // Execute the function
                            func(task_def.args.clone()) // Pass args
                        }
                        None => Err(ComputeError::FunctionNotFound(task_def.function_id.clone())),
                    };

                    // Send the result back.
                    // If send fails, it means the receiver (TaskFuture) was dropped.
                    if result_sender.send(func_result).is_err() {
                        warn!(
                            "Failed to send result for task {}: receiver dropped.",
                            task_def.task_id
                        );
                    }
                }
            });
        }

        Self {
            registry,
            task_sender,
        }
    }

    /// Registers a function with the scheduler's FunctionRegistry.
    pub async fn register_function(
        &self,
        function_id: String,
        func: RegisteredFunction,
    ) -> Result<(), ComputeError> {
        let mut registry_guard = self.registry.lock().await;
        registry_guard.register(function_id, func);
        Ok(())
    }
}

#[async_trait]
impl ComputeService for LocalTaskScheduler {
    async fn submit<Args, R>(
        &self,
        function_id: String,
        args: Args,
    ) -> Result<TaskFuture<R>, ComputeError>
    where
        Args: Serialize + Send + Sync + 'static,
        R: DeserializeOwned + Send + 'static,
    {
        let task_id = Uuid::new_v4().to_string();

        let serialized_args = bincode::serialize(&args).map_err(|e| {
            ComputeError::SerializationError(format!(
                "Failed to serialize args for task {}: {}",
                task_id, e
            ))
        })?;

        let task_def = TaskDefinition {
            task_id: task_id.clone(),
            function_id,
            args: serialized_args,
        };

        let (result_sender, result_receiver) = oneshot::channel::<Result<Vec<u8>, ComputeError>>();

        let scheduled_task = ScheduledTask {
            task_def,
            result_sender,
        };

        self.task_sender.send(scheduled_task).await.map_err(|e| {
            ComputeError::SubmissionFailed(format!(
                "Failed to send task {} to worker queue: {}",
                task_id, e
            ))
        })?;
        debug!("Submitted task {} to queue.", task_id);

        Ok(TaskFuture::new(task_id, result_receiver))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compute::registry::FunctionRegistry; // For wrap_fn
    use serde::{Deserialize, Serialize};
    // use std::time::Duration; // Not strictly needed for these tests if not using std::thread::sleep
    // use tokio::time::sleep; // Not strictly needed for these tests

    #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
    struct SchedulerTestArgs {
        id: u32,
        val: String,
    }
    #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
    struct SchedulerTestRet {
        processed_val: String,
    }

    fn scheduler_test_func(args: SchedulerTestArgs) -> Result<SchedulerTestRet, ComputeError> {
        if args.id == 0 {
            Err(ComputeError::ExecutionFailed("ID cannot be 0".to_string()))
        } else {
            Ok(SchedulerTestRet {
                processed_val: format!("{}-processed-{}", args.val, args.id),
            })
        }
    }

    async fn setup_scheduler(num_workers: usize) -> LocalTaskScheduler {
        let scheduler = LocalTaskScheduler::new(num_workers, 10); // queue_depth of 10
        let wrapped_func = FunctionRegistry::wrap_fn(scheduler_test_func);
        scheduler
            .register_function("test_func".to_string(), wrapped_func)
            .await
            .unwrap();
        scheduler
    }

    #[tokio::test]
    async fn scheduler_submit_and_get_result_success() {
        let scheduler = setup_scheduler(1).await;
        let args = SchedulerTestArgs {
            id: 1,
            val: "data".to_string(),
        };

        let future: TaskFuture<SchedulerTestRet> = scheduler
            .submit("test_func".to_string(), args.clone())
            .await
            .unwrap();
        let result = future.await.unwrap();

        assert_eq!(result.processed_val, "data-processed-1");
    }

    #[tokio::test]
    async fn scheduler_handles_function_execution_error() {
        let scheduler = setup_scheduler(1).await;
        let args = SchedulerTestArgs {
            id: 0,
            val: "fail_data".to_string(),
        }; // Will cause error in scheduler_test_func

        let future: TaskFuture<SchedulerTestRet> = scheduler
            .submit("test_func".to_string(), args)
            .await
            .unwrap();
        match future.await {
            Err(ComputeError::ExecutionFailed(msg)) => assert_eq!(msg, "ID cannot be 0"),
            res => panic!("Expected ExecutionFailed error, got {:?}", res),
        }
    }

    #[tokio::test]
    async fn scheduler_handles_function_not_found() {
        let scheduler = setup_scheduler(1).await;
        let args = SchedulerTestArgs {
            id: 1,
            val: "some_data".to_string(),
        };

        let future: TaskFuture<SchedulerTestRet> = scheduler
            .submit("non_existent_func".to_string(), args)
            .await
            .unwrap();
        match future.await {
            Err(ComputeError::FunctionNotFound(id)) => assert_eq!(id, "non_existent_func"),
            res => panic!("Expected FunctionNotFound error, got {:?}", res),
        }
    }

    #[tokio::test]
    async fn scheduler_handles_concurrent_tasks() {
        let scheduler = setup_scheduler(2).await; // Use 2 workers
        let mut futures = Vec::new();

        for i in 1..=5 {
            // Submit 5 tasks
            let args = SchedulerTestArgs {
                id: i,
                val: format!("task_val_{}", i),
            };
            // Ensure submit is awaited if it's async, or handle the Result if it's sync
            let future: TaskFuture<SchedulerTestRet> = scheduler
                .submit("test_func".to_string(), args)
                .await
                .unwrap();
            futures.push(future);
        }

        let mut results = Vec::new();
        for future in futures {
            results.push(future.await.unwrap());
        }

        assert_eq!(results.len(), 5);
        // Check that all tasks completed and results are as expected (order might vary)
        for i in 1..=5 {
            let expected_val = format!("task_val_{}-processed-{}", i, i);
            assert!(
                results.iter().any(|r| r.processed_val == expected_val),
                "Result not found for id {}",
                i
            );
        }
    }
}
