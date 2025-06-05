// src/compute/api.rs
use crate::compute::errors::ComputeError;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::future::Future;
use std::pin::Pin;
use tokio::sync::oneshot;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TaskDefinition {
    pub task_id: String,
    pub function_id: String,
    pub args: Vec<u8>, // Serialized arguments
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TaskResult {
    pub task_id: String,
    pub value: Option<Vec<u8>>, // Serialized result value
    pub error: Option<String>,  // Error message if execution failed
}

/// A future representing a task submitted for execution.
/// Resolves to the deserialized result of the task or a ComputeError.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct TaskFuture<R: DeserializeOwned + Send + 'static> {
    task_id: String,
    receiver: oneshot::Receiver<Result<Vec<u8>, ComputeError>>, // Receives serialized result or execution error
    _marker: std::marker::PhantomData<R>,
}

impl<R: DeserializeOwned + Send + 'static> TaskFuture<R> {
    pub fn new(
        task_id: String,
        receiver: oneshot::Receiver<Result<Vec<u8>, ComputeError>>,
    ) -> Self {
        Self {
            task_id,
            receiver,
            _marker: std::marker::PhantomData,
        }
    }

    pub fn task_id(&self) -> &str {
        &self.task_id
    }
}

impl<R: DeserializeOwned + Send + 'static> Future for TaskFuture<R> {
    type Output = Result<R, ComputeError>;

    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match Pin::new(&mut self.receiver).poll(cx) {
            std::task::Poll::Ready(Ok(Ok(serialized_value))) => {
                // Received serialized value, now deserialize
                match bincode::deserialize(&serialized_value) {
                    Ok(deserialized_value) => std::task::Poll::Ready(Ok(deserialized_value)),
                    Err(e) => {
                        std::task::Poll::Ready(Err(ComputeError::DeserializationError(format!(
                            "Failed to deserialize result for task {}: {}",
                            self.task_id, e
                        ))))
                    }
                }
            }
            std::task::Poll::Ready(Ok(Err(compute_err))) => {
                // Received a ComputeError directly (e.g., execution error from worker)
                std::task::Poll::Ready(Err(compute_err))
            }
            std::task::Poll::Ready(Err(oneshot_err)) => {
                // oneshot channel was cancelled/closed
                std::task::Poll::Ready(Err(ComputeError::ResultRetrievalFailed(format!(
                    "Result channel closed for task {}: {}",
                    self.task_id, oneshot_err
                ))))
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

#[async_trait::async_trait]
pub trait ComputeService: Send + Sync {
    /// Submits a task for execution.
    ///
    /// # Arguments
    /// * `function_id`: A unique identifier for the function to be executed.
    /// * `args`: The arguments for the function, which must implement `Serialize`.
    ///
    /// # Returns
    /// A `TaskFuture<R>` which can be awaited to get the deserialized result `R`
    /// or a `ComputeError`.
    async fn submit<Args, R>(
        &self,
        function_id: String,
        args: Args,
    ) -> Result<TaskFuture<R>, ComputeError>
    where
        Args: Serialize + Send + Sync + 'static,
        R: DeserializeOwned + Send + 'static;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compute::errors::ComputeError;
    use serde::{Deserialize, Serialize};
    use tokio::sync::oneshot;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct TestData {
        x: i32,
        s: String,
    }

    #[test]
    fn task_definition_serialization() {
        let task_def = TaskDefinition {
            task_id: "task_123".to_string(),
            function_id: "func_abc".to_string(),
            args: bincode::serialize(&"test_args").unwrap(),
        };
        let serialized = bincode::serialize(&task_def).unwrap();
        let deserialized: TaskDefinition = bincode::deserialize(&serialized).unwrap();
        assert_eq!(task_def.task_id, deserialized.task_id);
        assert_eq!(task_def.function_id, deserialized.function_id);
        assert_eq!(task_def.args, deserialized.args);
    }

    #[test]
    fn task_result_serialization_success() {
        let data = TestData {
            x: 10,
            s: "hello".to_string(),
        };
        let task_res = TaskResult {
            task_id: "task_123".to_string(),
            value: Some(bincode::serialize(&data).unwrap()),
            error: None,
        };
        let serialized = bincode::serialize(&task_res).unwrap();
        let deserialized: TaskResult = bincode::deserialize(&serialized).unwrap();
        assert_eq!(task_res.task_id, deserialized.task_id);
        assert_eq!(task_res.error, deserialized.error);
        let deserialized_data: TestData =
            bincode::deserialize(&deserialized.value.unwrap()).unwrap();
        assert_eq!(data, deserialized_data);
    }

    #[test]
    fn task_result_serialization_error() {
        let task_res = TaskResult {
            task_id: "task_123".to_string(),
            value: None,
            error: Some("failed miserably".to_string()),
        };
        let serialized = bincode::serialize(&task_res).unwrap();
        let deserialized: TaskResult = bincode::deserialize(&serialized).unwrap();
        assert_eq!(task_res.task_id, deserialized.task_id);
        assert_eq!(task_res.value, deserialized.value);
        assert_eq!(task_res.error, deserialized.error);
    }

    #[tokio::test]
    async fn task_future_resolves_value() {
        let (tx, rx) = oneshot::channel::<Result<Vec<u8>, ComputeError>>();
        let future: TaskFuture<TestData> = TaskFuture::new("test_task".to_string(), rx);

        let data = TestData {
            x: 42,
            s: "success".to_string(),
        };
        let serialized_data = bincode::serialize(&data).unwrap();
        tx.send(Ok(serialized_data)).unwrap();

        match future.await {
            Ok(resolved_data) => assert_eq!(resolved_data, data),
            Err(e) => panic!("Future returned error: {:?}", e),
        }
    }

    #[tokio::test]
    async fn task_future_resolves_compute_error() {
        let (tx, rx) = oneshot::channel::<Result<Vec<u8>, ComputeError>>();
        let future: TaskFuture<TestData> = TaskFuture::new("test_task".to_string(), rx);

        let error = ComputeError::ExecutionFailed("test_execution_error".to_string());
        // Note: The TaskFuture expects Result<Vec<u8>, ComputeError>, so an execution error from a worker
        // would be Err(ComputeError::ExecutionFailed(...))
        tx.send(Err(error)).unwrap(); // This error is the ComputeError itself.

        match future.await {
            Ok(_) => panic!("Future should have returned an error"),
            Err(ComputeError::ExecutionFailed(msg)) => assert_eq!(msg, "test_execution_error"),
            Err(e) => panic!("Future returned unexpected error type: {:?}", e),
        }
    }

    #[tokio::test]
    async fn task_future_handles_deserialization_error() {
        let (tx, rx) = oneshot::channel::<Result<Vec<u8>, ComputeError>>();
        let future: TaskFuture<TestData> = TaskFuture::new("test_task".to_string(), rx);

        let bad_data = vec![1, 2, 3]; // Not valid TestData bincode
        tx.send(Ok(bad_data)).unwrap();

        match future.await {
            Ok(_) => panic!("Future should have returned a deserialization error"),
            Err(ComputeError::DeserializationError(_)) => { /* Expected */ }
            Err(e) => panic!("Future returned unexpected error type: {:?}", e),
        }
    }

    #[tokio::test]
    async fn task_future_handles_channel_closed_error() {
        let (tx, rx) = oneshot::channel::<Result<Vec<u8>, ComputeError>>();
        let future: TaskFuture<TestData> = TaskFuture::new("test_task".to_string(), rx);

        drop(tx); // Close the channel

        match future.await {
            Ok(_) => panic!("Future should have returned a channel closed error"),
            Err(ComputeError::ResultRetrievalFailed(_)) => { /* Expected */ }
            Err(e) => panic!("Future returned unexpected error type: {:?}", e),
        }
    }
}
