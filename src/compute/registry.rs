// src/compute/registry.rs
use crate::compute::errors::ComputeError;
use serde::{de::DeserializeOwned, Serialize}; // Add Serialize, DeserializeOwned
use std::collections::HashMap;

pub type RegisteredFunction =
    Box<dyn Fn(Vec<u8>) -> Result<Vec<u8>, ComputeError> + Send + Sync + 'static>;

#[derive(Default)]
pub struct FunctionRegistry {
    functions: HashMap<String, RegisteredFunction>,
}

impl FunctionRegistry {
    pub fn new() -> Self {
        Default::default()
    }

    /// Registers a new function.
    /// If a function with the same ID already exists, it will be overwritten.
    pub fn register(&mut self, function_id: String, func: RegisteredFunction) {
        self.functions.insert(function_id, func);
    }

    /// Retrieves a reference to the registered function.
    pub fn get(&self, function_id: &str) -> Option<&RegisteredFunction> {
        self.functions.get(function_id)
    }

    // Helper for tests or direct invocation if needed
    #[cfg(test)]
    pub fn call(&self, function_id: &str, args: Vec<u8>) -> Result<Vec<u8>, ComputeError> {
        match self.get(function_id) {
            Some(func) => func(args),
            None => Err(ComputeError::FunctionNotFound(function_id.to_string())),
        }
    }

    /// Wraps a given function `F` that takes arguments `Args` and returns `Result<Ret, E>`
    /// into a `RegisteredFunction` type.
    ///
    /// `Args`: Tuple of arguments, must be DeserializeOwned.
    /// `Ret`: Return type, must be Serialize.
    /// `F`: The function to wrap. It must be Send + Sync + 'static.
    /// `E`: The error type of the wrapped function, must be `Into<ComputeError>`.
    ///
    /// This version assumes the user function itself returns a Result.
    pub fn wrap_fn<Args, Ret, F, E>(func: F) -> RegisteredFunction
    where
        Args: DeserializeOwned + Send + Sync + 'static,
        Ret: Serialize + Send + Sync + 'static,
        E: Into<ComputeError> + Send + Sync + 'static,
        F: Fn(Args) -> Result<Ret, E> + Send + Sync + 'static,
    {
        Box::new(move |serialized_args: Vec<u8>| {
            // 1. Deserialize arguments
            let deserialized_args: Args = bincode::deserialize(&serialized_args).map_err(|e| {
                ComputeError::DeserializationError(format!(
                    "Failed to deserialize arguments: {}",
                    e
                ))
            })?;

            // 2. Execute the provided function
            match func(deserialized_args) {
                Ok(result) => {
                    // 3. Serialize the successful result
                    bincode::serialize(&result).map_err(|e| {
                        ComputeError::SerializationError(format!(
                            "Failed to serialize result: {}",
                            e
                        ))
                    })
                }
                Err(user_err) => {
                    // 4. Convert user error into ComputeError
                    Err(user_err.into())
                }
            }
        })
    }

    /// Wraps a given fallible function `F` that takes arguments `Args` and returns `Ret`
    /// (panics on error or assumes it's infallible in its own context but we handle panics).
    ///
    /// `Args`: Tuple of arguments, must be DeserializeOwned.
    /// `Ret`: Return type, must be Serialize.
    /// `F`: The function to wrap. It must be Send + Sync + 'static.
    ///
    /// This version is for functions that do not return `Result` themselves.
    /// It catches panics during execution.
    pub fn wrap_infallible_fn<Args, Ret, F>(func: F) -> RegisteredFunction
    where
        Args: DeserializeOwned + Send + Sync + 'static,
        Ret: Serialize + Send + Sync + 'static,
        F: Fn(Args) -> Ret + Send + Sync + 'static + std::panic::UnwindSafe, // UnwindSafe for catch_unwind
    {
        Box::new(move |serialized_args: Vec<u8>| {
            // 1. Deserialize arguments
            let deserialized_args: Args = match bincode::deserialize(&serialized_args) {
                Ok(args) => args,
                Err(e) => {
                    return Err(ComputeError::DeserializationError(format!(
                        "Failed to deserialize arguments: {}",
                        e
                    )))
                }
            };

            // 2. Execute the provided function, catching panics
            // Wrap the call in catch_unwind
            let result = std::panic::catch_unwind(|| func(deserialized_args));

            match result {
                Ok(ret_val) => {
                    // 3. Serialize the successful result
                    bincode::serialize(&ret_val).map_err(|e| {
                        ComputeError::SerializationError(format!(
                            "Failed to serialize result: {}",
                            e
                        ))
                    })
                }
                Err(panic_payload) => {
                    // 4. Handle panic
                    let err_msg = if let Some(s) = panic_payload.downcast_ref::<String>() {
                        s.clone()
                    } else if let Some(s) = panic_payload.downcast_ref::<&str>() {
                        s.to_string()
                    } else {
                        "Panicked but could not retrieve error message".to_string()
                    };
                    Err(ComputeError::ExecutionFailed(format!(
                        "Function panicked: {}",
                        err_msg
                    )))
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compute::errors::ComputeError;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
    struct TestArgs {
        a: i32,
        b: String,
    }
    #[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
    struct TestRet {
        sum: i32,
        concat: String,
    }

    fn sample_func(args: TestArgs) -> Result<TestRet, ComputeError> {
        if args.a < 0 {
            Err(ComputeError::ExecutionFailed(
                "Negative a not allowed".to_string(),
            ))
        } else {
            Ok(TestRet {
                sum: args.a + args.b.len() as i32,
                concat: format!("{}{}", args.b, args.a),
            })
        }
    }

    fn infallible_func(args: TestArgs) -> TestRet {
        if args.a == 99 {
            panic!("a is 99, panic!");
        }
        TestRet {
            sum: args.a + args.b.len() as i32,
            concat: format!("{}{}", args.b, args.a),
        }
    }

    #[test]
    fn register_and_get_function() {
        let mut registry = FunctionRegistry::new();
        let wrapped_func = FunctionRegistry::wrap_fn(sample_func);
        registry.register("my_func".to_string(), wrapped_func);

        assert!(registry.get("my_func").is_some());
        assert!(registry.get("non_existent_func").is_none());
    }

    #[test]
    fn wrap_fn_success() {
        let wrapped = FunctionRegistry::wrap_fn(sample_func);
        let args = TestArgs {
            a: 5,
            b: "hello".to_string(),
        };
        let serialized_args = bincode::serialize(&args).unwrap();

        let result_vec = wrapped(serialized_args).unwrap();
        let result: TestRet = bincode::deserialize(&result_vec).unwrap();

        assert_eq!(
            result,
            TestRet {
                sum: 10,
                concat: "hello5".to_string()
            }
        );
    }

    #[test]
    fn wrap_fn_propagates_user_error() {
        let wrapped = FunctionRegistry::wrap_fn(sample_func);
        let args = TestArgs {
            a: -1,
            b: "error".to_string(),
        };
        let serialized_args = bincode::serialize(&args).unwrap();

        match wrapped(serialized_args) {
            Err(ComputeError::ExecutionFailed(msg)) => assert_eq!(msg, "Negative a not allowed"),
            _ => panic!("Expected ExecutionFailed"),
        }
    }

    #[test]
    fn wrap_fn_handles_arg_deserialization_error() {
        let wrapped = FunctionRegistry::wrap_fn(sample_func);
        let bad_args = vec![1, 2, 3]; // Invalid

        match wrapped(bad_args) {
            Err(ComputeError::DeserializationError(_)) => { /* Expected */ }
            _ => panic!("Expected DeserializationError"),
        }
    }

    #[test]
    fn wrap_infallible_fn_success() {
        let wrapped = FunctionRegistry::wrap_infallible_fn(infallible_func);
        let args = TestArgs {
            a: 10,
            b: "world".to_string(),
        };
        let serialized_args = bincode::serialize(&args).unwrap();

        let result_vec = wrapped(serialized_args).unwrap();
        let result: TestRet = bincode::deserialize(&result_vec).unwrap();

        assert_eq!(
            result,
            TestRet {
                sum: 15,
                concat: "world10".to_string()
            }
        );
    }

    #[test]
    fn wrap_infallible_fn_handles_panic() {
        let wrapped = FunctionRegistry::wrap_infallible_fn(infallible_func);
        let args = TestArgs {
            a: 99,
            b: "panic".to_string(),
        }; // Will cause panic
        let serialized_args = bincode::serialize(&args).unwrap();

        match wrapped(serialized_args) {
            Err(ComputeError::ExecutionFailed(msg)) => {
                assert!(msg.contains("Function panicked: a is 99, panic!"))
            }
            _ => panic!("Expected ExecutionFailed due to panic"),
        }
    }
    #[test]
    fn call_method_on_registry() {
        let mut registry = FunctionRegistry::new();
        registry.register(
            "sample_func".to_string(),
            FunctionRegistry::wrap_fn(sample_func),
        );

        let args = TestArgs {
            a: 5,
            b: "hello".to_string(),
        };
        let serialized_args = bincode::serialize(&args).unwrap();

        let result_vec = registry.call("sample_func", serialized_args).unwrap();
        let result: TestRet = bincode::deserialize(&result_vec).unwrap();
        assert_eq!(
            result,
            TestRet {
                sum: 10,
                concat: "hello5".to_string()
            }
        );

        match registry.call("non_existent", vec![]) {
            Err(ComputeError::FunctionNotFound(id)) => assert_eq!(id, "non_existent"),
            _ => panic!("Expected FunctionNotFound"),
        }
    }
}
