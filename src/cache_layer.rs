// src/cache_layer.rs
// A simple in-memory cache for query results.
// Currently, operations are infallible, but could return Result in the future
// if storage involved I/O or other fallible operations.
use std::collections::HashMap;

// Potentially add: use crate::errors::Result if cache operations become fallible.

pub struct Cache {
    store: HashMap<String, String>, // Key: query string, Value: serialized result string
}

impl Cache {
    pub fn new() -> Self {
        // Using log::debug here assumes that the log facade is available
        // and configured appropriately by the main application.
        // If this module were to be used more independently, direct logging setup
        // or passing a logger might be considered.
        log::debug!(target: "igloo_cache", "Initializing new in-memory cache instance.");
        Self { store: HashMap::new() }
    }

    pub fn get(&self, query: &str) -> Option<&String> {
        log::trace!(target: "igloo_cache", "Cache GET attempt for query: {}", query);
        let result = self.store.get(query);
        if result.is_some() {
            log::debug!(target: "igloo_cache", "Cache HIT for query: {}", query);
        } else {
            log::debug!(target: "igloo_cache", "Cache MISS for query: {}", query);
        }
        result
    }

    pub fn set(&mut self, query: &str, result: &str) {
        log::debug!(target: "igloo_cache", "Cache SET for query: {}. Result length: {}", query, result.len());
        self.store.insert(query.to_string(), result.to_string());
    }
}
