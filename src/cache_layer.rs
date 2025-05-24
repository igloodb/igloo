// src/cache_layer.rs
use std::collections::HashMap;

pub struct Cache {
    store: HashMap<String, String>,
}

impl Cache {
    pub fn new() -> Self {
        Self { store: HashMap::new() }
    }

    pub fn get(&self, query: &str) -> Option<&String> {
        self.store.get(query)
    }

    pub fn set(&mut self, query: &str, result: &str) {
        self.store.insert(query.to_string(), result.to_string());
    }
}
