use crate::types::TypedValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    pub data: HashMap<String, TypedValue>,
}

impl Row {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn insert(&mut self, column: String, value: TypedValue) {
        self.data.insert(column, value);
    }

    pub fn get(&self, column: &str) -> Option<&TypedValue> {
        self.data.get(column)
    }

    pub fn get_all(&self) -> &HashMap<String, TypedValue> {
        &self.data
    }

    pub fn get_as_string(&self, column: &str) -> String {
        match self.data.get(column) {
            Some(value) => value.to_string(),
            None => "NULL".to_string(),
        }
    }

    pub fn from_values(values: Vec<(String, TypedValue)>) -> Self {
        let mut row = Self::new();
        for (column, value) in values {
            row.insert(column, value);
        }
        row
    }
} 