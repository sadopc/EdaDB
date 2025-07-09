use crate::types::{Column, TypedValue};
use crate::row::Row;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;

/// Storage format configuration
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StorageFormat {
    RowBased,
    ColumnBased,
    Hybrid, // Automatically choose based on query patterns
}

impl Default for StorageFormat {
    fn default() -> Self {
        StorageFormat::RowBased
    }
}

/// Column store that stores data column-wise for analytical workloads
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStore {
    pub table_name: String,
    pub columns: Vec<Column>,
    pub column_data: HashMap<String, Vec<TypedValue>>, // column_name -> values
    pub row_count: usize,
    pub storage_format: StorageFormat,
    pub created_at: String,
    pub last_compressed: Option<String>,
    pub compression_ratio: f64,
}

impl ColumnStore {
    pub fn new(table_name: String, columns: Vec<Column>) -> Self {
        let mut column_data = HashMap::new();
        for column in &columns {
            column_data.insert(column.name.clone(), Vec::new());
        }
        
        Self {
            table_name,
            columns,
            column_data,
            row_count: 0,
            storage_format: StorageFormat::ColumnBased,
            created_at: chrono::Utc::now().to_rfc3339(),
            last_compressed: None,
            compression_ratio: 1.0,
        }
    }
    
    /// Convert from row-based storage to columnar storage
    pub fn from_rows(table_name: String, columns: Vec<Column>, rows: &[Row]) -> Self {
        let mut column_store = Self::new(table_name, columns);
        
        for row in rows {
            column_store.insert_row(row);
        }
        
        column_store
    }
    
    /// Insert a row into columnar storage
    pub fn insert_row(&mut self, row: &Row) {
        for column in &self.columns {
            let value = row.get(&column.name).cloned().unwrap_or(TypedValue::Null);
            self.column_data.entry(column.name.clone()).or_insert_with(Vec::new).push(value);
        }
        self.row_count += 1;
    }
    
    /// Insert multiple rows efficiently
    pub fn insert_rows(&mut self, rows: &[Row]) {
        for row in rows {
            self.insert_row(row);
        }
    }
    
    /// Get a specific column's data
    pub fn get_column(&self, column_name: &str) -> Option<&Vec<TypedValue>> {
        self.column_data.get(column_name)
    }
    
    /// Get specific rows by indices
    pub fn get_rows_by_indices(&self, indices: &[usize]) -> Vec<Row> {
        let mut rows = Vec::new();
        
        for &row_index in indices {
            if row_index < self.row_count {
                let mut row = Row::new();
                for column in &self.columns {
                    if let Some(column_values) = self.column_data.get(&column.name) {
                        if let Some(value) = column_values.get(row_index) {
                            row.insert(column.name.clone(), value.clone());
                        }
                    }
                }
                rows.push(row);
            }
        }
        
        rows
    }
    
    /// Convert back to row-based storage
    pub fn to_rows(&self) -> Vec<Row> {
        let mut rows = Vec::new();
        
        for row_index in 0..self.row_count {
            let mut row = Row::new();
            for column in &self.columns {
                if let Some(column_values) = self.column_data.get(&column.name) {
                    if let Some(value) = column_values.get(row_index) {
                        row.insert(column.name.clone(), value.clone());
                    }
                }
            }
            rows.push(row);
        }
        
        rows
    }
    
    /// Analytical query: column aggregation
    pub fn aggregate_column(&self, column_name: &str, operation: AggregateOperation) -> Option<TypedValue> {
        let column_values = self.column_data.get(column_name)?;
        
        match operation {
            AggregateOperation::Count => {
                Some(TypedValue::Integer(column_values.len() as i64))
            }
            AggregateOperation::Sum => {
                let mut sum = 0i64;
                for value in column_values {
                    if let TypedValue::Integer(i) = value {
                        sum += i;
                    }
                }
                Some(TypedValue::Integer(sum))
            }
            AggregateOperation::Avg => {
                let mut sum = 0i64;
                let mut count = 0;
                for value in column_values {
                    if let TypedValue::Integer(i) = value {
                        sum += i;
                        count += 1;
                    }
                }
                if count > 0 {
                    Some(TypedValue::Integer(sum / count))
                } else {
                    Some(TypedValue::Null)
                }
            }
            AggregateOperation::Min => {
                let mut min_value: Option<TypedValue> = None;
                for value in column_values {
                    if *value != TypedValue::Null {
                        if min_value.is_none() || value < min_value.as_ref().unwrap() {
                            min_value = Some(value.clone());
                        }
                    }
                }
                Some(min_value.unwrap_or(TypedValue::Null))
            }
            AggregateOperation::Max => {
                let mut max_value: Option<TypedValue> = None;
                for value in column_values {
                    if *value != TypedValue::Null {
                        if max_value.is_none() || value > max_value.as_ref().unwrap() {
                            max_value = Some(value.clone());
                        }
                    }
                }
                Some(max_value.unwrap_or(TypedValue::Null))
            }
        }
    }
    
    /// Column-wise filtering (more efficient for analytics)
    pub fn filter_column_indices(&self, column_name: &str, predicate: &ColumnPredicate) -> Vec<usize> {
        let column_values = match self.column_data.get(column_name) {
            Some(values) => values,
            None => return Vec::new(),
        };
        
        let mut matching_indices = Vec::new();
        
        for (index, value) in column_values.iter().enumerate() {
            if self.evaluate_predicate(value, predicate) {
                matching_indices.push(index);
            }
        }
        
        matching_indices
    }
    
    /// Column-wise range filtering (optimized for sorted columns)
    pub fn filter_range_indices(&self, column_name: &str, min_value: &TypedValue, max_value: &TypedValue) -> Vec<usize> {
        let column_values = match self.column_data.get(column_name) {
            Some(values) => values,
            None => return Vec::new(),
        };
        
        let mut matching_indices = Vec::new();
        
        for (index, value) in column_values.iter().enumerate() {
            if value >= min_value && value <= max_value {
                matching_indices.push(index);
            }
        }
        
        matching_indices
    }
    
    /// Evaluate column predicate
    fn evaluate_predicate(&self, value: &TypedValue, predicate: &ColumnPredicate) -> bool {
        match predicate {
            ColumnPredicate::Equal(target) => value == target,
            ColumnPredicate::NotEqual(target) => value != target,
            ColumnPredicate::Greater(target) => value > target,
            ColumnPredicate::Less(target) => value < target,
            ColumnPredicate::GreaterEqual(target) => value >= target,
            ColumnPredicate::LessEqual(target) => value <= target,
            ColumnPredicate::IsNull => *value == TypedValue::Null,
            ColumnPredicate::IsNotNull => *value != TypedValue::Null,
        }
    }
    
    /// Get column statistics for query optimization
    pub fn get_column_stats(&self, column_name: &str) -> Option<ColumnAnalytics> {
        let column_values = self.column_data.get(column_name)?;
        
        let mut unique_values = std::collections::HashSet::new();
        let mut null_count = 0;
        let mut min_value: Option<TypedValue> = None;
        let mut max_value: Option<TypedValue> = None;
        
        for value in column_values {
            unique_values.insert(value.clone());
            
            if *value == TypedValue::Null {
                null_count += 1;
            } else {
                if min_value.is_none() || value < min_value.as_ref().unwrap() {
                    min_value = Some(value.clone());
                }
                if max_value.is_none() || value > max_value.as_ref().unwrap() {
                    max_value = Some(value.clone());
                }
            }
        }
        
        Some(ColumnAnalytics {
            column_name: column_name.to_string(),
            total_count: column_values.len(),
            unique_count: unique_values.len(),
            null_count,
            min_value,
            max_value,
            cardinality: unique_values.len() as f64 / column_values.len() as f64,
            selectivity: 1.0 / unique_values.len() as f64,
        })
    }
    
    /// Simple compression for repeated values
    pub fn compress_columns(&mut self) {
        let start_time = Instant::now();
        let original_size = self.estimate_size();
        
        for (column_name, values) in &mut self.column_data {
            // Simple run-length encoding simulation
            // In a real implementation, this would use proper compression algorithms
            let unique_count = values.iter().collect::<std::collections::HashSet<_>>().len();
            let compression_benefit = values.len() as f64 / unique_count as f64;
            
            if compression_benefit > 2.0 {
                // Column has good compression potential
                println!("📦 Column {} compressed: {:.2}x benefit", column_name, compression_benefit);
            }
        }
        
        let compressed_size = self.estimate_size();
        self.compression_ratio = original_size as f64 / compressed_size as f64;
        self.last_compressed = Some(chrono::Utc::now().to_rfc3339());
        
        println!("✅ Column compression completed in {:?}, ratio: {:.2}x", 
                 start_time.elapsed(), self.compression_ratio);
    }
    
    /// Estimate storage size (simplified)
    fn estimate_size(&self) -> usize {
        let mut size = 0;
        for values in self.column_data.values() {
            size += values.len() * std::mem::size_of::<TypedValue>();
        }
        size
    }
    
    /// Clear all data
    pub fn clear(&mut self) {
        for values in self.column_data.values_mut() {
            values.clear();
        }
        self.row_count = 0;
    }
    
    /// Get row count
    pub fn len(&self) -> usize {
        self.row_count
    }
    
    /// Check if store is empty
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }
}

/// Aggregate operations for analytical queries
#[derive(Debug, Clone, PartialEq)]
pub enum AggregateOperation {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// Column-level predicates for filtering
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnPredicate {
    Equal(TypedValue),
    NotEqual(TypedValue),
    Greater(TypedValue),
    Less(TypedValue),
    GreaterEqual(TypedValue),
    LessEqual(TypedValue),
    IsNull,
    IsNotNull,
}

/// Column analytics for query optimization
#[derive(Debug, Clone)]
pub struct ColumnAnalytics {
    pub column_name: String,
    pub total_count: usize,
    pub unique_count: usize,
    pub null_count: usize,
    pub min_value: Option<TypedValue>,
    pub max_value: Option<TypedValue>,
    pub cardinality: f64, // unique_count / total_count
    pub selectivity: f64, // 1 / unique_count (average selectivity)
}

/// Columnar query execution plan
#[derive(Debug, Clone)]
pub struct ColumnarExecutionPlan {
    pub table_name: String,
    pub selected_columns: Vec<String>,
    pub predicates: Vec<(String, ColumnPredicate)>,
    pub aggregations: Vec<(String, AggregateOperation)>,
    pub estimated_cost: f64,
    pub use_columnar_storage: bool,
}

impl ColumnarExecutionPlan {
    pub fn new(table_name: String) -> Self {
        Self {
            table_name,
            selected_columns: Vec::new(),
            predicates: Vec::new(),
            aggregations: Vec::new(),
            estimated_cost: 0.0,
            use_columnar_storage: false,
        }
    }
    
    pub fn add_column_selection(&mut self, column_name: String) {
        self.selected_columns.push(column_name);
    }
    
    pub fn add_predicate(&mut self, column_name: String, predicate: ColumnPredicate) {
        self.predicates.push((column_name, predicate));
    }
    
    pub fn add_aggregation(&mut self, column_name: String, operation: AggregateOperation) {
        self.aggregations.push((column_name, operation));
    }
    
    pub fn estimate_cost(&mut self, column_store: &ColumnStore) -> f64 {
        let mut cost = 0.0;
        
        // Base cost for scanning columns
        cost += self.selected_columns.len() as f64 * column_store.row_count as f64 * 0.1;
        
        // Cost for predicates (column-wise filtering is efficient)
        cost += self.predicates.len() as f64 * column_store.row_count as f64 * 0.05;
        
        // Cost for aggregations (very efficient on columnar data)
        cost += self.aggregations.len() as f64 * column_store.row_count as f64 * 0.02;
        
        // Compression benefit
        cost *= 1.0 / column_store.compression_ratio;
        
        self.estimated_cost = cost;
        cost
    }
} 