use crate::table::Table;
use crate::row::Row;
use crate::types::TypedValue;
use crate::parser::{Condition, SqlValue};
// use crate::query_planner::{QueryPlanner, ExecutionPlan};
use rayon::prelude::*;
use std::sync::{Arc, RwLock};
use std::time::Instant;

/// Minimum number of rows to trigger parallel processing
const MIN_ROWS_FOR_PARALLEL: usize = 1000;

/// Number of rows per chunk for parallel processing
const ROWS_PER_CHUNK: usize = 500;

/// Parallel query executor that utilizes multiple CPU cores
#[derive(Debug, Clone)]
pub struct ParallelQueryExecutor {
    pub enabled: bool,
    pub min_rows_threshold: usize,
    pub chunk_size: usize,
    pub max_threads: Option<usize>,
}

impl ParallelQueryExecutor {
    pub fn new() -> Self {
        Self {
            enabled: true,
            min_rows_threshold: MIN_ROWS_FOR_PARALLEL,
            chunk_size: ROWS_PER_CHUNK,
            max_threads: None,
        }
    }

    pub fn with_settings(min_rows: usize, chunk_size: usize, max_threads: Option<usize>) -> Self {
        Self {
            enabled: true,
            min_rows_threshold: min_rows,
            chunk_size,
            max_threads,
        }
    }

    pub fn disable(&mut self) {
        self.enabled = false;
    }

    pub fn enable(&mut self) {
        self.enabled = true;
    }

    /// Executes a SELECT query with parallel processing
    pub fn execute_select_parallel(
        &self,
        table: &Table,
        where_condition: Option<&Condition>,
        columns: &[String],
    ) -> Vec<Row> {
        let start_time = Instant::now();
        
        // If table is small or parallel processing is disabled, use sequential processing
        if !self.enabled || table.rows.len() < self.min_rows_threshold {
            return self.execute_select_sequential(table, where_condition, columns);
        }

        // Configure thread pool if needed
        if let Some(max_threads) = self.max_threads {
            rayon::ThreadPoolBuilder::new()
                .num_threads(max_threads)
                .build_global()
                .unwrap_or_else(|_| {}); // Ignore if already configured
        }

        let result = if let Some(condition) = where_condition {
            self.parallel_filter_and_project(table, condition, columns)
        } else {
            self.parallel_project_only(table, columns)
        };

        println!("Parallel query execution time: {:?}", start_time.elapsed());
        result
    }

    /// Sequential fallback for small tables
    fn execute_select_sequential(
        &self,
        table: &Table,
        where_condition: Option<&Condition>,
        columns: &[String],
    ) -> Vec<Row> {
        let mut result = Vec::new();
        
        for row in &table.rows {
            // Apply WHERE condition if present
            if let Some(condition) = where_condition {
                if !self.evaluate_condition(condition, row) {
                    continue;
                }
            }
            
            // Project columns
            let projected_row = self.project_row(row, columns);
            result.push(projected_row);
        }
        
        result
    }

    /// Parallel filtering and projection
    fn parallel_filter_and_project(
        &self,
        table: &Table,
        condition: &Condition,
        columns: &[String],
    ) -> Vec<Row> {
        let rows = &table.rows;
        let columns_vec = columns.to_vec();
        
        // Create chunks for parallel processing
        let chunks: Vec<&[Row]> = rows.chunks(self.chunk_size).collect();
        
        // Process chunks in parallel
        let results: Vec<Vec<Row>> = chunks
            .into_par_iter()
            .map(|chunk| {
                let mut chunk_result = Vec::new();
                
                for row in chunk {
                    // Apply WHERE condition
                    if self.evaluate_condition(condition, row) {
                        // Project columns
                        let projected_row = self.project_row(row, &columns_vec);
                        chunk_result.push(projected_row);
                    }
                }
                
                chunk_result
            })
            .collect();
        
        // Combine results from all chunks
        let mut final_result = Vec::new();
        for chunk_result in results {
            final_result.extend(chunk_result);
        }
        
        final_result
    }

    /// Parallel projection only (no filtering)
    fn parallel_project_only(&self, table: &Table, columns: &[String]) -> Vec<Row> {
        let rows = &table.rows;
        let columns_vec = columns.to_vec();
        
        // Create chunks for parallel processing
        let chunks: Vec<&[Row]> = rows.chunks(self.chunk_size).collect();
        
        // Process chunks in parallel
        let results: Vec<Vec<Row>> = chunks
            .into_par_iter()
            .map(|chunk| {
                chunk
                    .iter()
                    .map(|row| self.project_row(row, &columns_vec))
                    .collect()
            })
            .collect();
        
        // Combine results from all chunks
        let mut final_result = Vec::new();
        for chunk_result in results {
            final_result.extend(chunk_result);
        }
        
        final_result
    }

    /// Parallel aggregation functions
    pub fn parallel_count(&self, table: &Table, where_condition: Option<&Condition>) -> i64 {
        if !self.enabled || table.rows.len() < self.min_rows_threshold {
            return self.sequential_count(table, where_condition);
        }

        let rows = &table.rows;
        let chunks: Vec<&[Row]> = rows.chunks(self.chunk_size).collect();
        
        let counts: Vec<i64> = chunks
            .into_par_iter()
            .map(|chunk| {
                let mut count = 0i64;
                for row in chunk {
                    if let Some(condition) = where_condition {
                        if self.evaluate_condition(condition, row) {
                            count += 1;
                        }
                    } else {
                        count += 1;
                    }
                }
                count
            })
            .collect();
        
        counts.into_iter().sum()
    }

    /// Parallel SUM aggregation
    pub fn parallel_sum(&self, table: &Table, column: &str, where_condition: Option<&Condition>) -> f64 {
        if !self.enabled || table.rows.len() < self.min_rows_threshold {
            return self.sequential_sum(table, column, where_condition);
        }

        let rows = &table.rows;
        let chunks: Vec<&[Row]> = rows.chunks(self.chunk_size).collect();
        let column_name = column.to_string();
        
        let sums: Vec<f64> = chunks
            .into_par_iter()
            .map(|chunk| {
                let mut sum = 0.0;
                for row in chunk {
                    if let Some(condition) = where_condition {
                        if !self.evaluate_condition(condition, row) {
                            continue;
                        }
                    }
                    
                    if let Some(value) = row.get(&column_name) {
                        match value {
                            TypedValue::Integer(i) => sum += *i as f64,
                            TypedValue::Text(s) => {
                                if let Ok(f) = s.parse::<f64>() {
                                    sum += f;
                                }
                            }
                            _ => {}
                        }
                    }
                }
                sum
            })
            .collect();
        
        sums.into_iter().sum()
    }

    /// Parallel AVG aggregation
    pub fn parallel_avg(&self, table: &Table, column: &str, where_condition: Option<&Condition>) -> f64 {
        let sum = self.parallel_sum(table, column, where_condition);
        let count = self.parallel_count(table, where_condition);
        
        if count > 0 {
            sum / count as f64
        } else {
            0.0
        }
    }

    /// Parallel MIN aggregation
    pub fn parallel_min(&self, table: &Table, column: &str, where_condition: Option<&Condition>) -> Option<TypedValue> {
        if !self.enabled || table.rows.len() < self.min_rows_threshold {
            return self.sequential_min(table, column, where_condition);
        }

        let rows = &table.rows;
        let chunks: Vec<&[Row]> = rows.chunks(self.chunk_size).collect();
        let column_name = column.to_string();
        
        let mins: Vec<Option<TypedValue>> = chunks
            .into_par_iter()
            .map(|chunk| {
                let mut min_val: Option<TypedValue> = None;
                
                for row in chunk {
                    if let Some(condition) = where_condition {
                        if !self.evaluate_condition(condition, row) {
                            continue;
                        }
                    }
                    
                    if let Some(value) = row.get(&column_name) {
                        match &min_val {
                            None => min_val = Some(value.clone()),
                            Some(current_min) => {
                                if value < current_min {
                                    min_val = Some(value.clone());
                                }
                            }
                        }
                    }
                }
                
                min_val
            })
            .collect();
        
        // Find global minimum
        let mut global_min: Option<TypedValue> = None;
        for min_val in mins {
            if let Some(val) = min_val {
                match &global_min {
                    None => global_min = Some(val),
                    Some(current_min) => {
                        if val < *current_min {
                            global_min = Some(val);
                        }
                    }
                }
            }
        }
        
        global_min
    }

    /// Parallel MAX aggregation
    pub fn parallel_max(&self, table: &Table, column: &str, where_condition: Option<&Condition>) -> Option<TypedValue> {
        if !self.enabled || table.rows.len() < self.min_rows_threshold {
            return self.sequential_max(table, column, where_condition);
        }

        let rows = &table.rows;
        let chunks: Vec<&[Row]> = rows.chunks(self.chunk_size).collect();
        let column_name = column.to_string();
        
        let maxs: Vec<Option<TypedValue>> = chunks
            .into_par_iter()
            .map(|chunk| {
                let mut max_val: Option<TypedValue> = None;
                
                for row in chunk {
                    if let Some(condition) = where_condition {
                        if !self.evaluate_condition(condition, row) {
                            continue;
                        }
                    }
                    
                    if let Some(value) = row.get(&column_name) {
                        match &max_val {
                            None => max_val = Some(value.clone()),
                            Some(current_max) => {
                                if value > current_max {
                                    max_val = Some(value.clone());
                                }
                            }
                        }
                    }
                }
                
                max_val
            })
            .collect();
        
        // Find global maximum
        let mut global_max: Option<TypedValue> = None;
        for max_val in maxs {
            if let Some(val) = max_val {
                match &global_max {
                    None => global_max = Some(val),
                    Some(current_max) => {
                        if val > *current_max {
                            global_max = Some(val);
                        }
                    }
                }
            }
        }
        
        global_max
    }

    /// Parallel JOIN operations
    pub fn parallel_join(
        &self,
        left_table: &Table,
        right_table: &Table,
        left_column: &str,
        right_column: &str,
        join_type: JoinType,
    ) -> Vec<Row> {
        // For large tables, use parallel processing
        if !self.enabled || left_table.rows.len() < self.min_rows_threshold {
            return self.sequential_join(left_table, right_table, left_column, right_column, join_type);
        }

        match join_type {
            JoinType::Inner => self.parallel_inner_join(left_table, right_table, left_column, right_column),
            JoinType::Left => self.parallel_left_join(left_table, right_table, left_column, right_column),
            JoinType::Right => self.parallel_right_join(left_table, right_table, left_column, right_column),
        }
    }

    /// Parallel inner join implementation
    fn parallel_inner_join(
        &self,
        left_table: &Table,
        right_table: &Table,
        left_column: &str,
        right_column: &str,
    ) -> Vec<Row> {
        // Create hash map from right table for efficient lookup
        let right_hash: std::collections::HashMap<TypedValue, Vec<&Row>> = {
            let mut hash = std::collections::HashMap::new();
            for row in &right_table.rows {
                if let Some(value) = row.get(right_column) {
                    hash.entry(value.clone()).or_insert_with(Vec::new).push(row);
                }
            }
            hash
        };

        let right_hash_arc = Arc::new(right_hash);
        let chunks: Vec<&[Row]> = left_table.rows.chunks(self.chunk_size).collect();
        let left_col = left_column.to_string();
        
        let results: Vec<Vec<Row>> = chunks
            .into_par_iter()
            .map(|chunk| {
                let mut chunk_result = Vec::new();
                let right_hash_ref = right_hash_arc.clone();
                
                for left_row in chunk {
                    if let Some(left_value) = left_row.get(&left_col) {
                        if let Some(right_rows) = right_hash_ref.get(left_value) {
                            for right_row in right_rows {
                                let joined_row = self.merge_rows(left_row, right_row);
                                chunk_result.push(joined_row);
                            }
                        }
                    }
                }
                
                chunk_result
            })
            .collect();
        
        // Combine results
        let mut final_result = Vec::new();
        for chunk_result in results {
            final_result.extend(chunk_result);
        }
        
        final_result
    }

    /// Parallel left join implementation
    fn parallel_left_join(
        &self,
        left_table: &Table,
        right_table: &Table,
        left_column: &str,
        right_column: &str,
    ) -> Vec<Row> {
        // Create hash map from right table for efficient lookup
        let right_hash: std::collections::HashMap<TypedValue, Vec<&Row>> = {
            let mut hash = std::collections::HashMap::new();
            for row in &right_table.rows {
                if let Some(value) = row.get(right_column) {
                    hash.entry(value.clone()).or_insert_with(Vec::new).push(row);
                }
            }
            hash
        };

        let right_hash_arc = Arc::new(right_hash);
        let chunks: Vec<&[Row]> = left_table.rows.chunks(self.chunk_size).collect();
        let left_col = left_column.to_string();
        
        let results: Vec<Vec<Row>> = chunks
            .into_par_iter()
            .map(|chunk| {
                let mut chunk_result = Vec::new();
                let right_hash_ref = right_hash_arc.clone();
                
                for left_row in chunk {
                    if let Some(left_value) = left_row.get(&left_col) {
                        if let Some(right_rows) = right_hash_ref.get(left_value) {
                            for right_row in right_rows {
                                let joined_row = self.merge_rows(left_row, right_row);
                                chunk_result.push(joined_row);
                            }
                        } else {
                            // Left join: include left row even if no match
                            chunk_result.push(left_row.clone());
                        }
                    } else {
                        // Include left row if join column is null
                        chunk_result.push(left_row.clone());
                    }
                }
                
                chunk_result
            })
            .collect();
        
        // Combine results
        let mut final_result = Vec::new();
        for chunk_result in results {
            final_result.extend(chunk_result);
        }
        
        final_result
    }

    /// Parallel right join implementation
    fn parallel_right_join(
        &self,
        left_table: &Table,
        right_table: &Table,
        left_column: &str,
        right_column: &str,
    ) -> Vec<Row> {
        // Right join is equivalent to left join with tables swapped
        self.parallel_left_join(right_table, left_table, right_column, left_column)
    }

    // Helper methods for sequential fallback
    fn sequential_count(&self, table: &Table, where_condition: Option<&Condition>) -> i64 {
        let mut count = 0i64;
        for row in &table.rows {
            if let Some(condition) = where_condition {
                if self.evaluate_condition(condition, row) {
                    count += 1;
                }
            } else {
                count += 1;
            }
        }
        count
    }

    fn sequential_sum(&self, table: &Table, column: &str, where_condition: Option<&Condition>) -> f64 {
        let mut sum = 0.0;
        for row in &table.rows {
            if let Some(condition) = where_condition {
                if !self.evaluate_condition(condition, row) {
                    continue;
                }
            }
            
            if let Some(value) = row.get(column) {
                match value {
                    TypedValue::Integer(i) => sum += *i as f64,
                    TypedValue::Text(s) => {
                        if let Ok(f) = s.parse::<f64>() {
                            sum += f;
                        }
                    }
                    _ => {}
                }
            }
        }
        sum
    }

    fn sequential_min(&self, table: &Table, column: &str, where_condition: Option<&Condition>) -> Option<TypedValue> {
        let mut min_val: Option<TypedValue> = None;
        
        for row in &table.rows {
            if let Some(condition) = where_condition {
                if !self.evaluate_condition(condition, row) {
                    continue;
                }
            }
            
            if let Some(value) = row.get(column) {
                match &min_val {
                    None => min_val = Some(value.clone()),
                    Some(current_min) => {
                        if value < current_min {
                            min_val = Some(value.clone());
                        }
                    }
                }
            }
        }
        
        min_val
    }

    fn sequential_max(&self, table: &Table, column: &str, where_condition: Option<&Condition>) -> Option<TypedValue> {
        let mut max_val: Option<TypedValue> = None;
        
        for row in &table.rows {
            if let Some(condition) = where_condition {
                if !self.evaluate_condition(condition, row) {
                    continue;
                }
            }
            
            if let Some(value) = row.get(column) {
                match &max_val {
                    None => max_val = Some(value.clone()),
                    Some(current_max) => {
                        if value > current_max {
                            max_val = Some(value.clone());
                        }
                    }
                }
            }
        }
        
        max_val
    }

    fn sequential_join(
        &self,
        left_table: &Table,
        right_table: &Table,
        left_column: &str,
        right_column: &str,
        join_type: JoinType,
    ) -> Vec<Row> {
        let mut result = Vec::new();
        
        match join_type {
            JoinType::Inner => {
                for left_row in &left_table.rows {
                    if let Some(left_value) = left_row.get(left_column) {
                        for right_row in &right_table.rows {
                            if let Some(right_value) = right_row.get(right_column) {
                                if left_value == right_value {
                                    let joined_row = self.merge_rows(left_row, right_row);
                                    result.push(joined_row);
                                }
                            }
                        }
                    }
                }
            }
            JoinType::Left => {
                for left_row in &left_table.rows {
                    let mut found_match = false;
                    
                    if let Some(left_value) = left_row.get(left_column) {
                        for right_row in &right_table.rows {
                            if let Some(right_value) = right_row.get(right_column) {
                                if left_value == right_value {
                                    let joined_row = self.merge_rows(left_row, right_row);
                                    result.push(joined_row);
                                    found_match = true;
                                }
                            }
                        }
                    }
                    
                    if !found_match {
                        result.push(left_row.clone());
                    }
                }
            }
            JoinType::Right => {
                // Right join is equivalent to left join with tables swapped
                return self.sequential_join(right_table, left_table, right_column, left_column, JoinType::Left);
            }
        }
        
        result
    }

    // Helper methods
    fn evaluate_condition(&self, condition: &Condition, row: &Row) -> bool {
        match condition {
            Condition::Equal(column, value) => {
                if let Some(row_value) = row.get(column) {
                    self.compare_sql_value(row_value, value)
                } else {
                    false
                }
            }
            Condition::NotEqual(column, value) => {
                if let Some(row_value) = row.get(column) {
                    !self.compare_sql_value(row_value, value)
                } else {
                    false
                }
            }
            Condition::Greater(column, value) => {
                if let Some(row_value) = row.get(column) {
                    self.compare_greater(row_value, value)
                } else {
                    false
                }
            }
            Condition::Less(column, value) => {
                if let Some(row_value) = row.get(column) {
                    self.compare_less(row_value, value)
                } else {
                    false
                }
            }
            Condition::GreaterEqual(column, value) => {
                if let Some(row_value) = row.get(column) {
                    self.compare_greater_equal(row_value, value)
                } else {
                    false
                }
            }
            Condition::LessEqual(column, value) => {
                if let Some(row_value) = row.get(column) {
                    self.compare_less_equal(row_value, value)
                } else {
                    false
                }
            }
            Condition::And(left, right) => {
                self.evaluate_condition(left, row) && self.evaluate_condition(right, row)
            }
            Condition::Or(left, right) => {
                self.evaluate_condition(left, row) || self.evaluate_condition(right, row)
            }
        }
    }

    fn compare_sql_value(&self, typed_value: &TypedValue, sql_value: &SqlValue) -> bool {
        match (typed_value, sql_value) {
            (TypedValue::Integer(i), SqlValue::Integer(j)) => i == j,
            (TypedValue::Text(s), SqlValue::Text(t)) => s == t,
            (TypedValue::Boolean(b), SqlValue::Boolean(c)) => b == c,
            _ => false,
        }
    }

    fn compare_greater(&self, typed_value: &TypedValue, sql_value: &SqlValue) -> bool {
        match (typed_value, sql_value) {
            (TypedValue::Integer(i), SqlValue::Integer(j)) => i > j,
            (TypedValue::Text(s), SqlValue::Text(t)) => s > t,
            _ => false,
        }
    }

    fn compare_less(&self, typed_value: &TypedValue, sql_value: &SqlValue) -> bool {
        match (typed_value, sql_value) {
            (TypedValue::Integer(i), SqlValue::Integer(j)) => i < j,
            (TypedValue::Text(s), SqlValue::Text(t)) => s < t,
            _ => false,
        }
    }

    fn compare_greater_equal(&self, typed_value: &TypedValue, sql_value: &SqlValue) -> bool {
        match (typed_value, sql_value) {
            (TypedValue::Integer(i), SqlValue::Integer(j)) => i >= j,
            (TypedValue::Text(s), SqlValue::Text(t)) => s >= t,
            _ => false,
        }
    }

    fn compare_less_equal(&self, typed_value: &TypedValue, sql_value: &SqlValue) -> bool {
        match (typed_value, sql_value) {
            (TypedValue::Integer(i), SqlValue::Integer(j)) => i <= j,
            (TypedValue::Text(s), SqlValue::Text(t)) => s <= t,
            _ => false,
        }
    }

    fn project_row(&self, row: &Row, columns: &[String]) -> Row {
        let mut new_row = Row::new();
        
        for column in columns {
            if let Some(value) = row.get(column) {
                new_row.insert(column.clone(), value.clone());
            }
        }
        
        new_row
    }

    fn merge_rows(&self, left_row: &Row, right_row: &Row) -> Row {
        let mut merged_row = Row::new();
        
        // Add all columns from left row
        for (column, value) in left_row.get_all() {
            merged_row.insert(column.clone(), value.clone());
        }
        
        // Add all columns from right row (with potential conflicts)
        for (column, value) in right_row.get_all() {
            let prefixed_column = format!("right_{}", column);
            merged_row.insert(prefixed_column, value.clone());
        }
        
        merged_row
    }
}

impl Default for ParallelQueryExecutor {
    fn default() -> Self {
        Self::new()
    }
}

/// Join types for parallel JOIN operations
#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
}

/// Thread-safe statistics collector for parallel operations
pub struct ParallelStats {
    total_queries: Arc<RwLock<u64>>,
    total_parallel_queries: Arc<RwLock<u64>>,
    total_execution_time: Arc<RwLock<std::time::Duration>>,
    parallel_execution_time: Arc<RwLock<std::time::Duration>>,
}

impl ParallelStats {
    pub fn new() -> Self {
        Self {
            total_queries: Arc::new(RwLock::new(0)),
            total_parallel_queries: Arc::new(RwLock::new(0)),
            total_execution_time: Arc::new(RwLock::new(std::time::Duration::new(0, 0))),
            parallel_execution_time: Arc::new(RwLock::new(std::time::Duration::new(0, 0))),
        }
    }

    pub fn record_query(&self, execution_time: std::time::Duration, was_parallel: bool) {
        if let Ok(mut total) = self.total_queries.write() {
            *total += 1;
        }
        
        if let Ok(mut total_time) = self.total_execution_time.write() {
            *total_time += execution_time;
        }
        
        if was_parallel {
            if let Ok(mut parallel) = self.total_parallel_queries.write() {
                *parallel += 1;
            }
            
            if let Ok(mut parallel_time) = self.parallel_execution_time.write() {
                *parallel_time += execution_time;
            }
        }
    }

    pub fn get_stats(&self) -> (u64, u64, std::time::Duration, std::time::Duration) {
        let total_queries = self.total_queries.read().unwrap_or_else(|_| {
            eprintln!("Failed to read total_queries");
            std::process::exit(1);
        });
        let total_parallel = self.total_parallel_queries.read().unwrap_or_else(|_| {
            eprintln!("Failed to read total_parallel_queries");
            std::process::exit(1);
        });
        let total_time = self.total_execution_time.read().unwrap_or_else(|_| {
            eprintln!("Failed to read total_execution_time");
            std::process::exit(1);
        });
        let parallel_time = self.parallel_execution_time.read().unwrap_or_else(|_| {
            eprintln!("Failed to read parallel_execution_time");
            std::process::exit(1);
        });
        
        (*total_queries, *total_parallel, *total_time, *parallel_time)
    }

    pub fn parallel_efficiency(&self) -> f64 {
        let (total_queries, parallel_queries, total_time, parallel_time) = self.get_stats();
        
        if total_queries > 0 && parallel_queries > 0 {
            let sequential_queries = total_queries - parallel_queries;
            let sequential_time = total_time - parallel_time;
            
            if sequential_queries > 0 {
                let avg_sequential_time = sequential_time.as_secs_f64() / sequential_queries as f64;
                let avg_parallel_time = parallel_time.as_secs_f64() / parallel_queries as f64;
                
                if avg_parallel_time > 0.0 {
                    return avg_sequential_time / avg_parallel_time;
                }
            }
        }
        
        1.0
    }
}

impl Default for ParallelStats {
    fn default() -> Self {
        Self::new()
    }
} 