use crate::table::{Table, Index};
use crate::parser::{Condition, WhereClause, SqlValue};
use crate::types::TypedValue;
use std::collections::HashMap;
use std::time::Instant;

/// Query optimization planner that uses statistics for cost-based decisions
#[derive(Debug, Clone)]
pub struct QueryPlanner {
    pub enable_cache: bool,
    pub enable_predicate_pushdown: bool,
    pub enable_index_hints: bool,
}

impl QueryPlanner {
    pub fn new() -> Self {
        Self {
            enable_cache: true,
            enable_predicate_pushdown: true,
            enable_index_hints: true,
        }
    }

    /// Creates an optimized execution plan for a SELECT query
    pub fn plan_select_query(&self, table: &Table, where_clause: &Option<WhereClause>) -> ExecutionPlan {
        let mut plan = ExecutionPlan::new();
        
        // Step 1: Analyze WHERE conditions if present
        if let Some(where_clause) = where_clause {
            let optimized_conditions = self.optimize_where_conditions(table, &where_clause.condition);
            plan.where_conditions = Some(optimized_conditions);
        }
        
        // Step 2: Select best index strategy
        plan.index_strategy = self.select_index_strategy(table, &plan.where_conditions);
        
        // Step 3: Estimate costs
        plan.estimated_cost = self.estimate_query_cost(table, &plan);
        
        // Step 4: Generate execution steps
        plan.execution_steps = self.generate_execution_steps(table, &plan);
        
        plan
    }

    /// Optimizes WHERE conditions by ordering them based on selectivity
    fn optimize_where_conditions(&self, table: &Table, condition: &Condition) -> OptimizedCondition {
        match condition {
            Condition::Equal(column, value) => {
                let selectivity = self.calculate_selectivity(table, column, value);
                OptimizedCondition::Equal {
                    column: column.clone(),
                    value: value.clone(),
                    selectivity,
                    has_index: table.indexes.contains_key(column),
                }
            },
            Condition::NotEqual(column, value) => {
                let selectivity = 1.0 - self.calculate_selectivity(table, column, value);
                OptimizedCondition::NotEqual {
                    column: column.clone(),
                    value: value.clone(),
                    selectivity,
                    has_index: table.indexes.contains_key(column),
                }
            },
            Condition::Greater(column, value) => {
                let selectivity = self.calculate_range_selectivity(table, column, value, true);
                OptimizedCondition::Greater {
                    column: column.clone(),
                    value: value.clone(),
                    selectivity,
                    has_index: table.indexes.contains_key(column),
                }
            },
            Condition::Less(column, value) => {
                let selectivity = self.calculate_range_selectivity(table, column, value, false);
                OptimizedCondition::Less {
                    column: column.clone(),
                    value: value.clone(),
                    selectivity,
                    has_index: table.indexes.contains_key(column),
                }
            },
            Condition::GreaterEqual(column, value) => {
                let selectivity = self.calculate_range_selectivity(table, column, value, true);
                OptimizedCondition::GreaterEqual {
                    column: column.clone(),
                    value: value.clone(),
                    selectivity,
                    has_index: table.indexes.contains_key(column),
                }
            },
            Condition::LessEqual(column, value) => {
                let selectivity = self.calculate_range_selectivity(table, column, value, false);
                OptimizedCondition::LessEqual {
                    column: column.clone(),
                    value: value.clone(),
                    selectivity,
                    has_index: table.indexes.contains_key(column),
                }
            },
            Condition::And(left, right) => {
                let left_opt = self.optimize_where_conditions(table, left);
                let right_opt = self.optimize_where_conditions(table, right);
                
                // Order by selectivity: most selective first
                if left_opt.get_selectivity() < right_opt.get_selectivity() {
                    OptimizedCondition::And(Box::new(left_opt), Box::new(right_opt))
                } else {
                    OptimizedCondition::And(Box::new(right_opt), Box::new(left_opt))
                }
            },
            Condition::Or(left, right) => {
                let left_opt = self.optimize_where_conditions(table, left);
                let right_opt = self.optimize_where_conditions(table, right);
                OptimizedCondition::Or(Box::new(left_opt), Box::new(right_opt))
            },
        }
    }

    /// Calculates selectivity for equality conditions
    fn calculate_selectivity(&self, table: &Table, column: &str, value: &SqlValue) -> f64 {
        if let Some(col_stats) = table.stats.column_stats.get(column) {
            if col_stats.total_count == 0 {
                return 1.0;
            }
            
            // Convert SqlValue to TypedValue for lookup
            if let Ok(typed_value) = self.convert_sql_to_typed_value(value) {
                if let Some(frequency) = col_stats.value_frequency.get(&typed_value) {
                    return *frequency as f64 / col_stats.total_count as f64;
                }
            }
            
            // If value not found in frequency, estimate based on unique count
            if col_stats.unique_count > 0 {
                return 1.0 / col_stats.unique_count as f64;
            }
        }
        
        // Default estimate
        0.1
    }

    /// Calculates selectivity for range conditions
    fn calculate_range_selectivity(&self, table: &Table, column: &str, value: &SqlValue, _is_greater: bool) -> f64 {
        if let Some(col_stats) = table.stats.column_stats.get(column) {
            if col_stats.total_count == 0 {
                return 1.0;
            }
            
            // Estimate based on min/max values if available
            if let (Some(_min_val), Some(_max_val)) = (&col_stats.min_value, &col_stats.max_value) {
                if let Ok(_typed_value) = self.convert_sql_to_typed_value(value) {
                    // Simple range estimation (can be improved with histograms)
                    return 0.33; // Assume 1/3 selectivity for range queries
                }
            }
        }
        
        // Default estimate for range queries
        0.33
    }

    /// Selects the best index strategy based on conditions
    fn select_index_strategy(&self, table: &Table, conditions: &Option<OptimizedCondition>) -> IndexStrategy {
        if let Some(condition) = conditions {
            match condition {
                OptimizedCondition::Equal { column, has_index: true, .. } => {
                    if let Some(index) = table.indexes.get(column) {
                        return IndexStrategy::HashLookup {
                            column: column.clone(),
                            index_type: match index {
                                Index::Hash(_) => IndexType::Hash,
                                Index::BTree(_) => IndexType::BTree,
                            }
                        };
                    }
                },
                OptimizedCondition::Greater { column, has_index: true, .. } |
                OptimizedCondition::Less { column, has_index: true, .. } |
                OptimizedCondition::GreaterEqual { column, has_index: true, .. } |
                OptimizedCondition::LessEqual { column, has_index: true, .. } => {
                    if let Some(Index::BTree(_)) = table.indexes.get(column) {
                        return IndexStrategy::BTreeRange {
                            column: column.clone(),
                        };
                    }
                },
                OptimizedCondition::And(left, right) => {
                    // Check if we can use index for the most selective condition
                    let left_selectivity = left.get_selectivity();
                    let right_selectivity = right.get_selectivity();
                    
                    if left_selectivity < right_selectivity {
                        return self.select_index_strategy(table, &Some((**left).clone()));
                    } else {
                        return self.select_index_strategy(table, &Some((**right).clone()));
                    }
                },
                _ => {}
            }
        }
        
        IndexStrategy::FullScan
    }

    /// Estimates the cost of executing a query
    fn estimate_query_cost(&self, table: &Table, plan: &ExecutionPlan) -> f64 {
        let total_rows = table.rows.len() as f64;
        
        match &plan.index_strategy {
            IndexStrategy::HashLookup { .. } => {
                // O(1) hash lookup + result processing
                1.0 + (total_rows * 0.001) // Very fast
            },
            IndexStrategy::BTreeRange { .. } => {
                // O(log n) + range scan
                total_rows.log2() + (total_rows * 0.1) // Fast for ranges
            },
            IndexStrategy::FullScan => {
                // O(n) full table scan
                total_rows // Slowest
            },
        }
    }

    /// Generates execution steps for the query
    fn generate_execution_steps(&self, _table: &Table, plan: &ExecutionPlan) -> Vec<ExecutionStep> {
        let mut steps = Vec::new();
        
        match &plan.index_strategy {
            IndexStrategy::HashLookup { column, index_type } => {
                steps.push(ExecutionStep::IndexLookup {
                    column: column.clone(),
                    index_type: index_type.clone(),
                });
            },
            IndexStrategy::BTreeRange { column } => {
                steps.push(ExecutionStep::IndexRangeScan {
                    column: column.clone(),
                });
            },
            IndexStrategy::FullScan => {
                steps.push(ExecutionStep::TableScan);
            },
        }
        
        if plan.where_conditions.is_some() {
            steps.push(ExecutionStep::FilterRows);
        }
        
        steps.push(ExecutionStep::FormatOutput);
        
        steps
    }

    /// Converts SqlValue to TypedValue for statistics lookup
    fn convert_sql_to_typed_value(&self, sql_value: &SqlValue) -> Result<TypedValue, String> {
        match sql_value {
            SqlValue::Integer(i) => Ok(TypedValue::Integer(*i)),
            SqlValue::Text(s) => Ok(TypedValue::Text(s.clone())),
            SqlValue::Boolean(b) => Ok(TypedValue::Boolean(*b)),
            SqlValue::Null => Ok(TypedValue::Null),
        }
    }
}

/// Optimized condition with selectivity information
#[derive(Debug, Clone)]
pub enum OptimizedCondition {
    Equal { column: String, value: SqlValue, selectivity: f64, has_index: bool },
    NotEqual { column: String, value: SqlValue, selectivity: f64, has_index: bool },
    Greater { column: String, value: SqlValue, selectivity: f64, has_index: bool },
    Less { column: String, value: SqlValue, selectivity: f64, has_index: bool },
    GreaterEqual { column: String, value: SqlValue, selectivity: f64, has_index: bool },
    LessEqual { column: String, value: SqlValue, selectivity: f64, has_index: bool },
    And(Box<OptimizedCondition>, Box<OptimizedCondition>),
    Or(Box<OptimizedCondition>, Box<OptimizedCondition>),
}

impl OptimizedCondition {
    pub fn get_selectivity(&self) -> f64 {
        match self {
            OptimizedCondition::Equal { selectivity, .. } |
            OptimizedCondition::NotEqual { selectivity, .. } |
            OptimizedCondition::Greater { selectivity, .. } |
            OptimizedCondition::Less { selectivity, .. } |
            OptimizedCondition::GreaterEqual { selectivity, .. } |
            OptimizedCondition::LessEqual { selectivity, .. } => *selectivity,
            OptimizedCondition::And(left, right) => {
                left.get_selectivity() * right.get_selectivity()
            },
            OptimizedCondition::Or(left, right) => {
                left.get_selectivity() + right.get_selectivity() - (left.get_selectivity() * right.get_selectivity())
            },
        }
    }
}

/// Index strategy to use for the query
#[derive(Debug, Clone)]
pub enum IndexStrategy {
    HashLookup { column: String, index_type: IndexType },
    BTreeRange { column: String },
    FullScan,
}

/// Index type for strategy
#[derive(Debug, Clone)]
pub enum IndexType {
    Hash,
    BTree,
}

/// Individual execution step
#[derive(Debug, Clone)]
pub enum ExecutionStep {
    IndexLookup { column: String, index_type: IndexType },
    IndexRangeScan { column: String },
    TableScan,
    FilterRows,
    FormatOutput,
}

/// Complete execution plan for a query
#[derive(Debug)]
pub struct ExecutionPlan {
    pub where_conditions: Option<OptimizedCondition>,
    pub index_strategy: IndexStrategy,
    pub estimated_cost: f64,
    pub execution_steps: Vec<ExecutionStep>,
}

impl ExecutionPlan {
    pub fn new() -> Self {
        Self {
            where_conditions: None,
            index_strategy: IndexStrategy::FullScan,
            estimated_cost: 0.0,
            execution_steps: Vec::new(),
        }
    }

    /// Formats the execution plan for EXPLAIN output
    pub fn format_explain(&self) -> String {
        let mut result = String::new();
        result.push_str("📊 QUERY EXECUTION PLAN\n");
        result.push_str("========================\n");
        
        result.push_str(&format!("💰 Estimated Cost: {:.2}\n", self.estimated_cost));
        
        result.push_str(&format!("🎯 Index Strategy: {:?}\n", self.index_strategy));
        
        if let Some(condition) = &self.where_conditions {
            result.push_str(&format!("📋 WHERE Optimization: Selectivity = {:.4}\n", condition.get_selectivity()));
        }
        
        result.push_str("\n📝 Execution Steps:\n");
        for (i, step) in self.execution_steps.iter().enumerate() {
            result.push_str(&format!("  {}. {:?}\n", i + 1, step));
        }
        
        result
    }
}

/// Query cache entry
#[derive(Debug, Clone)]
pub struct QueryCacheEntry {
    pub query_pattern: String,
    pub result: String, // Cached result as JSON
    pub created_at: std::time::Instant,
    pub hit_count: usize,
}

/// Query cache for storing frequent query results
#[derive(Debug, Clone)]
pub struct QueryCache {
    pub entries: HashMap<String, QueryCacheEntry>,
    pub max_size: usize,
    pub ttl_seconds: u64,
}

impl QueryCache {
    pub fn new(max_size: usize, ttl_seconds: u64) -> Self {
        Self {
            entries: HashMap::new(),
            max_size,
            ttl_seconds,
        }
    }

    pub fn get(&mut self, query_pattern: &str) -> Option<String> {
        if let Some(entry) = self.entries.get_mut(query_pattern) {
            // Check if entry is still valid
            if entry.created_at.elapsed().as_secs() < self.ttl_seconds {
                entry.hit_count += 1;
                return Some(entry.result.clone());
            } else {
                // Remove expired entry
                self.entries.remove(query_pattern);
            }
        }
        None
    }

    pub fn put(&mut self, query_pattern: String, result: String) {
        // Remove old entries if cache is full
        if self.entries.len() >= self.max_size {
            self.evict_oldest();
        }

        self.entries.insert(query_pattern.clone(), QueryCacheEntry {
            query_pattern,
            result,
            created_at: Instant::now(),
            hit_count: 0,
        });
    }

    fn evict_oldest(&mut self) {
        if let Some(oldest_key) = self.entries.iter()
            .min_by_key(|(_, entry)| entry.created_at)
            .map(|(key, _)| key.clone()) {
            self.entries.remove(&oldest_key);
        }
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }
}

impl Default for QueryCache {
    fn default() -> Self {
        Self::new(50, 300) // 50 entries, 5 minutes TTL
    }
} 