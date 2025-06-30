// query_engine.rs - Query execution engine for SQL-like queries

use crate::ast::*;
use crate::{DatabaseError, MemoryStorage, CrudDatabase};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

/// Query execution engine that runs parsed SQL queries on the database
pub struct QueryEngine<T> {
    storage: Arc<MemoryStorage<T>>,
    collections: HashMap<String, Arc<MemoryStorage<Value>>>,
}

/// Result of query execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub rows: Vec<Value>,
    pub rows_affected: usize,
    pub execution_time_ms: u64,
}

impl QueryResult {
    pub fn new(rows: Vec<Value>, rows_affected: usize, execution_time_ms: u64) -> Self {
        Self {
            rows,
            rows_affected,
            execution_time_ms,
        }
    }

    pub fn empty() -> Self {
        Self::new(Vec::new(), 0, 0)
    }

    pub fn with_rows(rows: Vec<Value>) -> Self {
        let count = rows.len();
        Self::new(rows, count, 0)
    }

    pub fn with_affected_rows(count: usize) -> Self {
        Self::new(Vec::new(), count, 0)
    }
}

impl<T> QueryEngine<T>
where
    T: Serialize + for<'de> Deserialize<'de> + Clone + Send + Sync + 'static,
{
    /// Creates a new query engine
    pub fn new(storage: Arc<MemoryStorage<T>>) -> Self {
        Self {
            storage,
            collections: HashMap::new(),
        }
    }

    /// Creates or gets a collection storage
    pub fn get_or_create_collection(&mut self, name: &str) -> Arc<MemoryStorage<Value>> {
        self.collections
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(MemoryStorage::new()))
            .clone()
    }

    /// Gets an existing collection or returns error if not found
    pub fn get_collection(&self, name: &str) -> Result<Arc<MemoryStorage<Value>>, DatabaseError> {
        self.collections
            .get(name)
            .cloned()
            .ok_or_else(|| DatabaseError::CollectionNotFound {
                collection: name.to_string(),
            })
    }

    /// Executes a parsed SQL query
    pub async fn execute(&mut self, query: Query) -> Result<QueryResult, DatabaseError> {
        let start_time = std::time::Instant::now();
        
        let result = match query {
            Query::Select(select_query) => self.execute_select(select_query).await,
            Query::Insert(insert_query) => self.execute_insert(insert_query).await,
            Query::Update(update_query) => self.execute_update(update_query).await,
            Query::Delete(delete_query) => self.execute_delete(delete_query).await,
            Query::Create(create_query) => self.execute_create(create_query).await,
        };

        let execution_time = start_time.elapsed().as_millis() as u64;

        // Add execution time to result
        result.map(|mut r| {
            r.execution_time_ms = execution_time;
            r
        })
    }

    /// Executes SELECT query
    async fn execute_select(&mut self, query: SelectQuery) -> Result<QueryResult, DatabaseError> {
        let collection = self.get_collection(&query.from)?;
        
        // Get all documents from collection
        let all_docs = collection.read_all(None, None).await?;
        
        // Convert documents to JSON values for filtering
        let mut rows: Vec<Value> = all_docs
            .into_iter()
            .map(|doc| serde_json::to_value(&doc.data).unwrap_or(Value::Null))
            .collect();

        // Apply WHERE clause filtering
        if let Some(condition) = &query.where_clause {
            rows = self.filter_rows(rows, condition)?;
        }

        // Apply ORDER BY sorting
        if let Some(order_by) = &query.order_by {
            rows = self.sort_rows(rows, order_by)?;
        }

        // Apply OFFSET
        if let Some(offset) = query.offset {
            rows = rows.into_iter().skip(offset).collect();
        }

        // Apply LIMIT
        if let Some(limit) = query.limit {
            rows = rows.into_iter().take(limit).collect();
        }

        // Apply field selection/projection
        rows = self.project_fields(rows, &query.fields)?;

        Ok(QueryResult::with_rows(rows))
    }

    /// Executes INSERT query
    async fn execute_insert(&mut self, query: InsertQuery) -> Result<QueryResult, DatabaseError> {
        let collection = self.get_collection(&query.into)?;

        if query.fields.len() != query.values.len() {
            return Err(DatabaseError::InvalidQuery {
                message: format!(
                    "Field count ({}) doesn't match value count ({})",
                    query.fields.len(),
                    query.values.len()
                ),
            });
        }

        // Create JSON object from fields and values
        let mut document_data = serde_json::Map::new();
        for (field, value) in query.fields.iter().zip(query.values.iter()) {
            document_data.insert(field.clone(), value.clone());
        }

        let document_value = Value::Object(document_data);
        
        // Insert the document
        collection.create(document_value).await?;

        Ok(QueryResult::with_affected_rows(1))
    }

    /// Executes UPDATE query
    async fn execute_update(&mut self, query: UpdateQuery) -> Result<QueryResult, DatabaseError> {
        let collection = self.get_collection(&query.table)?;
        
        // Get all documents
        let all_docs = collection.read_all(None, None).await?;
        let mut updated_count = 0;

        for doc in all_docs {
            let doc_value = serde_json::to_value(&doc.data)?;
            
            // Check if document matches WHERE condition
            let matches = if let Some(condition) = &query.where_clause {
                self.evaluate_condition(&doc_value, condition)?
            } else {
                true // No WHERE clause means update all
            };

            if matches {
                // Apply SET assignments
                let mut updated_data = doc_value;
                for assignment in &query.set {
                    self.apply_assignment(&mut updated_data, assignment)?;
                }

                // Update the document
                collection.update(&doc.metadata.id, updated_data).await?;
                updated_count += 1;
            }
        }

        Ok(QueryResult::with_affected_rows(updated_count))
    }

    /// Executes DELETE query
    async fn execute_delete(&mut self, query: DeleteQuery) -> Result<QueryResult, DatabaseError> {
        let collection = self.get_collection(&query.from)?;
        
        // Get all documents
        let all_docs = collection.read_all(None, None).await?;
        let mut deleted_count = 0;

        for doc in all_docs {
            let doc_value = serde_json::to_value(&doc.data)?;
            
            // Check if document matches WHERE condition
            let matches = if let Some(condition) = &query.where_clause {
                self.evaluate_condition(&doc_value, condition)?
            } else {
                true // No WHERE clause means delete all
            };

            if matches {
                collection.delete(&doc.metadata.id).await?;
                deleted_count += 1;
            }
        }

        Ok(QueryResult::with_affected_rows(deleted_count))
    }

    /// Executes CREATE query
    async fn execute_create(&mut self, query: CreateQuery) -> Result<QueryResult, DatabaseError> {
        // Create new collection (just add to our collections map)
        self.get_or_create_collection(&query.collection_name);
        
        Ok(QueryResult::with_affected_rows(1))
    }

    /// Filters rows based on WHERE condition
    fn filter_rows(&self, rows: Vec<Value>, condition: &Condition) -> Result<Vec<Value>, DatabaseError> {
        let mut filtered = Vec::new();
        
        for row in rows {
            if self.evaluate_condition(&row, condition)? {
                filtered.push(row);
            }
        }
        
        Ok(filtered)
    }

    /// Evaluates a condition against a document
    fn evaluate_condition(&self, document: &Value, condition: &Condition) -> Result<bool, DatabaseError> {
        match condition {
            Condition::Comparison { field, operator, value } => {
                let field_value = self.get_field_value(document, field);
                self.compare_values(&field_value, operator, value)
            }
            Condition::And(left, right) => {
                let left_result = self.evaluate_condition(document, left)?;
                let right_result = self.evaluate_condition(document, right)?;
                Ok(left_result && right_result)
            }
            Condition::Or(left, right) => {
                let left_result = self.evaluate_condition(document, left)?;
                let right_result = self.evaluate_condition(document, right)?;
                Ok(left_result || right_result)
            }
            Condition::Not(inner) => {
                let inner_result = self.evaluate_condition(document, inner)?;
                Ok(!inner_result)
            }
            Condition::Parenthesized(inner) => {
                self.evaluate_condition(document, inner)
            }
        }
    }

    /// Gets field value from document (supports nested fields with dot notation)
    fn get_field_value(&self, document: &Value, field_path: &str) -> Value {
        let parts: Vec<&str> = field_path.split('.').collect();
        let mut current = document;

        for part in parts {
            match current {
                Value::Object(obj) => {
                    if let Some(value) = obj.get(part) {
                        current = value;
                    } else {
                        return Value::Null;
                    }
                }
                _ => return Value::Null,
            }
        }

        current.clone()
    }

    /// Compares two values using the given operator
    fn compare_values(&self, left: &Value, operator: &ComparisonOperator, right: &Value) -> Result<bool, DatabaseError> {
        match operator {
            ComparisonOperator::Equal => Ok(left == right),
            ComparisonOperator::NotEqual => Ok(left != right),
            ComparisonOperator::GreaterThan => self.compare_numbers(left, right, |a, b| a > b),
            ComparisonOperator::GreaterThanOrEqual => self.compare_numbers(left, right, |a, b| a >= b),
            ComparisonOperator::LessThan => self.compare_numbers(left, right, |a, b| a < b),
            ComparisonOperator::LessThanOrEqual => self.compare_numbers(left, right, |a, b| a <= b),
            ComparisonOperator::Like => self.compare_like(left, right),
            ComparisonOperator::NotLike => Ok(!self.compare_like(left, right)?),
            ComparisonOperator::In => self.compare_in(left, right),
            ComparisonOperator::NotIn => Ok(!self.compare_in(left, right)?),
            ComparisonOperator::IsNull => Ok(left.is_null()),
            ComparisonOperator::IsNotNull => Ok(!left.is_null()),
        }
    }

    /// Compares numbers with the given comparison function
    fn compare_numbers<F>(&self, left: &Value, right: &Value, compare_fn: F) -> Result<bool, DatabaseError>
    where
        F: Fn(f64, f64) -> bool,
    {
        match (left, right) {
            (Value::Number(l), Value::Number(r)) => {
                if let (Some(l_float), Some(r_float)) = (l.as_f64(), r.as_f64()) {
                    Ok(compare_fn(l_float, r_float))
                } else {
                    Ok(false)
                }
            }
            _ => Ok(false),
        }
    }

    /// Compares strings using LIKE operator (basic pattern matching)
    fn compare_like(&self, left: &Value, right: &Value) -> Result<bool, DatabaseError> {
        match (left, right) {
            (Value::String(text), Value::String(pattern)) => {
                // Simple LIKE implementation: % = wildcard, _ = single char
                // For simplicity, just use contains for now
                Ok(text.to_lowercase().contains(&pattern.replace("%", "").to_lowercase()))
            }
            _ => Ok(false),
        }
    }

    /// Compares value against array using IN operator
    fn compare_in(&self, left: &Value, right: &Value) -> Result<bool, DatabaseError> {
        match right {
            Value::Array(arr) => Ok(arr.contains(left)),
            _ => Ok(false),
        }
    }

    /// Sorts rows based on ORDER BY clauses
    fn sort_rows(&self, mut rows: Vec<Value>, order_by: &[OrderBy]) -> Result<Vec<Value>, DatabaseError> {
        rows.sort_by(|a, b| {
            for order in order_by {
                let a_val = self.get_field_value(a, &order.field);
                let b_val = self.get_field_value(b, &order.field);
                
                let cmp = self.compare_json_values(&a_val, &b_val);
                
                let result = match order.direction {
                    SortDirection::Asc => cmp,
                    SortDirection::Desc => cmp.reverse(),
                };
                
                if result != std::cmp::Ordering::Equal {
                    return result;
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(rows)
    }

    /// Compares two JSON values for sorting
    fn compare_json_values(&self, a: &Value, b: &Value) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        
        match (a, b) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
            
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            
            (Value::Number(a), Value::Number(b)) => {
                if let (Some(a_f), Some(b_f)) = (a.as_f64(), b.as_f64()) {
                    a_f.partial_cmp(&b_f).unwrap_or(Ordering::Equal)
                } else {
                    Ordering::Equal
                }
            }
            
            (Value::String(a), Value::String(b)) => a.cmp(b),
            
            // Arrays and objects: compare by JSON string representation
            (a, b) => {
                let a_str = serde_json::to_string(a).unwrap_or_default();
                let b_str = serde_json::to_string(b).unwrap_or_default();
                a_str.cmp(&b_str)
            }
        }
    }

    /// Projects fields from rows based on SELECT field list
    fn project_fields(&self, rows: Vec<Value>, fields: &[Field]) -> Result<Vec<Value>, DatabaseError> {
        if fields.len() == 1 && matches!(fields[0], Field::All) {
            // SELECT * - return all fields
            return Ok(rows);
        }

        let mut projected = Vec::new();
        
        for row in rows {
            let mut projected_row = serde_json::Map::new();
            
            for field in fields {
                match field {
                    Field::All => {
                        // This shouldn't happen as we handle it above
                        continue;
                    }
                    Field::Named(name) => {
                        let value = self.get_field_value(&row, name);
                        projected_row.insert(name.clone(), value);
                    }
                    Field::Aliased { field, alias } => {
                        let value = self.get_field_value(&row, field);
                        projected_row.insert(alias.clone(), value);
                    }
                }
            }
            
            projected.push(Value::Object(projected_row));
        }

        Ok(projected)
    }

    /// Applies an assignment to a document (for UPDATE queries)
    fn apply_assignment(&self, document: &mut Value, assignment: &Assignment) -> Result<(), DatabaseError> {
        // For simplicity, only support top-level field assignments
        if let Value::Object(obj) = document {
            obj.insert(assignment.field.clone(), assignment.value.clone());
            Ok(())
        } else {
            Err(DatabaseError::InvalidQuery {
                message: "Cannot apply assignment to non-object document".to_string(),
            })
        }
    }

    /// Gets collection names for debugging/introspection
    pub fn get_collection_names(&self) -> Vec<String> {
        self.collections.keys().cloned().collect()
    }

    /// Gets collection statistics
    pub async fn get_collection_stats(&self, collection_name: &str) -> Result<(usize, String), DatabaseError> {
        if let Some(collection) = self.collections.get(collection_name) {
            let stats = collection.stats().await?;
            Ok((stats.total_documents, format!("{} documents", stats.total_documents)))
        } else {
            Err(DatabaseError::InvalidQuery {
                message: format!("Collection '{}' not found", collection_name),
            })
        }
    }
}
