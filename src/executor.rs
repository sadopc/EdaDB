use crate::errors::DbError;
use crate::parser::{parse_sql, SqlStatement, SqlValue, ColumnDefinition, WhereClause, Condition, Assignment};
use crate::row::Row;
use crate::table::Table;
use crate::types::{DataType, Column, TypedValue};
use crate::query_planner::QueryPlanner;
use crate::parallel_executor::ParallelQueryExecutor;
use std::collections::HashMap;

/// QueryExecutor - AST'yi yorumlayıp veri işlemlerini çalıştıran yapı
#[derive(Debug, Clone)]
pub struct QueryExecutor {
    parallel_executor: ParallelQueryExecutor,
    query_planner: QueryPlanner,
}

/// Sorgu sonuçlarını temsil eden enum
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum QueryResult {
    /// Başarılı işlem (INSERT, UPDATE, DELETE, CREATE, DROP)
    Success {
        message: String,
        execution_time_ms: u64,
    },
    /// SELECT sorgusu sonucu
    Select { 
        columns: Vec<String>, 
        rows: Vec<Vec<String>>,
        execution_time_ms: u64,
    },
}

impl QueryExecutor {
    /// Yeni QueryExecutor yaratır
    pub fn new() -> Self {
        QueryExecutor {
            parallel_executor: ParallelQueryExecutor::new(),
            query_planner: QueryPlanner::new(),
        }
    }

    /// Parallel execution ayarlarını yapılandırır
    pub fn with_parallel_settings(min_rows: usize, chunk_size: usize, max_threads: Option<usize>) -> Self {
        QueryExecutor {
            parallel_executor: ParallelQueryExecutor::with_settings(min_rows, chunk_size, max_threads),
            query_planner: QueryPlanner::new(),
        }
    }

    /// Parallel execution'ı devre dışı bırakır
    pub fn disable_parallel(&mut self) {
        self.parallel_executor.disable();
    }

    /// Parallel execution'ı etkinleştirir
    pub fn enable_parallel(&mut self) {
        self.parallel_executor.enable();
    }
    
    /// SQL string'ini parse edip çalıştırır
    pub fn execute_sql(&self, sql: &str, tables: &mut HashMap<String, Table>) -> Result<QueryResult, DbError> {
        let start_time = std::time::Instant::now();
        
        let statement = parse_sql(sql)
            .map_err(|e| DbError::parse_error(&e))?;
        
        let mut result = self.execute_statement(statement, tables)?;
        
        let execution_time_ms = start_time.elapsed().as_micros() as u64;
        
        // Execution time'ı result'a ekle
        match &mut result {
            QueryResult::Success { execution_time_ms: ref mut time, .. } => {
                *time = execution_time_ms;
            }
            QueryResult::Select { execution_time_ms: ref mut time, .. } => {
                *time = execution_time_ms;
            }
        }
        
        Ok(result)
    }
    
    /// Parse edilmiş AST'yi çalıştırır
    pub fn execute_statement(&self, statement: SqlStatement, tables: &mut HashMap<String, Table>) -> Result<QueryResult, DbError> {
        match statement {
            SqlStatement::CreateTable { table_name, columns } => {
                self.execute_create_table(table_name, columns, tables)
            }
            SqlStatement::CreateIndex { table_name, column_name, index_type } => {
                self.execute_create_index(table_name, column_name, index_type, tables)
            }
            SqlStatement::Insert { table_name, values } => {
                self.execute_insert(table_name, values, tables)
            }
            SqlStatement::Select { table_name, columns, where_clause } => {
                self.execute_select(table_name, columns, where_clause, tables)
            }
            SqlStatement::Update { table_name, assignments, where_clause } => {
                self.execute_update(table_name, assignments, where_clause, tables)
            }
            SqlStatement::Delete { table_name, where_clause } => {
                self.execute_delete(table_name, where_clause, tables)
            }
            SqlStatement::DropTable { table_name } => {
                self.execute_drop_table(table_name, tables)
            }
            SqlStatement::ShowStats { table_name } => {
                self.execute_show_stats(table_name, tables)
            }
            SqlStatement::Explain { statement } => {
                self.execute_explain(*statement, tables)
            }
            SqlStatement::SetStorageFormat { table_name, format } => {
                self.execute_set_storage_format(table_name, format, tables)
            }
            SqlStatement::ShowStorageInfo { table_name } => {
                self.execute_show_storage_info(table_name, tables)
            }
            SqlStatement::CompressColumns { table_name } => {
                self.execute_compress_columns(table_name, tables)
            }
            SqlStatement::AnalyticalQuery { table_name, operation, column_name } => {
                self.execute_analytical_query(table_name, operation, column_name, tables)
            }
            SqlStatement::BeginTransaction { isolation_level: _ } => {
                // Transaction handling is done in database.rs, not in executor
                Err(DbError::ExecutionError("BEGIN TRANSACTION should be handled by database layer".to_string()))
            }
            SqlStatement::CommitTransaction => {
                Err(DbError::ExecutionError("COMMIT should be handled by database layer".to_string()))
            }
            SqlStatement::RollbackTransaction => {
                Err(DbError::ExecutionError("ROLLBACK should be handled by database layer".to_string()))
            }
            SqlStatement::ShowTransactions => {
                Err(DbError::ExecutionError("SHOW TRANSACTIONS should be handled by database layer".to_string()))
            }
        }
    }
    
    /// CREATE TABLE işlemini çalıştırır
    fn execute_create_table(&self, table_name: String, columns: Vec<ColumnDefinition>, tables: &mut HashMap<String, Table>) -> Result<QueryResult, DbError> {
        // Tablo zaten var mı kontrol et
        if tables.contains_key(&table_name) {
            return Err(DbError::table_already_exists(&table_name));
        }
        
        // Kolonları oluştur
        let mut table_columns = Vec::new();
        for col_def in columns {
            table_columns.push(Column::new(col_def.name, col_def.data_type));
        }
        
        if table_columns.is_empty() {
            return Err(DbError::execution_error("Table must have at least one column"));
        }
        
        // Tabloyu oluştur
        let table = Table::new(table_name.clone(), table_columns);
        tables.insert(table_name.clone(), table);
        
        Ok(QueryResult::Success {
            message: format!("Table '{}' created successfully", table_name),
            execution_time_ms: 0, // Will be set by execute_sql
        })
    }
    
    /// CREATE INDEX işlemini çalıştırır
    fn execute_create_index(&self, table_name: String, column_name: String, index_type: crate::table::IndexType, tables: &mut HashMap<String, Table>) -> Result<QueryResult, DbError> {
        let table = tables.get_mut(&table_name)
            .ok_or_else(|| DbError::table_not_found(&table_name))?;
        
        // Index oluştur
        match table.create_index_with_type(column_name.clone(), index_type.clone()) {
            Ok(()) => {
                let index_type_str = match index_type {
                    crate::table::IndexType::Hash => "HASH",
                    crate::table::IndexType::BTree => "BTREE",
                };
                Ok(QueryResult::Success {
                    message: format!("{} index created on column '{}' for table '{}'", index_type_str, column_name, table_name),
                    execution_time_ms: 0, // Will be set by execute_sql
                })
            }
            Err(e) => Err(DbError::execution_error(&e)),
        }
    }
    
    /// INSERT INTO işlemini çalıştırır
    fn execute_insert(&self, table_name: String, values: Vec<SqlValue>, tables: &mut HashMap<String, Table>) -> Result<QueryResult, DbError> {
        let table = tables.get_mut(&table_name)
            .ok_or_else(|| DbError::table_not_found(&table_name))?;
        
        let columns = table.get_columns().clone();
        
        // Değer sayısı kontrol et
        if values.len() != columns.len() {
            return Err(DbError::invalid_column_count(columns.len(), values.len()));
        }
        
        // Row oluştur ve tip kontrolü yap
        let mut row = Row::new();
        for (i, value) in values.iter().enumerate() {
            let column = &columns[i];
            let typed_value = Self::convert_sql_value_to_typed_value(value, &column.data_type)?;
            row.insert(column.name.clone(), typed_value);
        }
        
        table.insert_row(row);
        
        Ok(QueryResult::Success {
            message: format!("1 row inserted into '{}'", table_name),
            execution_time_ms: 0, // Will be set by execute_sql
        })
    }
    
    /// SELECT işlemini çalıştırır
    fn execute_select(&self, table_name: String, columns: Vec<String>, where_clause: Option<WhereClause>, tables: &mut HashMap<String, Table>) -> Result<QueryResult, DbError> {
        let start_time = std::time::Instant::now();
        let table = tables.get_mut(&table_name)
            .ok_or_else(|| DbError::table_not_found(&table_name))?;
        
        // Clone the data we need to avoid borrowing conflicts
        let table_columns = table.get_columns().clone();
        let _all_rows = table.get_all_rows().clone();
        
        // Seçilecek kolonları belirle
        let selected_columns: Vec<Column> = if columns.is_empty() {
            // SELECT * durumu
            table_columns.clone()
        } else {
            // Belirli kolonlar
            let mut selected = Vec::new();
            for col_name in &columns {
                let column = table_columns.iter()
                    .find(|c| c.name == *col_name)
                    .ok_or_else(|| DbError::column_not_found(col_name))?;
                selected.push(column.clone());
            }
            selected
        };
        
        // Kolon adlarını al
        let column_names: Vec<String> = selected_columns.iter()
            .map(|col| col.name.clone())
            .collect();
        
        // Use parallel executor for query execution
        let query_condition = where_clause.as_ref().map(|wc| &wc.condition);
        let result_rows = self.parallel_executor.execute_select_parallel(
            table,
            query_condition,
            &column_names,
        );
        
        // Convert result rows to string format
        let string_rows: Vec<Vec<String>> = result_rows.iter()
            .map(|row| {
                column_names.iter()
                    .map(|col_name| row.get_as_string(col_name))
                    .collect()
            })
            .collect();
        
        // Record query statistics
        let query_pattern = Self::create_query_pattern(&column_names, &where_clause);
        let execution_time = start_time.elapsed();
        table.stats.record_select(query_pattern, execution_time, string_rows.len());
        
        Ok(QueryResult::Select {
            columns: column_names,
            rows: string_rows,
            execution_time_ms: execution_time.as_micros() as u64,
        })
    }

    /// Parallel COUNT aggregate function
    pub fn execute_count(&self, table_name: String, where_clause: Option<WhereClause>, tables: &mut HashMap<String, Table>) -> Result<QueryResult, DbError> {
        let start_time = std::time::Instant::now();
        let table = tables.get_mut(&table_name)
            .ok_or_else(|| DbError::table_not_found(&table_name))?;
        
        let query_condition = where_clause.as_ref().map(|wc| &wc.condition);
        let count = self.parallel_executor.parallel_count(table, query_condition);
        
        let execution_time = start_time.elapsed();
        let query_pattern = format!("SELECT COUNT(*) FROM {}", table_name);
        table.stats.record_select(query_pattern, execution_time, 1);
        
        Ok(QueryResult::Select {
            columns: vec!["count".to_string()],
            rows: vec![vec![count.to_string()]],
            execution_time_ms: execution_time.as_micros() as u64,
        })
    }

    /// Parallel SUM aggregate function
    pub fn execute_sum(&self, table_name: String, column: String, where_clause: Option<WhereClause>, tables: &mut HashMap<String, Table>) -> Result<QueryResult, DbError> {
        let start_time = std::time::Instant::now();
        let table = tables.get_mut(&table_name)
            .ok_or_else(|| DbError::table_not_found(&table_name))?;
        
        let query_condition = where_clause.as_ref().map(|wc| &wc.condition);
        let sum = self.parallel_executor.parallel_sum(table, &column, query_condition);
        
        let execution_time = start_time.elapsed();
        let query_pattern = format!("SELECT SUM({}) FROM {}", column, table_name);
        table.stats.record_select(query_pattern, execution_time, 1);
        
        Ok(QueryResult::Select {
            columns: vec![format!("sum({})", column)],
            rows: vec![vec![sum.to_string()]],
            execution_time_ms: execution_time.as_micros() as u64,
        })
    }

    /// Parallel AVG aggregate function
    pub fn execute_avg(&self, table_name: String, column: String, where_clause: Option<WhereClause>, tables: &mut HashMap<String, Table>) -> Result<QueryResult, DbError> {
        let start_time = std::time::Instant::now();
        let table = tables.get_mut(&table_name)
            .ok_or_else(|| DbError::table_not_found(&table_name))?;
        
        let query_condition = where_clause.as_ref().map(|wc| &wc.condition);
        let avg = self.parallel_executor.parallel_avg(table, &column, query_condition);
        
        let execution_time = start_time.elapsed();
        let query_pattern = format!("SELECT AVG({}) FROM {}", column, table_name);
        table.stats.record_select(query_pattern, execution_time, 1);
        
        Ok(QueryResult::Select {
            columns: vec![format!("avg({})", column)],
            rows: vec![vec![avg.to_string()]],
            execution_time_ms: execution_time.as_micros() as u64,
        })
    }

    /// Parallel MIN aggregate function
    pub fn execute_min(&self, table_name: String, column: String, where_clause: Option<WhereClause>, tables: &mut HashMap<String, Table>) -> Result<QueryResult, DbError> {
        let start_time = std::time::Instant::now();
        let table = tables.get_mut(&table_name)
            .ok_or_else(|| DbError::table_not_found(&table_name))?;
        
        let query_condition = where_clause.as_ref().map(|wc| &wc.condition);
        let min = self.parallel_executor.parallel_min(table, &column, query_condition);
        
        let min_str = match min {
            Some(value) => value.to_string(),
            None => "NULL".to_string(),
        };
        
        let execution_time = start_time.elapsed();
        let query_pattern = format!("SELECT MIN({}) FROM {}", column, table_name);
        table.stats.record_select(query_pattern, execution_time, 1);
        
        Ok(QueryResult::Select {
            columns: vec![format!("min({})", column)],
            rows: vec![vec![min_str]],
            execution_time_ms: execution_time.as_micros() as u64,
        })
    }

    /// Parallel MAX aggregate function
    pub fn execute_max(&self, table_name: String, column: String, where_clause: Option<WhereClause>, tables: &mut HashMap<String, Table>) -> Result<QueryResult, DbError> {
        let start_time = std::time::Instant::now();
        let table = tables.get_mut(&table_name)
            .ok_or_else(|| DbError::table_not_found(&table_name))?;
        
        let query_condition = where_clause.as_ref().map(|wc| &wc.condition);
        let max = self.parallel_executor.parallel_max(table, &column, query_condition);
        
        let max_str = match max {
            Some(value) => value.to_string(),
            None => "NULL".to_string(),
        };
        
        let execution_time = start_time.elapsed();
        let query_pattern = format!("SELECT MAX({}) FROM {}", column, table_name);
        table.stats.record_select(query_pattern, execution_time, 1);
        
        Ok(QueryResult::Select {
            columns: vec![format!("max({})", column)],
            rows: vec![vec![max_str]],
            execution_time_ms: execution_time.as_micros() as u64,
        })
    }

    /// Parallel JOIN operation
    pub fn execute_join(&self, left_table: String, right_table: String, left_column: String, right_column: String, join_type: crate::parallel_executor::JoinType, tables: &mut HashMap<String, Table>) -> Result<QueryResult, DbError> {
        let start_time = std::time::Instant::now();
        
        let left_table_ref = tables.get(&left_table)
            .ok_or_else(|| DbError::table_not_found(&left_table))?;
        let right_table_ref = tables.get(&right_table)
            .ok_or_else(|| DbError::table_not_found(&right_table))?;
        
        let result_rows = self.parallel_executor.parallel_join(
            left_table_ref,
            right_table_ref,
            &left_column,
            &right_column,
            join_type,
        );
        
        // Get column names from both tables
        let mut all_columns = Vec::new();
        for col in left_table_ref.get_columns() {
            all_columns.push(col.name.clone());
        }
        for col in right_table_ref.get_columns() {
            all_columns.push(format!("right_{}", col.name));
        }
        
        // Convert result rows to string format
        let string_rows: Vec<Vec<String>> = result_rows.iter()
            .map(|row| {
                all_columns.iter()
                    .map(|col_name| row.get_as_string(col_name))
                    .collect()
            })
            .collect();
        
        let execution_time = start_time.elapsed();
        
        Ok(QueryResult::Select {
            columns: all_columns,
            rows: string_rows,
            execution_time_ms: execution_time.as_micros() as u64,
        })
    }
    
    /// Legacy method for backward compatibility - will be removed in future versions
    fn execute_select_legacy(&self, table_name: String, columns: Vec<String>, where_clause: Option<WhereClause>, tables: &mut HashMap<String, Table>) -> Result<QueryResult, DbError> {
        let start_time = std::time::Instant::now();
        let table = tables.get_mut(&table_name)
            .ok_or_else(|| DbError::table_not_found(&table_name))?;
        
        // Clone the data we need to avoid borrowing conflicts
        let table_columns = table.get_columns().clone();
        let all_rows = table.get_all_rows().clone();
        
        // Seçilecek kolonları belirle
        let selected_columns: Vec<Column> = if columns.is_empty() {
            // SELECT * durumu
            table_columns.clone()
        } else {
            // Belirli kolonlar
            let mut selected = Vec::new();
            for col_name in &columns {
                let column = table_columns.iter()
                    .find(|c| c.name == *col_name)
                    .ok_or_else(|| DbError::column_not_found(col_name))?;
                selected.push(column.clone());
            }
            selected
        };
        
        // Kolon adlarını al
        let column_names: Vec<String> = selected_columns.iter()
            .map(|col| col.name.clone())
            .collect();
        
        // WHERE koşuluna göre satırları filtrele
        let filtered_row_indices = if let Some(ref where_clause) = where_clause {
            Self::get_filtered_row_indices(table, &where_clause.condition, &all_rows)?
        } else {
            // WHERE yoksa tüm satırları al
            (0..all_rows.len()).collect()
        };
        
        // Sonuç satırlarını hazırla
        let mut result_rows = Vec::new();
        for row_index in filtered_row_indices {
            let row = &all_rows[row_index];
            let mut row_values = Vec::new();
            for column in &selected_columns {
                let value = row.get(&column.name)
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "NULL".to_string());
                row_values.push(value);
            }
            result_rows.push(row_values);
        }
        
        // Query statistics tracking
        let execution_time = start_time.elapsed();
        let query_pattern = Self::create_query_pattern(&columns, &where_clause);
        table.stats.record_select(query_pattern, execution_time, result_rows.len());
        
        Ok(QueryResult::Select {
            columns: column_names,
            rows: result_rows,
            execution_time_ms: 0, // Will be set by execute_sql
        })
    }
    
    /// WHERE koşuluna göre filtrelenmiş row index'lerini döndürür
    /// Hash index kullanarak equality queries'i optimize eder
    /// B-Tree index kullanarak range queries'i optimize eder
    fn get_filtered_row_indices(table: &mut Table, condition: &Condition, all_rows: &[Row]) -> Result<Vec<usize>, DbError> {
        match condition {
            Condition::Equal(column, value) => {
                // Hash index var mı kontrol et
                let typed_value = Self::sql_value_to_typed_value(value);
                if let Some(row_indices) = table.get_indexed_rows(column, &typed_value) {
                    // Index kullan - O(1) lookup
                    Ok(row_indices)
                } else {
                    // Index yok, linear search - O(n)
                    Self::linear_search_for_condition(condition, all_rows)
                }
            }
            Condition::Greater(column, value) => {
                // B-Tree index var mı kontrol et
                let typed_value = Self::sql_value_to_typed_value(value);
                if let Some(row_indices) = table.get_indexed_comparison_rows(column, &typed_value, ">") {
                    // B-Tree index kullan - O(log n) lookup
                    Ok(row_indices)
                } else {
                    // Index yok, linear search - O(n)
                    Self::linear_search_for_condition(condition, all_rows)
                }
            }
            Condition::GreaterEqual(column, value) => {
                // B-Tree index var mı kontrol et
                let typed_value = Self::sql_value_to_typed_value(value);
                if let Some(row_indices) = table.get_indexed_comparison_rows(column, &typed_value, ">=") {
                    // B-Tree index kullan - O(log n) lookup
                    Ok(row_indices)
                } else {
                    // Index yok, linear search - O(n)
                    Self::linear_search_for_condition(condition, all_rows)
                }
            }
            Condition::Less(column, value) => {
                // B-Tree index var mı kontrol et
                let typed_value = Self::sql_value_to_typed_value(value);
                if let Some(row_indices) = table.get_indexed_comparison_rows(column, &typed_value, "<") {
                    // B-Tree index kullan - O(log n) lookup
                    Ok(row_indices)
                } else {
                    // Index yok, linear search - O(n)
                    Self::linear_search_for_condition(condition, all_rows)
                }
            }
            Condition::LessEqual(column, value) => {
                // B-Tree index var mı kontrol et
                let typed_value = Self::sql_value_to_typed_value(value);
                if let Some(row_indices) = table.get_indexed_comparison_rows(column, &typed_value, "<=") {
                    // B-Tree index kullan - O(log n) lookup
                    Ok(row_indices)
                } else {
                    // Index yok, linear search - O(n)
                    Self::linear_search_for_condition(condition, all_rows)
                }
            }
            Condition::NotEqual(_column, _value) => {
                // TODO: Implement NotEqual condition
                Self::linear_search_for_condition(condition, all_rows)
            }
            Condition::And(left, right) => {
                // AND koşulu için her iki koşulun da sağlandığı satırlar
                let left_indices = Self::get_filtered_row_indices(table, left, all_rows)?;
                let right_indices = Self::get_filtered_row_indices(table, right, all_rows)?;
                
                // İki set'in kesişimi
                let mut result = Vec::new();
                for index in left_indices {
                    if right_indices.contains(&index) {
                        result.push(index);
                    }
                }
                Ok(result)
            }
            Condition::Or(left, right) => {
                // OR koşulu için her iki koşuldan birinin sağlandığı satırlar
                let left_indices = Self::get_filtered_row_indices(table, left, all_rows)?;
                let right_indices = Self::get_filtered_row_indices(table, right, all_rows)?;
                
                // İki set'in birleşimi (tekrar eden index'leri kaldır)
                let mut result = left_indices;
                for index in right_indices {
                    if !result.contains(&index) {
                        result.push(index);
                    }
                }
                result.sort();
                Ok(result)
            }
        }
    }
    
    /// Linear search ile condition'ı sağlayan row index'lerini bulur
    fn linear_search_for_condition(condition: &Condition, all_rows: &[Row]) -> Result<Vec<usize>, DbError> {
        let mut result = Vec::new();
        for (row_index, row) in all_rows.iter().enumerate() {
            if Self::evaluate_condition(condition, row)? {
                result.push(row_index);
            }
        }
        Ok(result)
    }
    
    /// SqlValue'yu TypedValue'ya dönüştürür (type inference ile)
    fn sql_value_to_typed_value(sql_value: &SqlValue) -> TypedValue {
        match sql_value {
            SqlValue::Integer(i) => TypedValue::Integer(*i),
            SqlValue::Text(s) => TypedValue::Text(s.clone()),
            SqlValue::Boolean(b) => TypedValue::Boolean(*b),
            SqlValue::Null => TypedValue::Null,
        }
    }
    
    /// Query pattern oluşturur (statistics için)
    fn create_query_pattern(columns: &[String], where_clause: &Option<WhereClause>) -> String {
        let column_part = if columns.is_empty() {
            "SELECT *".to_string()
        } else {
            format!("SELECT {}", columns.join(", "))
        };
        
        let where_part = if let Some(where_clause) = where_clause {
            format!(" WHERE {}", Self::condition_to_pattern(&where_clause.condition))
        } else {
            String::new()
        };
        
        format!("{}{}", column_part, where_part)
    }
    
    /// Condition'ı pattern string'e dönüştürür
    fn condition_to_pattern(condition: &Condition) -> String {
        match condition {
            Condition::Equal(column, _) => format!("{} = ?", column),
            Condition::NotEqual(column, _) => format!("{} != ?", column),
            Condition::Greater(column, _) => format!("{} > ?", column),
            Condition::Less(column, _) => format!("{} < ?", column),
            Condition::GreaterEqual(column, _) => format!("{} >= ?", column),
            Condition::LessEqual(column, _) => format!("{} <= ?", column),
            Condition::And(left, right) => format!("({}) AND ({})", 
                Self::condition_to_pattern(left), 
                Self::condition_to_pattern(right)),
            Condition::Or(left, right) => format!("({}) OR ({})", 
                Self::condition_to_pattern(left), 
                Self::condition_to_pattern(right)),
        }
    }
    
    /// UPDATE işlemini çalıştırır
    fn execute_update(&self, table_name: String, assignments: Vec<Assignment>, where_clause: Option<WhereClause>, tables: &mut HashMap<String, Table>) -> Result<QueryResult, DbError> {
        let table = tables.get_mut(&table_name)
            .ok_or_else(|| DbError::table_not_found(&table_name))?;
        
        let columns = table.get_columns().clone();
        let mut updated_count = 0;
        
        // Önce WHERE koşuluna uyan row index'lerini topla
        let mut rows_to_update = Vec::new();
        for (row_index, row) in table.get_all_rows().iter().enumerate() {
            if let Some(ref where_clause) = where_clause {
                if !Self::evaluate_condition(&where_clause.condition, row)? {
                    continue;
                }
            }
            rows_to_update.push(row_index);
        }
        
        // Her güncellenecek row için
        for row_index in rows_to_update {
            // Her assignment için
            for assignment in &assignments {
                // Kolonun varlığını kontrol et
                let column = columns.iter()
                    .find(|c| c.name == assignment.column)
                    .ok_or_else(|| DbError::column_not_found(&assignment.column))?;
                
                // Tip kontrolü ve dönüştürme
                let typed_value = Self::convert_sql_value_to_typed_value(&assignment.value, &column.data_type)?;
                
                // Index-aware update kullan
                table.update_row(row_index, &assignment.column, typed_value)
                    .map_err(|e| DbError::execution_error(&e))?;
            }
            updated_count += 1;
        }
        
        Ok(QueryResult::Success {
            message: format!("{} rows updated in '{}'", updated_count, table_name),
            execution_time_ms: 0, // Will be set by execute_sql
        })
    }
    
    /// DELETE işlemini çalıştırır
    fn execute_delete(&self, table_name: String, where_clause: Option<WhereClause>, tables: &mut HashMap<String, Table>) -> Result<QueryResult, DbError> {
        let table = tables.get_mut(&table_name)
            .ok_or_else(|| DbError::table_not_found(&table_name))?;
        
        let original_count = table.get_all_rows().len();
        
        if let Some(where_clause) = where_clause {
            // WHERE koşuluna uyan row index'lerini topla (ters sırada, büyükten küçüğe)
            let mut rows_to_delete = Vec::new();
            for (row_index, row) in table.get_all_rows().iter().enumerate() {
                if let Ok(matches) = Self::evaluate_condition(&where_clause.condition, row) {
                    if matches {
                        rows_to_delete.push(row_index);
                    }
                }
            }
            
            // Büyükten küçüğe sıralayarak sil (index'lerin kaymaması için)
            rows_to_delete.sort_by(|a, b| b.cmp(a));
            
            // Her bir row'u index-aware delete ile sil
            for row_index in rows_to_delete {
                table.delete_row(row_index)
                    .map_err(|e| DbError::execution_error(&e))?;
            }
        } else {
            // WHERE yoksa tüm satırları sil
            table.clear_rows();
        }
        
        let deleted_count = original_count - table.get_all_rows().len();
        
        Ok(QueryResult::Success {
            message: format!("{} rows deleted from '{}'", deleted_count, table_name),
            execution_time_ms: 0, // Will be set by execute_sql
        })
    }
    
    /// DROP TABLE işlemini çalıştırır
    fn execute_drop_table(&self, table_name: String, tables: &mut HashMap<String, Table>) -> Result<QueryResult, DbError> {
        if tables.remove(&table_name).is_some() {
            Ok(QueryResult::Success {
                message: format!("Table '{}' dropped successfully", table_name),
                execution_time_ms: 0, // Will be set by execute_sql
            })
        } else {
            Err(DbError::table_not_found(&table_name))
        }
    }
    
    /// EXPLAIN işlemini çalıştırır
    fn execute_explain(&self, statement: SqlStatement, tables: &HashMap<String, Table>) -> Result<QueryResult, DbError> {
        match statement {
            SqlStatement::Select { table_name, columns: _, where_clause } => {
                let table = tables.get(&table_name)
                    .ok_or_else(|| DbError::table_not_found(&table_name))?;
                
                let planner = QueryPlanner::new();
                let execution_plan = planner.plan_select_query(table, &where_clause);
                
                let explain_output = execution_plan.format_explain();
                
                // Convert to select result for display
                let lines: Vec<String> = explain_output.lines().map(|s| s.to_string()).collect();
                let result_rows: Vec<Vec<String>> = lines.iter()
                    .map(|line| vec![line.clone()])
                    .collect();
                
                Ok(QueryResult::Select {
                    columns: vec!["Execution Plan".to_string()],
                    rows: result_rows,
                    execution_time_ms: 0,
                })
            }
            _ => {
                Err(DbError::execution_error("EXPLAIN is currently only supported for SELECT statements"))
            }
        }
    }
    
    /// SHOW STATS işlemini çalıştırır
    fn execute_show_stats(&self, table_name: String, tables: &HashMap<String, Table>) -> Result<QueryResult, DbError> {
        let table = tables.get(&table_name)
            .ok_or_else(|| DbError::table_not_found(&table_name))?;
        
        let stats = &table.stats;
        let mut result_rows = Vec::new();
        
        // Table genel istatistikleri
        result_rows.push(vec![
            "Table".to_string(),
            table_name.clone(),
            "Total Rows".to_string(),
            stats.total_rows.to_string(),
        ]);
        
        result_rows.push(vec![
            "Table".to_string(),
            table_name.clone(),
            "Total Inserts".to_string(),
            stats.total_inserts.to_string(),
        ]);
        
        result_rows.push(vec![
            "Table".to_string(),
            table_name.clone(),
            "Total Updates".to_string(),
            stats.total_updates.to_string(),
        ]);
        
        result_rows.push(vec![
            "Table".to_string(),
            table_name.clone(),
            "Total Deletes".to_string(),
            stats.total_deletes.to_string(),
        ]);
        
        result_rows.push(vec![
            "Table".to_string(),
            table_name.clone(),
            "Total Selects".to_string(),
            stats.total_selects.to_string(),
        ]);
        
        // Column statistics
        for (column_name, column_stats) in &stats.column_stats {
            result_rows.push(vec![
                "Column".to_string(),
                column_name.clone(),
                "Unique Count".to_string(),
                column_stats.unique_count.to_string(),
            ]);
            
            result_rows.push(vec![
                "Column".to_string(),
                column_name.clone(),
                "Null Count".to_string(),
                column_stats.null_count.to_string(),
            ]);
            
            result_rows.push(vec![
                "Column".to_string(),
                column_name.clone(),
                "Null Ratio".to_string(),
                format!("{:.2}%", column_stats.null_ratio() * 100.0),
            ]);
            
            result_rows.push(vec![
                "Column".to_string(),
                column_name.clone(),
                "Cardinality".to_string(),
                format!("{:.4}", column_stats.cardinality()),
            ]);
            
            if let Some(min_val) = &column_stats.min_value {
                result_rows.push(vec![
                    "Column".to_string(),
                    column_name.clone(),
                    "Min Value".to_string(),
                    min_val.to_string(),
                ]);
            }
            
            if let Some(max_val) = &column_stats.max_value {
                result_rows.push(vec![
                    "Column".to_string(),
                    column_name.clone(),
                    "Max Value".to_string(),
                    max_val.to_string(),
                ]);
            }
            
            if let Some((most_freq_val, freq)) = column_stats.most_frequent_value() {
                result_rows.push(vec![
                    "Column".to_string(),
                    column_name.clone(),
                    "Most Frequent Value".to_string(),
                    format!("{} ({}x)", most_freq_val, freq),
                ]);
            }
        }
        
        // Index usage statistics
        for (index_name, index_stats) in &stats.index_usage_stats {
            result_rows.push(vec![
                "Index".to_string(),
                index_name.clone(),
                "Usage Count".to_string(),
                index_stats.usage_count.to_string(),
            ]);
            
            result_rows.push(vec![
                "Index".to_string(),
                index_name.clone(),
                "Avg Lookup Time".to_string(),
                format!("{:.2}ns", index_stats.avg_lookup_time_ns),
            ]);
            
            result_rows.push(vec![
                "Index".to_string(),
                index_name.clone(),
                "Usage Frequency".to_string(),
                format!("{:.2}/hour", index_stats.usage_frequency()),
            ]);
        }
        
        // Query statistics (top 5 most frequent)
        let mut query_stats_vec: Vec<_> = stats.query_stats.iter().collect();
        query_stats_vec.sort_by(|a, b| b.1.execution_count.cmp(&a.1.execution_count));
        
        for (query_pattern, query_stats) in query_stats_vec.iter().take(5) {
            result_rows.push(vec![
                "Query".to_string(),
                query_pattern.to_string(),
                "Execution Count".to_string(),
                query_stats.execution_count.to_string(),
            ]);
            
            result_rows.push(vec![
                "Query".to_string(),
                query_pattern.to_string(),
                "Avg Execution Time".to_string(),
                format!("{:.2}μs", query_stats.avg_execution_time_ns / 1000.0),
            ]);
            
            result_rows.push(vec![
                "Query".to_string(),
                query_pattern.to_string(),
                "Avg Rows Returned".to_string(),
                format!("{:.1}", query_stats.avg_rows_returned),
            ]);
        }
        
        Ok(QueryResult::Select {
            columns: vec!["Type".to_string(), "Name".to_string(), "Metric".to_string(), "Value".to_string()],
            rows: result_rows,
            execution_time_ms: 0, // Will be set by execute_sql
        })
    }
    
    /// SqlValue'yu TypedValue'ya dönüştürür
    fn convert_sql_value_to_typed_value(sql_value: &SqlValue, data_type: &DataType) -> Result<TypedValue, DbError> {
        match (sql_value, data_type) {
            (SqlValue::Integer(i), DataType::INT) => Ok(TypedValue::Integer(*i)),
            (SqlValue::Text(s), DataType::TEXT) => Ok(TypedValue::Text(s.clone())),
            (SqlValue::Boolean(b), DataType::BOOL) => Ok(TypedValue::Boolean(*b)),
            (SqlValue::Null, _) => Ok(TypedValue::Null),
            
            // Tip dönüştürme girişimleri
            (SqlValue::Text(s), DataType::INT) => {
                s.parse::<i64>()
                    .map(TypedValue::Integer)
                    .map_err(|_| DbError::type_mismatch("INT", &format!("TEXT({})", s)))
            }
            (SqlValue::Text(s), DataType::BOOL) => {
                match s.to_lowercase().as_str() {
                    "true" | "1" | "yes" => Ok(TypedValue::Boolean(true)),
                    "false" | "0" | "no" => Ok(TypedValue::Boolean(false)),
                    _ => Err(DbError::type_mismatch("BOOL", &format!("TEXT({})", s))),
                }
            }
            (SqlValue::Integer(i), DataType::TEXT) => Ok(TypedValue::Text(i.to_string())),
            (SqlValue::Boolean(b), DataType::TEXT) => Ok(TypedValue::Text(b.to_string())),
            
            // Diğer durumlar hata
            _ => Err(DbError::type_mismatch(
                &data_type.to_string(),
                &format!("{:?}", sql_value)
            )),
        }
    }
    
    /// WHERE koşulunu değerlendirir
    fn evaluate_condition(condition: &Condition, row: &Row) -> Result<bool, DbError> {
        match condition {
            Condition::Equal(column, value) => {
                let row_value = row.get(column)
                    .ok_or_else(|| DbError::column_not_found(column))?;
                Ok(Self::compare_values(row_value, value) == std::cmp::Ordering::Equal)
            }
            Condition::NotEqual(column, value) => {
                let row_value = row.get(column)
                    .ok_or_else(|| DbError::column_not_found(column))?;
                Ok(Self::compare_values(row_value, value) != std::cmp::Ordering::Equal)
            }
            Condition::Greater(column, value) => {
                let row_value = row.get(column)
                    .ok_or_else(|| DbError::column_not_found(column))?;
                Ok(Self::compare_values(row_value, value) == std::cmp::Ordering::Greater)
            }
            Condition::Less(column, value) => {
                let row_value = row.get(column)
                    .ok_or_else(|| DbError::column_not_found(column))?;
                Ok(Self::compare_values(row_value, value) == std::cmp::Ordering::Less)
            }
            Condition::GreaterEqual(column, value) => {
                let row_value = row.get(column)
                    .ok_or_else(|| DbError::column_not_found(column))?;
                let cmp = Self::compare_values(row_value, value);
                Ok(cmp == std::cmp::Ordering::Greater || cmp == std::cmp::Ordering::Equal)
            }
            Condition::LessEqual(column, value) => {
                let row_value = row.get(column)
                    .ok_or_else(|| DbError::column_not_found(column))?;
                let cmp = Self::compare_values(row_value, value);
                Ok(cmp == std::cmp::Ordering::Less || cmp == std::cmp::Ordering::Equal)
            }
            Condition::And(left, right) => {
                let left_result = Self::evaluate_condition(left, row)?;
                let right_result = Self::evaluate_condition(right, row)?;
                Ok(left_result && right_result)
            }
            Condition::Or(left, right) => {
                let left_result = Self::evaluate_condition(left, row)?;
                let right_result = Self::evaluate_condition(right, row)?;
                Ok(left_result || right_result)
            }
        }
    }
    
    /// TypedValue ile SqlValue'yu karşılaştırır
    fn compare_values(typed_value: &TypedValue, sql_value: &SqlValue) -> std::cmp::Ordering {
        match (typed_value, sql_value) {
            (TypedValue::Integer(a), SqlValue::Integer(b)) => a.cmp(b),
            (TypedValue::Text(a), SqlValue::Text(b)) => a.cmp(b),
            (TypedValue::Boolean(a), SqlValue::Boolean(b)) => a.cmp(b),
            (TypedValue::Null, SqlValue::Null) => std::cmp::Ordering::Equal,
            
            // Tip uyuşmazlığı durumunda string karşılaştırması
            (typed_val, sql_val) => {
                let typed_str = typed_val.to_string();
                let sql_str = match sql_val {
                    SqlValue::Integer(i) => i.to_string(),
                    SqlValue::Text(s) => s.clone(),
                    SqlValue::Boolean(b) => b.to_string(),
                    SqlValue::Null => "NULL".to_string(),
                };
                typed_str.cmp(&sql_str)
            }
        }
    }
    
    /// SET STORAGE FORMAT işlemini çalıştırır
    fn execute_set_storage_format(&self, table_name: String, format: String, tables: &mut HashMap<String, Table>) -> Result<QueryResult, DbError> {
        let table = tables.get_mut(&table_name)
            .ok_or_else(|| DbError::table_not_found(&table_name))?;
        
        use crate::columnar_storage::StorageFormat;
        
        let storage_format = match format.to_uppercase().as_str() {
            "ROW" => StorageFormat::RowBased,
            "COLUMN" => StorageFormat::ColumnBased,
            "HYBRID" => StorageFormat::Hybrid,
            _ => return Err(DbError::execution_error(&format!("Invalid storage format: {}. Use ROW, COLUMN, or HYBRID", format))),
        };
        
        match table.set_storage_format(storage_format) {
            Ok(()) => Ok(QueryResult::Success {
                message: format!("Storage format for table '{}' set to {}", table_name, format.to_uppercase()),
                execution_time_ms: 0,
            }),
            Err(e) => Err(DbError::execution_error(&e)),
        }
    }
    
    /// SHOW STORAGE INFO işlemini çalıştırır
    fn execute_show_storage_info(&self, table_name: String, tables: &HashMap<String, Table>) -> Result<QueryResult, DbError> {
        let table = tables.get(&table_name)
            .ok_or_else(|| DbError::table_not_found(&table_name))?;
        
        let storage_info = table.get_storage_info();
        
        let columns = vec!["Property".to_string(), "Value".to_string()];
        let mut rows = Vec::new();
        
        for (key, value) in storage_info {
            rows.push(vec![key, value]);
        }
        
        Ok(QueryResult::Select {
            columns,
            rows,
            execution_time_ms: 0,
        })
    }
    
    /// COMPRESS COLUMNS işlemini çalıştırır
    fn execute_compress_columns(&self, table_name: String, tables: &mut HashMap<String, Table>) -> Result<QueryResult, DbError> {
        let table = tables.get_mut(&table_name)
            .ok_or_else(|| DbError::table_not_found(&table_name))?;
        
        match table.compress_columns() {
            Ok(()) => Ok(QueryResult::Success {
                message: format!("Columns compressed for table '{}'", table_name),
                execution_time_ms: 0,
            }),
            Err(e) => Err(DbError::execution_error(&e)),
        }
    }
    
    /// Analytical query işlemini çalıştırır
    fn execute_analytical_query(&self, table_name: String, operation: String, column_name: String, tables: &mut HashMap<String, Table>) -> Result<QueryResult, DbError> {
        let table = tables.get_mut(&table_name)
            .ok_or_else(|| DbError::table_not_found(&table_name))?;
        
        use crate::columnar_storage::AggregateOperation;
        
        let aggregate_op = match operation.to_uppercase().as_str() {
            "COUNT" => AggregateOperation::Count,
            "SUM" => AggregateOperation::Sum,
            "AVG" => AggregateOperation::Avg,
            "MIN" => AggregateOperation::Min,
            "MAX" => AggregateOperation::Max,
            _ => return Err(DbError::execution_error(&format!("Invalid operation: {}. Use COUNT, SUM, AVG, MIN, or MAX", operation))),
        };
        
        // Check if column exists
        if !table.get_columns().iter().any(|c| c.name == column_name) {
            return Err(DbError::execution_error(&format!("Column '{}' not found in table '{}'", column_name, table_name)));
        }
        
        let result = table.execute_analytical_query(&column_name, aggregate_op)
            .unwrap_or(TypedValue::Null);
        
        let columns = vec![format!("{}({})", operation.to_uppercase(), column_name)];
        let rows = vec![vec![result.to_string()]];
        
        Ok(QueryResult::Select {
            columns,
            rows,
            execution_time_ms: 0,
        })
    }
} 

impl Default for QueryExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    
    fn create_test_table() -> (HashMap<String, Table>, String) {
        let mut tables = HashMap::new();
        let table_name = "users".to_string();
        
        let executor = QueryExecutor::new();
        
        // CREATE TABLE users (id INT, name TEXT, age INT, active BOOL)
        let create_sql = "CREATE TABLE users (id INT, name TEXT, age INT, active BOOL)";
        executor.execute_sql(create_sql, &mut tables).unwrap();
        
        // INSERT test data
        let insert_queries = vec![
            "INSERT INTO users VALUES (1, 'John', 25, true)",
            "INSERT INTO users VALUES (2, 'Jane', 30, false)",
            "INSERT INTO users VALUES (3, 'Bob', 35, true)",
            "INSERT INTO users VALUES (4, 'Alice', 28, false)",
        ];
        
        for query in insert_queries {
            executor.execute_sql(query, &mut tables).unwrap();
        }
        
        (tables, table_name)
    }
    
    #[test]
    fn test_update_with_where() {
        let (mut tables, _) = create_test_table();
        let executor = QueryExecutor::new();
        
        // UPDATE users SET name = 'Johnny' WHERE id = 1
        let update_sql = "UPDATE users SET name = 'Johnny' WHERE id = 1";
        let result = executor.execute_sql(update_sql, &mut tables).unwrap();
        
        match result {
            QueryResult::Success { message, .. } => {
                assert!(message.contains("1 rows updated"));
            }
            _ => panic!("Expected success result"),
        }
        
        // SELECT to verify the update
        let select_sql = "SELECT * FROM users WHERE id = 1";
        let result = executor.execute_sql(select_sql, &mut tables).unwrap();
        
        match result {
            QueryResult::Select { columns: _, rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][1], "Johnny"); // name column
            }
            _ => panic!("Expected select result"),
        }
    }
    
    #[test]
    fn test_update_multiple_columns() {
        let (mut tables, _) = create_test_table();
        let executor = QueryExecutor::new();
        
        // UPDATE users SET name = 'Updated', age = 99 WHERE id = 2
        let update_sql = "UPDATE users SET name = 'Updated', age = 99 WHERE id = 2";
        let result = executor.execute_sql(update_sql, &mut tables).unwrap();
        
        match result {
            QueryResult::Success { message, .. } => {
                assert!(message.contains("1 rows updated"));
            }
            _ => panic!("Expected success result"),
        }
        
        // Verify the update
        let select_sql = "SELECT name, age FROM users WHERE id = 2";
        let result = executor.execute_sql(select_sql, &mut tables).unwrap();
        
        match result {
            QueryResult::Select { columns: _, rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], "Updated"); // name
                assert_eq!(rows[0][1], "99"); // age
            }
            _ => panic!("Expected select result"),
        }
    }
    
    #[test]
    fn test_update_all_rows() {
        let (mut tables, _) = create_test_table();
        let executor = QueryExecutor::new();
        
        // UPDATE users SET active = true (no WHERE clause)
        let update_sql = "UPDATE users SET active = true";
        let result = executor.execute_sql(update_sql, &mut tables).unwrap();
        
        match result {
            QueryResult::Success { message, .. } => {
                assert!(message.contains("4 rows updated"));
            }
            _ => panic!("Expected success result"),
        }
        
        // Verify all rows are updated
        let select_sql = "SELECT active FROM users";
        let result = executor.execute_sql(select_sql, &mut tables).unwrap();
        
        match result {
            QueryResult::Select { columns: _, rows, .. } => {
                assert_eq!(rows.len(), 4);
                for row in rows {
                    assert_eq!(row[0], "true");
                }
            }
            _ => panic!("Expected select result"),
        }
    }
} 