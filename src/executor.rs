use crate::errors::DbError;
use crate::parser::{parse_sql, SqlStatement, SqlValue, ColumnDefinition, WhereClause, Condition, Assignment};
use crate::row::Row;
use crate::table::Table;
use crate::types::{DataType, Column, TypedValue};
use std::collections::HashMap;

/// QueryExecutor - AST'yi yorumlayıp veri işlemlerini çalıştıran yapı
#[derive(Debug, Clone)]
pub struct QueryExecutor;

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
        QueryExecutor
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
    fn execute_select(&self, table_name: String, columns: Vec<String>, where_clause: Option<WhereClause>, tables: &HashMap<String, Table>) -> Result<QueryResult, DbError> {
        let table = tables.get(&table_name)
            .ok_or_else(|| DbError::table_not_found(&table_name))?;
        
        let table_columns = table.get_columns();
        let all_rows = table.get_all_rows();
        
        // Seçilecek kolonları belirle
        let selected_columns: Vec<&Column> = if columns.is_empty() {
            // SELECT * durumu
            table_columns.iter().collect()
        } else {
            // Belirli kolonlar
            let mut selected = Vec::new();
            for col_name in &columns {
                let column = table_columns.iter()
                    .find(|c| c.name == *col_name)
                    .ok_or_else(|| DbError::column_not_found(col_name))?;
                selected.push(column);
            }
            selected
        };
        
        // Kolon adlarını al
        let column_names: Vec<String> = selected_columns.iter()
            .map(|col| col.name.clone())
            .collect();
        
        // Satırları hazırla (WHERE koşulu ile filtreleme)
        let mut result_rows = Vec::new();
        for row in all_rows {
            // WHERE koşulunu kontrol et
            if let Some(ref where_clause) = where_clause {
                if !Self::evaluate_condition(&where_clause.condition, row)? {
                    continue;
                }
            }
            
            let mut row_values = Vec::new();
            for column in &selected_columns {
                let value = row.get(&column.name)
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "NULL".to_string());
                row_values.push(value);
            }
            result_rows.push(row_values);
        }
        
        Ok(QueryResult::Select {
            columns: column_names,
            rows: result_rows,
            execution_time_ms: 0, // Will be set by execute_sql
        })
    }
    
    /// UPDATE işlemini çalıştırır
    fn execute_update(&self, table_name: String, assignments: Vec<Assignment>, where_clause: Option<WhereClause>, tables: &mut HashMap<String, Table>) -> Result<QueryResult, DbError> {
        let table = tables.get_mut(&table_name)
            .ok_or_else(|| DbError::table_not_found(&table_name))?;
        
        let columns = table.get_columns().clone();
        let rows = table.get_all_rows_mut();
        
        let mut updated_count = 0;
        
        for row in rows.iter_mut() {
            // WHERE koşulunu kontrol et
            if let Some(ref where_clause) = where_clause {
                if !Self::evaluate_condition(&where_clause.condition, row)? {
                    continue;
                }
            }
            
            // Atamaları uygula
            for assignment in &assignments {
                // Kolonun varlığını kontrol et
                let column = columns.iter()
                    .find(|c| c.name == assignment.column)
                    .ok_or_else(|| DbError::column_not_found(&assignment.column))?;
                
                // Tip kontrolü ve dönüştürme
                let typed_value = Self::convert_sql_value_to_typed_value(&assignment.value, &column.data_type)?;
                
                // Değeri güncelle
                row.insert(assignment.column.clone(), typed_value);
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
            // WHERE koşuluna göre filtrele
            let rows_to_keep: Vec<Row> = table.get_all_rows()
                .iter()
                .filter(|row| {
                    match Self::evaluate_condition(&where_clause.condition, row) {
                        Ok(matches) => !matches, // Koşula uymayan satırları tut
                        Err(_) => true, // Hata durumunda satırı tut
                    }
                })
                .cloned()
                .collect();
            
            table.set_rows(rows_to_keep);
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
            QueryResult::Select { columns: _, rows } => {
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
            QueryResult::Select { columns: _, rows } => {
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
            QueryResult::Select { columns: _, rows } => {
                assert_eq!(rows.len(), 4);
                for row in rows {
                    assert_eq!(row[0], "true");
                }
            }
            _ => panic!("Expected select result"),
        }
    }
    
    #[test]
    fn test_delete_with_where() {
        let (mut tables, _) = create_test_table();
        let executor = QueryExecutor::new();
        
        // DELETE FROM users WHERE id = 1
        let delete_sql = "DELETE FROM users WHERE id = 1";
        let result = executor.execute_sql(delete_sql, &mut tables).unwrap();
        
        match result {
            QueryResult::Success { message, .. } => {
                assert!(message.contains("1 rows deleted"));
            }
            _ => panic!("Expected success result"),
        }
        
        // Verify the deletion
        let select_sql = "SELECT * FROM users";
        let result = executor.execute_sql(select_sql, &mut tables).unwrap();
        
        match result {
            QueryResult::Select { columns: _, rows } => {
                assert_eq!(rows.len(), 3); // Should be 3 rows left
                // Make sure id=1 is not in the results
                for row in rows {
                    assert_ne!(row[0], "1");
                }
            }
            _ => panic!("Expected select result"),
        }
    }
    
    #[test]
    fn test_delete_multiple_rows() {
        let (mut tables, _) = create_test_table();
        let executor = QueryExecutor::new();
        
        // DELETE FROM users WHERE active = false
        let delete_sql = "DELETE FROM users WHERE active = false";
        let result = executor.execute_sql(delete_sql, &mut tables).unwrap();
        
        match result {
            QueryResult::Success { message, .. } => {
                assert!(message.contains("2 rows deleted"));
            }
            _ => panic!("Expected success result"),
        }
        
        // Verify the deletion
        let select_sql = "SELECT * FROM users";
        let result = executor.execute_sql(select_sql, &mut tables).unwrap();
        
        match result {
            QueryResult::Select { columns: _, rows } => {
                assert_eq!(rows.len(), 2); // Should be 2 rows left
                // Make sure all remaining rows have active = true
                for row in rows {
                    assert_eq!(row[3], "true");
                }
            }
            _ => panic!("Expected select result"),
        }
    }
    
    #[test]
    fn test_delete_all_rows() {
        let (mut tables, _) = create_test_table();
        let executor = QueryExecutor::new();
        
        // DELETE FROM users (no WHERE clause)
        let delete_sql = "DELETE FROM users";
        let result = executor.execute_sql(delete_sql, &mut tables).unwrap();
        
        match result {
            QueryResult::Success { message, .. } => {
                assert!(message.contains("4 rows deleted"));
            }
            _ => panic!("Expected success result"),
        }
        
        // Verify all rows are deleted
        let select_sql = "SELECT * FROM users";
        let result = executor.execute_sql(select_sql, &mut tables).unwrap();
        
        match result {
            QueryResult::Select { columns: _, rows } => {
                assert_eq!(rows.len(), 0);
            }
            _ => panic!("Expected select result"),
        }
    }
    
    #[test]
    fn test_update_nonexistent_table() {
        let mut tables = HashMap::new();
        let executor = QueryExecutor::new();
        
        let update_sql = "UPDATE nonexistent SET name = 'test'";
        let result = executor.execute_sql(update_sql, &mut tables);
        
        assert!(result.is_err());
        match result.unwrap_err() {
            DbError::TableNotFound(_) => {}, // Expected
            _ => panic!("Expected TableNotFound error"),
        }
    }
    
    #[test]
    fn test_delete_nonexistent_table() {
        let mut tables = HashMap::new();
        let executor = QueryExecutor::new();
        
        let delete_sql = "DELETE FROM nonexistent";
        let result = executor.execute_sql(delete_sql, &mut tables);
        
        assert!(result.is_err());
        match result.unwrap_err() {
            DbError::TableNotFound(_) => {}, // Expected
            _ => panic!("Expected TableNotFound error"),
        }
    }

    #[test]
    fn test_complex_where_conditions() {
        let (mut tables, _) = create_test_table();
        let executor = QueryExecutor::new();
        
        // Test AND condition
        let sql = "SELECT * FROM users WHERE age > 25 AND active = true";
        let result = executor.execute_sql(sql, &mut tables).unwrap();
        
        match result {
            QueryResult::Select { columns: _, rows } => {
                // Should return John (25 is not > 25) and Bob (35 > 25 and active = true)
                assert_eq!(rows.len(), 1); // Only Bob meets criteria
                assert_eq!(rows[0][1], "Bob"); // name column
            }
            _ => panic!("Expected select result"),
        }
        
        // Test OR condition
        let sql = "SELECT * FROM users WHERE age < 20 OR age > 50";
        let result = executor.execute_sql(sql, &mut tables).unwrap();
        
        match result {
            QueryResult::Select { columns: _, rows } => {
                // No users meet this criteria (ages are 25, 30, 35, 28)
                assert_eq!(rows.len(), 0);
            }
            _ => panic!("Expected select result"),
        }
        
        // Test parentheses
        let sql = "SELECT * FROM users WHERE (age > 25 AND active = true) OR name = 'Jane'";
        let result = executor.execute_sql(sql, &mut tables).unwrap();
        
        match result {
            QueryResult::Select { columns: _, rows } => {
                // Should return Bob (age > 25 AND active = true) and Jane (name = 'Jane')
                assert_eq!(rows.len(), 2);
                // Order might vary, so check both names are present
                let names: Vec<&str> = rows.iter().map(|row| row[1].as_str()).collect();
                assert!(names.contains(&"Bob"));
                assert!(names.contains(&"Jane"));
            }
            _ => panic!("Expected select result"),
        }
    }
    
    #[test]
    fn test_complex_update_with_where() {
        let (mut tables, _) = create_test_table();
        let executor = QueryExecutor::new();
        
        // Complex UPDATE with AND condition
        let sql = "UPDATE users SET active = false WHERE age >= 30 AND name != 'Bob'";
        let result = executor.execute_sql(sql, &mut tables).unwrap();
        
        match result {
            QueryResult::Success { message, .. } => {
                assert!(message.contains("1 rows updated")); // Only Jane meets criteria
            }
            _ => panic!("Expected success result"),
        }
        
        // Verify the update
        let sql = "SELECT name, active FROM users WHERE name = 'Jane'";
        let result = executor.execute_sql(sql, &mut tables).unwrap();
        
        match result {
            QueryResult::Select { columns: _, rows } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][0], "Jane");
                assert_eq!(rows[0][1], "false");
            }
            _ => panic!("Expected select result"),
        }
    }
    
    #[test]
    fn test_complex_delete_with_where() {
        let (mut tables, _) = create_test_table();
        let executor = QueryExecutor::new();
        
        // Complex DELETE with OR condition
        let sql = "DELETE FROM users WHERE age <= 25 OR active = false";
        let result = executor.execute_sql(sql, &mut tables).unwrap();
        
        match result {
            QueryResult::Success { message, .. } => {
                assert!(message.contains("3 rows deleted")); // John (25), Jane (false), Alice (false)
            }
            _ => panic!("Expected success result"),
        }
        
        // Verify only Bob remains
        let sql = "SELECT * FROM users";
        let result = executor.execute_sql(sql, &mut tables).unwrap();
        
        match result {
            QueryResult::Select { columns: _, rows } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0][1], "Bob");
            }
            _ => panic!("Expected select result"),
        }
    }
} 