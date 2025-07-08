use crate::row::Row;
use crate::types::Column;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        Self {
            name,
            columns,
            rows: Vec::new(),
        }
    }

    pub fn insert_row(&mut self, row: Row) {
        self.rows.push(row);
    }

    pub fn get_all_rows(&self) -> &Vec<Row> {
        &self.rows
    }

    pub fn get_all_rows_mut(&mut self) -> &mut Vec<Row> {
        &mut self.rows
    }

    pub fn get_columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub fn get_column_names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    pub fn find_column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn set_rows(&mut self, rows: Vec<Row>) {
        self.rows = rows;
    }

    pub fn clear(&mut self) {
        self.rows.clear();
    }
    
    pub fn clear_rows(&mut self) {
        self.rows.clear();
    }
} 