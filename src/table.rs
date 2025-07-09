use crate::row::Row;
use crate::types::{Column, TypedValue};
use crate::columnar_storage::{ColumnStore, StorageFormat, AggregateOperation, ColumnAnalytics};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, BTreeMap};
use std::time::{Duration, Instant};

/// Index türleri
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IndexType {
    Hash,
    BTree,
}

/// Hash index yapısı: column_value -> row_indices mapping
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HashIndex {
    pub column_name: String,
    pub index_data: HashMap<TypedValue, Vec<usize>>, // value -> row indices
}

impl HashIndex {
    pub fn new(column_name: String) -> Self {
        Self {
            column_name,
            index_data: HashMap::new(),
        }
    }
    
    /// Index'e yeni bir row ekler
    pub fn insert(&mut self, value: TypedValue, row_index: usize) {
        self.index_data.entry(value).or_insert_with(Vec::new).push(row_index);
    }
    
    /// Index'den bir row siler
    pub fn remove(&mut self, value: &TypedValue, row_index: usize) {
        if let Some(indices) = self.index_data.get_mut(value) {
            indices.retain(|&idx| idx != row_index);
            if indices.is_empty() {
                self.index_data.remove(value);
            }
        }
    }
    
    /// Index'den bir row'un value'sunu günceller
    pub fn update(&mut self, old_value: &TypedValue, new_value: TypedValue, row_index: usize) {
        self.remove(old_value, row_index);
        self.insert(new_value, row_index);
    }
    
    /// Belirli bir value için row index'lerini getirir
    pub fn get_row_indices(&self, value: &TypedValue) -> Option<&Vec<usize>> {
        self.index_data.get(value)
    }
    
    /// Index'i tamamen yeniden oluşturur
    pub fn rebuild_from_rows(&mut self, rows: &[Row]) {
        self.index_data.clear();
        for (row_index, row) in rows.iter().enumerate() {
            if let Some(value) = row.get(&self.column_name) {
                self.insert(value.clone(), row_index);
            }
        }
    }
    
    /// Tüm row index'lerini günceller (silme işlemi sonrası)
    pub fn reindex_after_deletion(&mut self, deleted_index: usize) {
        for indices in self.index_data.values_mut() {
            // Silinen index'den sonraki tüm index'leri bir azalt
            for idx in indices.iter_mut() {
                if *idx > deleted_index {
                    *idx -= 1;
                }
            }
        }
    }
}

/// B-Tree index yapısı: sıralı arama ve range queries için
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BTreeIndex {
    pub column_name: String,
    pub index_data: BTreeMap<TypedValue, Vec<usize>>, // value -> row indices (sorted)
}

impl BTreeIndex {
    pub fn new(column_name: String) -> Self {
        Self {
            column_name,
            index_data: BTreeMap::new(),
        }
    }
    
    /// Index'e yeni bir row ekler
    pub fn insert(&mut self, value: TypedValue, row_index: usize) {
        self.index_data.entry(value).or_insert_with(Vec::new).push(row_index);
    }
    
    /// Index'den bir row siler
    pub fn remove(&mut self, value: &TypedValue, row_index: usize) {
        if let Some(indices) = self.index_data.get_mut(value) {
            indices.retain(|&idx| idx != row_index);
            if indices.is_empty() {
                self.index_data.remove(value);
            }
        }
    }
    
    /// Index'den bir row'un value'sunu günceller
    pub fn update(&mut self, old_value: &TypedValue, new_value: TypedValue, row_index: usize) {
        self.remove(old_value, row_index);
        self.insert(new_value, row_index);
    }
    
    /// Belirli bir value için row index'lerini getirir
    pub fn get_row_indices(&self, value: &TypedValue) -> Option<&Vec<usize>> {
        self.index_data.get(value)
    }
    
    /// Range query: start_value <= x < end_value
    pub fn get_range_indices(&self, start_value: &TypedValue, end_value: &TypedValue) -> Vec<usize> {
        let mut result = Vec::new();
        for (_, indices) in self.index_data.range(start_value..end_value) {
            result.extend(indices.iter());
        }
        result.sort();
        result.dedup();
        result
    }
    
    /// Range query: start_value <= x <= end_value (inclusive)
    pub fn get_range_indices_inclusive(&self, start_value: &TypedValue, end_value: &TypedValue) -> Vec<usize> {
        let mut result = Vec::new();
        for (_, indices) in self.index_data.range(start_value..=end_value) {
            result.extend(indices.iter());
        }
        result.sort();
        result.dedup();
        result
    }
    
    /// Greater than query: x > value
    pub fn get_greater_than_indices(&self, value: &TypedValue) -> Vec<usize> {
        let mut result = Vec::new();
        use std::ops::Bound;
        for (_, indices) in self.index_data.range((Bound::Excluded(value), Bound::Unbounded)) {
            result.extend(indices.iter());
        }
        result.sort();
        result.dedup();
        result
    }
    
    /// Greater than or equal query: x >= value
    pub fn get_greater_equal_indices(&self, value: &TypedValue) -> Vec<usize> {
        let mut result = Vec::new();
        for (_, indices) in self.index_data.range(value..) {
            result.extend(indices.iter());
        }
        result.sort();
        result.dedup();
        result
    }
    
    /// Less than query: x < value
    pub fn get_less_than_indices(&self, value: &TypedValue) -> Vec<usize> {
        let mut result = Vec::new();
        for (_, indices) in self.index_data.range(..value) {
            result.extend(indices.iter());
        }
        result.sort();
        result.dedup();
        result
    }
    
    /// Less than or equal query: x <= value
    pub fn get_less_equal_indices(&self, value: &TypedValue) -> Vec<usize> {
        let mut result = Vec::new();
        for (_, indices) in self.index_data.range(..=value) {
            result.extend(indices.iter());
        }
        result.sort();
        result.dedup();
        result
    }
    
    /// Index'i tamamen yeniden oluşturur
    pub fn rebuild_from_rows(&mut self, rows: &[Row]) {
        self.index_data.clear();
        for (row_index, row) in rows.iter().enumerate() {
            if let Some(value) = row.get(&self.column_name) {
                self.insert(value.clone(), row_index);
            }
        }
    }
    
    /// Tüm row index'lerini günceller (silme işlemi sonrası)
    pub fn reindex_after_deletion(&mut self, deleted_index: usize) {
        for indices in self.index_data.values_mut() {
            // Silinen index'den sonraki tüm index'leri bir azalt
            for idx in indices.iter_mut() {
                if *idx > deleted_index {
                    *idx -= 1;
                }
            }
        }
    }
}

/// Unified index wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Index {
    Hash(HashIndex),
    BTree(BTreeIndex),
}

impl Index {
    pub fn new_hash(column_name: String) -> Self {
        Index::Hash(HashIndex::new(column_name))
    }
    
    pub fn new_btree(column_name: String) -> Self {
        Index::BTree(BTreeIndex::new(column_name))
    }
    
    pub fn get_column_name(&self) -> &str {
        match self {
            Index::Hash(idx) => &idx.column_name,
            Index::BTree(idx) => &idx.column_name,
        }
    }
    
    pub fn insert(&mut self, value: TypedValue, row_index: usize) {
        match self {
            Index::Hash(idx) => idx.insert(value, row_index),
            Index::BTree(idx) => idx.insert(value, row_index),
        }
    }
    
    pub fn remove(&mut self, value: &TypedValue, row_index: usize) {
        match self {
            Index::Hash(idx) => idx.remove(value, row_index),
            Index::BTree(idx) => idx.remove(value, row_index),
        }
    }
    
    pub fn update(&mut self, old_value: &TypedValue, new_value: TypedValue, row_index: usize) {
        match self {
            Index::Hash(idx) => idx.update(old_value, new_value, row_index),
            Index::BTree(idx) => idx.update(old_value, new_value, row_index),
        }
    }
    
    pub fn get_row_indices(&self, value: &TypedValue) -> Option<&Vec<usize>> {
        match self {
            Index::Hash(idx) => idx.get_row_indices(value),
            Index::BTree(idx) => idx.get_row_indices(value),
        }
    }
    
    pub fn rebuild_from_rows(&mut self, rows: &[Row]) {
        match self {
            Index::Hash(idx) => idx.rebuild_from_rows(rows),
            Index::BTree(idx) => idx.rebuild_from_rows(rows),
        }
    }
    
    pub fn reindex_after_deletion(&mut self, deleted_index: usize) {
        match self {
            Index::Hash(idx) => idx.reindex_after_deletion(deleted_index),
            Index::BTree(idx) => idx.reindex_after_deletion(deleted_index),
        }
    }
    
    /// Range query methods (only available for B-Tree indexes)
    pub fn get_range_indices(&self, start_value: &TypedValue, end_value: &TypedValue) -> Option<Vec<usize>> {
        match self {
            Index::BTree(idx) => Some(idx.get_range_indices(start_value, end_value)),
            Index::Hash(_) => None,
        }
    }
    
    pub fn get_range_indices_inclusive(&self, start_value: &TypedValue, end_value: &TypedValue) -> Option<Vec<usize>> {
        match self {
            Index::BTree(idx) => Some(idx.get_range_indices_inclusive(start_value, end_value)),
            Index::Hash(_) => None,
        }
    }
    
    pub fn get_greater_than_indices(&self, value: &TypedValue) -> Option<Vec<usize>> {
        match self {
            Index::BTree(idx) => Some(idx.get_greater_than_indices(value)),
            Index::Hash(_) => None,
        }
    }
    
    pub fn get_greater_equal_indices(&self, value: &TypedValue) -> Option<Vec<usize>> {
        match self {
            Index::BTree(idx) => Some(idx.get_greater_equal_indices(value)),
            Index::Hash(_) => None,
        }
    }
    
    pub fn get_less_than_indices(&self, value: &TypedValue) -> Option<Vec<usize>> {
        match self {
            Index::BTree(idx) => Some(idx.get_less_than_indices(value)),
            Index::Hash(_) => None,
        }
    }
    
    pub fn get_less_equal_indices(&self, value: &TypedValue) -> Option<Vec<usize>> {
        match self {
            Index::BTree(idx) => Some(idx.get_less_equal_indices(value)),
            Index::Hash(_) => None,
        }
    }
    
    /// Index'i temizler
    pub fn clear(&mut self) {
        match self {
            Index::Hash(idx) => idx.index_data.clear(),
            Index::BTree(idx) => idx.index_data.clear(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
    #[serde(default)]
    pub indexes: HashMap<String, Index>, // column_name -> Index (Hash or BTree)
    #[serde(default)]
    pub stats: TableStats, // Statistics tracking
    #[serde(default)]
    pub storage_format: StorageFormat,
    #[serde(default)]
    pub column_store: Option<ColumnStore>, // Optional columnar storage
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        let stats = TableStats::new(name.clone(), &columns);
        Self {
            name,
            columns,
            rows: Vec::new(),
            indexes: HashMap::new(),
            stats,
            storage_format: StorageFormat::RowBased,
            column_store: None,
        }
    }

    /// Hash index oluşturur
    pub fn create_index(&mut self, column_name: String) -> Result<(), String> {
        self.create_index_with_type(column_name, IndexType::Hash)
    }
    
    /// Belirli tipte index oluşturur
    pub fn create_index_with_type(&mut self, column_name: String, index_type: IndexType) -> Result<(), String> {
        // Kolon var mı kontrol et
        if !self.columns.iter().any(|col| col.name == column_name) {
            return Err(format!("Column '{}' does not exist", column_name));
        }
        
        // Index zaten var mı kontrol et
        if self.indexes.contains_key(&column_name) {
            return Err(format!("Index on column '{}' already exists", column_name));
        }
        
        // Yeni index oluştur ve mevcut verilerle doldur
        let mut index = match index_type {
            IndexType::Hash => Index::new_hash(column_name.clone()),
            IndexType::BTree => Index::new_btree(column_name.clone()),
        };
        
        index.rebuild_from_rows(&self.rows);
        self.indexes.insert(column_name, index);
        Ok(())
    }
    
    /// Index'i siler
    pub fn drop_index(&mut self, column_name: &str) -> Result<(), String> {
        if self.indexes.remove(column_name).is_some() {
            Ok(())
        } else {
            Err(format!("Index on column '{}' does not exist", column_name))
        }
    }
    
    /// Belirli bir column için index var mı kontrol eder
    pub fn has_index(&self, column_name: &str) -> bool {
        self.indexes.contains_key(column_name)
    }
    
    /// Index'i kullanarak equality sorgusu için row index'lerini getirir
    pub fn get_indexed_rows(&mut self, column_name: &str, value: &TypedValue) -> Option<Vec<usize>> {
        let start_time = Instant::now();
        let result = self.indexes.get(column_name)?.get_row_indices(value).cloned();
        
        // Index usage tracking
        if let Some(index) = self.indexes.get(column_name) {
            let index_type = match index {
                Index::Hash(_) => IndexType::Hash,
                Index::BTree(_) => IndexType::BTree,
            };
            self.stats.record_index_usage(column_name, index_type, start_time.elapsed());
        }
        
        result
    }
    
    /// Index'i kullanarak range sorgusu için row index'lerini getirir
    pub fn get_indexed_range_rows(&mut self, column_name: &str, start_value: &TypedValue, end_value: &TypedValue, inclusive: bool) -> Option<Vec<usize>> {
        let start_time = Instant::now();
        let index = self.indexes.get(column_name)?;
        let result = if inclusive {
            index.get_range_indices_inclusive(start_value, end_value)
        } else {
            index.get_range_indices(start_value, end_value)
        };
        
        // Index usage tracking
        if let Some(index) = self.indexes.get(column_name) {
            let index_type = match index {
                Index::Hash(_) => IndexType::Hash,
                Index::BTree(_) => IndexType::BTree,
            };
            self.stats.record_index_usage(column_name, index_type, start_time.elapsed());
        }
        
        result
    }
    
    /// Index'i kullanarak comparison sorgusu için row index'lerini getirir
    pub fn get_indexed_comparison_rows(&mut self, column_name: &str, value: &TypedValue, operator: &str) -> Option<Vec<usize>> {
        let start_time = Instant::now();
        let index = self.indexes.get(column_name)?;
        let result = match operator {
            ">" => index.get_greater_than_indices(value),
            ">=" => index.get_greater_equal_indices(value),
            "<" => index.get_less_than_indices(value),
            "<=" => index.get_less_equal_indices(value),
            _ => None,
        };
        
        // Index usage tracking
        if let Some(index) = self.indexes.get(column_name) {
            let index_type = match index {
                Index::Hash(_) => IndexType::Hash,
                Index::BTree(_) => IndexType::BTree,
            };
            self.stats.record_index_usage(column_name, index_type, start_time.elapsed());
        }
        
        result
    }

    pub fn insert_row(&mut self, row: Row) {
        let row_index = self.rows.len();
        
        // Statistics'i güncelle
        self.stats.record_insert(&row);
        
        // Row'u ekle
        self.rows.push(row);
        
        // Tüm index'leri güncelle
        for (column_name, index) in &mut self.indexes {
            if let Some(value) = self.rows[row_index].get(column_name) {
                index.insert(value.clone(), row_index);
            }
        }
        
        // Columnar storage'ı güncelle
        if let Some(ref mut column_store) = self.column_store {
            column_store.insert_row(&self.rows[row_index]);
        }
    }
    
    /// Row'u günceller ve index'leri maintain eder
    pub fn update_row(&mut self, row_index: usize, column_name: &str, new_value: TypedValue) -> Result<(), String> {
        if row_index >= self.rows.len() {
            return Err("Row index out of bounds".to_string());
        }
        
        // Eski value'yu al
        let old_value = self.rows[row_index].get(column_name).cloned();
        
        // Statistics'i güncelle
        if let Some(old_val) = &old_value {
            self.stats.record_update(column_name, old_val, &new_value);
        } else {
            self.stats.record_update(column_name, &TypedValue::Null, &new_value);
        }
        
        // Row'u güncelle
        self.rows[row_index].insert(column_name.to_string(), new_value.clone());
        
        // Index'i güncelle
        if let Some(index) = self.indexes.get_mut(column_name) {
            if let Some(old_val) = old_value {
                index.update(&old_val, new_value, row_index);
            } else {
                index.insert(new_value, row_index);
            }
        }
        
        Ok(())
    }
    
    /// Row'u siler ve index'leri maintain eder
    pub fn delete_row(&mut self, row_index: usize) -> Result<(), String> {
        if row_index >= self.rows.len() {
            return Err("Row index out of bounds".to_string());
        }
        
        // Row'dan değerleri al
        let row = &self.rows[row_index];
        
        // Statistics'i güncelle
        self.stats.record_delete(row);
        
        // Tüm index'lerden bu row'u sil
        for (column_name, index) in &mut self.indexes {
            if let Some(value) = row.get(column_name) {
                index.remove(value, row_index);
            }
        }
        
        // Row'u sil
        self.rows.remove(row_index);
        
        // Tüm index'lerde silinen index'den sonraki index'leri güncelle
        for index in self.indexes.values_mut() {
            index.reindex_after_deletion(row_index);
        }
        
        Ok(())
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
        // Index'leri yeniden oluştur
        for index in self.indexes.values_mut() {
            index.rebuild_from_rows(&self.rows);
        }
        // Statistics'i yeniden oluştur
        self.stats.rebuild_from_rows(&self.rows);
        // Columnar storage'ı yeniden oluştur
        if let Some(ref mut column_store) = self.column_store {
            *column_store = ColumnStore::from_rows(self.name.clone(), self.columns.clone(), &self.rows);
        }
    }

    pub fn clear(&mut self) {
        self.rows.clear();
        // Index'leri temizle
        for index in self.indexes.values_mut() {
            index.clear();
        }
        // Statistics'i yeniden oluştur
        self.stats.rebuild_from_rows(&self.rows);
    }
    
    pub fn clear_rows(&mut self) {
        self.rows.clear();
        // Index'leri temizle
        for index in self.indexes.values_mut() {
            index.clear();
        }
        // Statistics'i yeniden oluştur
        self.stats.rebuild_from_rows(&self.rows);
        // Columnar storage'ı temizle
        if let Some(ref mut column_store) = self.column_store {
            column_store.clear();
        }
    }
    
    /// Convert table to columnar storage format
    pub fn convert_to_columnar(&mut self) -> Result<(), String> {
        if self.storage_format == StorageFormat::ColumnBased {
            return Ok(()); // Already in columnar format
        }
        
        let column_store = ColumnStore::from_rows(self.name.clone(), self.columns.clone(), &self.rows);
        self.column_store = Some(column_store);
        self.storage_format = StorageFormat::ColumnBased;
        
        println!("✅ Table '{}' converted to columnar storage", self.name);
        Ok(())
    }
    
    /// Convert table to row-based storage format
    pub fn convert_to_row_based(&mut self) -> Result<(), String> {
        if self.storage_format == StorageFormat::RowBased {
            return Ok(()); // Already in row-based format
        }
        
        if let Some(ref column_store) = self.column_store {
            self.rows = column_store.to_rows();
        }
        
        self.column_store = None;
        self.storage_format = StorageFormat::RowBased;
        
        println!("✅ Table '{}' converted to row-based storage", self.name);
        Ok(())
    }
    
    /// Set storage format (automatic conversion)
    pub fn set_storage_format(&mut self, format: StorageFormat) -> Result<(), String> {
        match format {
            StorageFormat::RowBased => self.convert_to_row_based(),
            StorageFormat::ColumnBased => self.convert_to_columnar(),
            StorageFormat::Hybrid => {
                // For hybrid mode, choose based on table characteristics
                if self.should_use_columnar_storage() {
                    self.convert_to_columnar()
                } else {
                    self.convert_to_row_based()
                }
            }
        }
    }
    
    /// Determine if table should use columnar storage based on characteristics
    fn should_use_columnar_storage(&self) -> bool {
        // Use columnar storage if:
        // 1. Table has many rows (> 10000)
        // 2. Table has many columns (> 10)
        // 3. Most queries are analytical (aggregations, column scans)
        
        let row_count = self.rows.len();
        let column_count = self.columns.len();
        
        // Simple heuristic: use columnar for large tables
        row_count > 10000 || column_count > 10
    }
    
    /// Get column analytics for optimization
    pub fn get_column_analytics(&self, column_name: &str) -> Option<ColumnAnalytics> {
        match &self.column_store {
            Some(column_store) => column_store.get_column_stats(column_name),
            None => {
                // Convert to columnar temporarily for analytics
                let temp_column_store = ColumnStore::from_rows(
                    self.name.clone(),
                    self.columns.clone(),
                    &self.rows,
                );
                temp_column_store.get_column_stats(column_name)
            }
        }
    }
    
    /// Execute analytical query using columnar storage
    pub fn execute_analytical_query(&self, column_name: &str, operation: AggregateOperation) -> Option<TypedValue> {
        match &self.column_store {
            Some(column_store) => column_store.aggregate_column(column_name, operation),
            None => {
                // Use row-based aggregation as fallback
                self.aggregate_column_row_based(column_name, operation)
            }
        }
    }
    
    /// Row-based aggregation fallback
    fn aggregate_column_row_based(&self, column_name: &str, operation: AggregateOperation) -> Option<TypedValue> {
        let _column_index = self.columns.iter().position(|c| c.name == column_name)?;
        let values: Vec<&TypedValue> = self.rows.iter()
            .filter_map(|row| row.get(column_name))
            .collect();
        
        if values.is_empty() {
            return Some(TypedValue::Null);
        }
        
        match operation {
            AggregateOperation::Count => Some(TypedValue::Integer(values.len() as i64)),
            AggregateOperation::Sum => {
                let mut sum = 0i64;
                for value in values {
                    if let TypedValue::Integer(i) = value {
                        sum += i;
                    }
                }
                Some(TypedValue::Integer(sum))
            }
            AggregateOperation::Avg => {
                let mut sum = 0i64;
                let mut count = 0;
                for value in values {
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
                let mut min_value: Option<&TypedValue> = None;
                for value in values {
                    if *value != TypedValue::Null {
                        if min_value.is_none() || value < min_value.unwrap() {
                            min_value = Some(value);
                        }
                    }
                }
                Some(min_value.unwrap_or(&TypedValue::Null).clone())
            }
            AggregateOperation::Max => {
                let mut max_value: Option<&TypedValue> = None;
                for value in values {
                    if *value != TypedValue::Null {
                        if max_value.is_none() || value > max_value.unwrap() {
                            max_value = Some(value);
                        }
                    }
                }
                Some(max_value.unwrap_or(&TypedValue::Null).clone())
            }
        }
    }
    
    /// Compress column data (only for columnar storage)
    pub fn compress_columns(&mut self) -> Result<(), String> {
        match &mut self.column_store {
            Some(column_store) => {
                column_store.compress_columns();
                Ok(())
            }
            None => Err("Table is not using columnar storage".to_string())
        }
    }
    
    /// Get storage format information
    pub fn get_storage_info(&self) -> HashMap<String, String> {
        let mut info = HashMap::new();
        
        info.insert("storage_format".to_string(), format!("{:?}", self.storage_format));
        info.insert("row_count".to_string(), self.rows.len().to_string());
        info.insert("column_count".to_string(), self.columns.len().to_string());
        
        if let Some(ref column_store) = self.column_store {
            info.insert("columnar_row_count".to_string(), column_store.len().to_string());
            info.insert("compression_ratio".to_string(), format!("{:.2}", column_store.compression_ratio));
            if let Some(ref last_compressed) = column_store.last_compressed {
                info.insert("last_compressed".to_string(), last_compressed.clone());
            }
        }
        
        info
    }
} 

/// Column-level statistics for query optimization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnStats {
    pub column_name: String,
    pub unique_count: usize,
    pub null_count: usize,
    pub total_count: usize,
    pub min_value: Option<TypedValue>,
    pub max_value: Option<TypedValue>,
    pub value_frequency: HashMap<TypedValue, usize>, // value -> frequency
    pub last_updated: String, // ISO 8601 timestamp
}

impl ColumnStats {
    pub fn new(column_name: String) -> Self {
        Self {
            column_name,
            unique_count: 0,
            null_count: 0,
            total_count: 0,
            min_value: None,
            max_value: None,
            value_frequency: HashMap::new(),
            last_updated: chrono::Utc::now().to_rfc3339(),
        }
    }
    
    /// Calculates null ratio (0.0 to 1.0)
    pub fn null_ratio(&self) -> f64 {
        if self.total_count == 0 {
            0.0
        } else {
            self.null_count as f64 / self.total_count as f64
        }
    }
    
    /// Calculates selectivity for a specific value (0.0 to 1.0)
    pub fn selectivity(&self, value: &TypedValue) -> f64 {
        if self.total_count == 0 {
            0.0
        } else {
            let frequency = self.value_frequency.get(value).unwrap_or(&0);
            *frequency as f64 / self.total_count as f64
        }
    }
    
    /// Gets the most frequent value
    pub fn most_frequent_value(&self) -> Option<(&TypedValue, usize)> {
        self.value_frequency.iter().max_by_key(|(_, &freq)| freq).map(|(value, &freq)| (value, freq))
    }
    
    /// Gets cardinality (unique values / total values)
    pub fn cardinality(&self) -> f64 {
        if self.total_count == 0 {
            0.0
        } else {
            self.unique_count as f64 / self.total_count as f64
        }
    }
    
    /// Updates statistics when inserting a value
    pub fn insert_value(&mut self, value: TypedValue) {
        self.total_count += 1;
        
        if matches!(value, TypedValue::Null) {
            self.null_count += 1;
        } else {
            // Update min/max
            if self.min_value.is_none() || value < *self.min_value.as_ref().unwrap() {
                self.min_value = Some(value.clone());
            }
            if self.max_value.is_none() || value > *self.max_value.as_ref().unwrap() {
                self.max_value = Some(value.clone());
            }
            
            // Update frequency
            let freq = self.value_frequency.entry(value).or_insert(0);
            if *freq == 0 {
                self.unique_count += 1;
            }
            *freq += 1;
        }
        
        self.last_updated = chrono::Utc::now().to_rfc3339();
    }
    
    /// Updates statistics when removing a value
    pub fn remove_value(&mut self, value: &TypedValue) {
        if self.total_count > 0 {
            self.total_count -= 1;
            
            if matches!(value, TypedValue::Null) {
                self.null_count = self.null_count.saturating_sub(1);
            } else {
                if let Some(freq) = self.value_frequency.get_mut(value) {
                    *freq -= 1;
                    if *freq == 0 {
                        self.value_frequency.remove(value);
                        self.unique_count = self.unique_count.saturating_sub(1);
                    }
                }
            }
            
            self.last_updated = chrono::Utc::now().to_rfc3339();
        }
    }
    
    /// Rebuilds statistics from scratch
    pub fn rebuild_from_rows(&mut self, rows: &[Row]) {
        self.unique_count = 0;
        self.null_count = 0;
        self.total_count = 0;
        self.min_value = None;
        self.max_value = None;
        self.value_frequency.clear();
        
        for row in rows {
            if let Some(value) = row.get(&self.column_name) {
                self.insert_value(value.clone());
            } else {
                self.insert_value(TypedValue::Null);
            }
        }
    }
}

/// Index usage statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexUsageStats {
    pub index_name: String,
    pub column_name: String,
    pub index_type: IndexType,
    pub usage_count: usize,
    pub last_used: Option<String>, // ISO 8601 timestamp
    pub avg_lookup_time_ns: f64,
    pub total_lookup_time_ns: u64,
    pub created_at: String, // ISO 8601 timestamp
}

impl IndexUsageStats {
    pub fn new(index_name: String, column_name: String, index_type: IndexType) -> Self {
        Self {
            index_name,
            column_name,
            index_type,
            usage_count: 0,
            last_used: None,
            avg_lookup_time_ns: 0.0,
            total_lookup_time_ns: 0,
            created_at: chrono::Utc::now().to_rfc3339(),
        }
    }
    
    /// Records a lookup operation
    pub fn record_lookup(&mut self, duration: Duration) {
        self.usage_count += 1;
        self.last_used = Some(chrono::Utc::now().to_rfc3339());
        
        let duration_ns = duration.as_nanos() as u64;
        self.total_lookup_time_ns += duration_ns;
        self.avg_lookup_time_ns = self.total_lookup_time_ns as f64 / self.usage_count as f64;
    }
    
    /// Gets usage frequency (uses per time unit)
    pub fn usage_frequency(&self) -> f64 {
        if let Some(last_used) = &self.last_used {
            if let (Ok(created), Ok(last)) = (
                chrono::DateTime::parse_from_rfc3339(&self.created_at),
                chrono::DateTime::parse_from_rfc3339(last_used)
            ) {
                let duration = last - created;
                let hours = duration.num_hours() as f64;
                if hours > 0.0 {
                    return self.usage_count as f64 / hours;
                }
            }
        }
        0.0
    }
}

/// Query execution statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryStats {
    pub query_pattern: String, // e.g., "SELECT * FROM users WHERE age > ?"
    pub execution_count: usize,
    pub total_execution_time_ns: u64,
    pub avg_execution_time_ns: f64,
    pub min_execution_time_ns: u64,
    pub max_execution_time_ns: u64,
    pub rows_returned_total: usize,
    pub avg_rows_returned: f64,
    pub last_executed: String, // ISO 8601 timestamp
    pub created_at: String, // ISO 8601 timestamp
}

impl QueryStats {
    pub fn new(query_pattern: String) -> Self {
        Self {
            query_pattern,
            execution_count: 0,
            total_execution_time_ns: 0,
            avg_execution_time_ns: 0.0,
            min_execution_time_ns: u64::MAX,
            max_execution_time_ns: 0,
            rows_returned_total: 0,
            avg_rows_returned: 0.0,
            last_executed: chrono::Utc::now().to_rfc3339(),
            created_at: chrono::Utc::now().to_rfc3339(),
        }
    }
    
    /// Records a query execution
    pub fn record_execution(&mut self, duration: Duration, rows_returned: usize) {
        self.execution_count += 1;
        self.last_executed = chrono::Utc::now().to_rfc3339();
        
        let duration_ns = duration.as_nanos() as u64;
        self.total_execution_time_ns += duration_ns;
        self.avg_execution_time_ns = self.total_execution_time_ns as f64 / self.execution_count as f64;
        
        if duration_ns < self.min_execution_time_ns {
            self.min_execution_time_ns = duration_ns;
        }
        if duration_ns > self.max_execution_time_ns {
            self.max_execution_time_ns = duration_ns;
        }
        
        self.rows_returned_total += rows_returned;
        self.avg_rows_returned = self.rows_returned_total as f64 / self.execution_count as f64;
    }
    
    /// Gets execution frequency (executions per hour)
    pub fn execution_frequency(&self) -> f64 {
        if let (Ok(created), Ok(last)) = (
            chrono::DateTime::parse_from_rfc3339(&self.created_at),
            chrono::DateTime::parse_from_rfc3339(&self.last_executed)
        ) {
            let duration = last - created;
            let hours = duration.num_hours() as f64;
            if hours > 0.0 {
                return self.execution_count as f64 / hours;
            }
        }
        0.0
    }
}

/// Table-level statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableStats {
    pub table_name: String,
    pub column_stats: HashMap<String, ColumnStats>,
    pub index_usage_stats: HashMap<String, IndexUsageStats>,
    pub query_stats: HashMap<String, QueryStats>,
    pub total_rows: usize,
    pub total_inserts: usize,
    pub total_updates: usize,
    pub total_deletes: usize,
    pub total_selects: usize,
    pub created_at: String, // ISO 8601 timestamp
    pub last_updated: String, // ISO 8601 timestamp
}

impl TableStats {
    pub fn new(table_name: String, columns: &[Column]) -> Self {
        let mut column_stats = HashMap::new();
        for column in columns {
            column_stats.insert(column.name.clone(), ColumnStats::new(column.name.clone()));
        }
        
        Self {
            table_name,
            column_stats,
            index_usage_stats: HashMap::new(),
            query_stats: HashMap::new(),
            total_rows: 0,
            total_inserts: 0,
            total_updates: 0,
            total_deletes: 0,
            total_selects: 0,
            created_at: chrono::Utc::now().to_rfc3339(),
            last_updated: chrono::Utc::now().to_rfc3339(),
        }
    }
}

impl Default for TableStats {
    fn default() -> Self {
        Self {
            table_name: String::new(),
            column_stats: HashMap::new(),
            index_usage_stats: HashMap::new(),
            query_stats: HashMap::new(),
            total_rows: 0,
            total_inserts: 0,
            total_updates: 0,
            total_deletes: 0,
            total_selects: 0,
            created_at: chrono::Utc::now().to_rfc3339(),
            last_updated: chrono::Utc::now().to_rfc3339(),
        }
    }
}

impl TableStats {
    /// Records an insert operation
    pub fn record_insert(&mut self, row: &Row) {
        self.total_inserts += 1;
        self.total_rows += 1;
        
        // Update column statistics
        for (column_name, column_stats) in &mut self.column_stats {
            if let Some(value) = row.get(column_name) {
                column_stats.insert_value(value.clone());
            } else {
                column_stats.insert_value(TypedValue::Null);
            }
        }
        
        self.last_updated = chrono::Utc::now().to_rfc3339();
    }
    
    /// Records an update operation
    pub fn record_update(&mut self, column_name: &str, old_value: &TypedValue, new_value: &TypedValue) {
        self.total_updates += 1;
        
        // Update column statistics
        if let Some(column_stats) = self.column_stats.get_mut(column_name) {
            column_stats.remove_value(old_value);
            column_stats.insert_value(new_value.clone());
        }
        
        self.last_updated = chrono::Utc::now().to_rfc3339();
    }
    
    /// Records a delete operation
    pub fn record_delete(&mut self, row: &Row) {
        self.total_deletes += 1;
        self.total_rows = self.total_rows.saturating_sub(1);
        
        // Update column statistics
        for (column_name, column_stats) in &mut self.column_stats {
            if let Some(value) = row.get(column_name) {
                column_stats.remove_value(value);
            } else {
                column_stats.remove_value(&TypedValue::Null);
            }
        }
        
        self.last_updated = chrono::Utc::now().to_rfc3339();
    }
    
    /// Records a select operation
    pub fn record_select(&mut self, query_pattern: String, execution_time: Duration, rows_returned: usize) {
        self.total_selects += 1;
        
        // Update query statistics
        let query_stats = self.query_stats.entry(query_pattern.clone()).or_insert_with(|| QueryStats::new(query_pattern));
        query_stats.record_execution(execution_time, rows_returned);
        
        self.last_updated = chrono::Utc::now().to_rfc3339();
    }
    
    /// Records index usage
    pub fn record_index_usage(&mut self, column_name: &str, index_type: IndexType, lookup_time: Duration) {
        let index_name = format!("{}_{:?}", column_name, index_type);
        
        let index_stats = self.index_usage_stats.entry(index_name.clone()).or_insert_with(|| {
            IndexUsageStats::new(index_name, column_name.to_string(), index_type)
        });
        
        index_stats.record_lookup(lookup_time);
        self.last_updated = chrono::Utc::now().to_rfc3339();
    }
    
    /// Rebuilds all statistics from scratch
    pub fn rebuild_from_rows(&mut self, rows: &[Row]) {
        self.total_rows = rows.len();
        
        // Rebuild column statistics
        for column_stats in self.column_stats.values_mut() {
            column_stats.rebuild_from_rows(rows);
        }
        
        self.last_updated = chrono::Utc::now().to_rfc3339();
    }
    
    /// Gets column with highest selectivity for a given value
    pub fn most_selective_column(&self, conditions: &HashMap<String, TypedValue>) -> Option<String> {
        let mut best_column = None;
        let mut best_selectivity = 1.0;
        
        for (column_name, value) in conditions {
            if let Some(column_stats) = self.column_stats.get(column_name) {
                let selectivity = column_stats.selectivity(value);
                if selectivity < best_selectivity {
                    best_selectivity = selectivity;
                    best_column = Some(column_name.clone());
                }
            }
        }
        
        best_column
    }
    
    /// Gets recommended index for a column based on cardinality
    pub fn recommend_index_type(&self, column_name: &str) -> Option<IndexType> {
        if let Some(column_stats) = self.column_stats.get(column_name) {
            let cardinality = column_stats.cardinality();
            
            // High cardinality: BTree for range queries
            // Low cardinality: Hash for exact matches
            if cardinality > 0.1 {
                Some(IndexType::BTree)
            } else {
                Some(IndexType::Hash)
            }
        } else {
            None
        }
    }
} 