use crate::row::Row;
use crate::table::Table;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Serialize, Deserialize};

/// Transaction ID type
pub type TransactionId = u64;

/// Row version for MVCC
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowVersion {
    pub transaction_id: TransactionId,
    pub commit_timestamp: u64,
    pub data: Row,
    pub is_deleted: bool,
}

/// Transaction state
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionState {
    Active,
    Committed,
    Aborted,
}

/// Transaction isolation level
#[derive(Debug, Clone, PartialEq)]
pub enum IsolationLevel {
    ReadCommitted,
    RepeatableRead,
    Serializable,
}

/// Transaction struct
#[derive(Debug, Clone)]
pub struct Transaction {
    pub id: TransactionId,
    pub state: TransactionState,
    pub isolation_level: IsolationLevel,
    pub start_timestamp: u64,
    pub commit_timestamp: Option<u64>,
    pub read_set: HashSet<String>, // table_name:row_id
    pub write_set: HashMap<String, Row>, // table_name:row_id -> new_row
    pub deleted_rows: HashSet<String>, // table_name:row_id
    pub snapshot: HashMap<String, HashMap<usize, RowVersion>>, // table_name -> row_id -> version
}

impl Transaction {
    pub fn new(id: TransactionId, isolation_level: IsolationLevel) -> Self {
        let start_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        Self {
            id,
            state: TransactionState::Active,
            isolation_level,
            start_timestamp,
            commit_timestamp: None,
            read_set: HashSet::new(),
            write_set: HashMap::new(),
            deleted_rows: HashSet::new(),
            snapshot: HashMap::new(),
        }
    }

    pub fn is_active(&self) -> bool {
        self.state == TransactionState::Active
    }

    pub fn commit(&mut self) {
        self.state = TransactionState::Committed;
        self.commit_timestamp = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
        );
    }

    pub fn abort(&mut self) {
        self.state = TransactionState::Aborted;
    }

    pub fn add_read(&mut self, table_name: &str, row_id: usize) {
        self.read_set.insert(format!("{}:{}", table_name, row_id));
    }

    pub fn add_write(&mut self, table_name: &str, row_id: usize, row: Row) {
        self.write_set.insert(format!("{}:{}", table_name, row_id), row);
    }

    pub fn add_delete(&mut self, table_name: &str, row_id: usize) {
        self.deleted_rows.insert(format!("{}:{}", table_name, row_id));
    }

    pub fn create_snapshot(&mut self, tables: &HashMap<String, Arc<RwLock<Table>>>) {
        for (table_name, table) in tables {
            let table_guard = table.read().unwrap();
            let mut table_snapshot = HashMap::new();
            
            for (row_id, row) in table_guard.rows.iter().enumerate() {
                let version = RowVersion {
                    transaction_id: 0, // committed version
                    commit_timestamp: self.start_timestamp,
                    data: row.clone(),
                    is_deleted: false,
                };
                table_snapshot.insert(row_id, version);
            }
            
            self.snapshot.insert(table_name.clone(), table_snapshot);
        }
    }
}

/// Transaction manager
#[derive(Debug, Default)]
pub struct TransactionManager {
    next_transaction_id: TransactionId,
    active_transactions: HashMap<TransactionId, Transaction>,
    committed_transactions: VecDeque<TransactionId>,
    row_versions: HashMap<String, Vec<RowVersion>>, // table_name:row_id -> versions
    deadlock_detection_enabled: bool,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            next_transaction_id: 1,
            active_transactions: HashMap::new(),
            committed_transactions: VecDeque::new(),
            row_versions: HashMap::new(),
            deadlock_detection_enabled: true,
        }
    }

    pub fn begin_transaction(&mut self, isolation_level: IsolationLevel) -> TransactionId {
        let transaction_id = self.next_transaction_id;
        self.next_transaction_id += 1;
        
        let transaction = Transaction::new(transaction_id, isolation_level);
        self.active_transactions.insert(transaction_id, transaction);
        
        transaction_id
    }

    pub fn commit_transaction(&mut self, transaction_id: TransactionId) -> Result<(), String> {
        // First, get the transaction and clone it
        let transaction_clone = {
            let transaction = self.active_transactions.get(&transaction_id)
                .ok_or("Transaction not found")?;
            
            if transaction.state != TransactionState::Active {
                return Err("Transaction is not active".to_string());
            }
            
            transaction.clone()
        };
        
        // Check for conflicts
        self.check_conflicts(&transaction_clone)?;
        
        // Apply changes
        self.apply_changes(&transaction_clone)?;
        
        // Commit transaction
        if let Some(transaction) = self.active_transactions.get_mut(&transaction_id) {
            transaction.commit();
        }
        self.committed_transactions.push_back(transaction_id);
        
        // Clean up
        self.active_transactions.remove(&transaction_id);
        
        Ok(())
    }

    pub fn rollback_transaction(&mut self, transaction_id: TransactionId) -> Result<(), String> {
        let transaction = self.active_transactions.get_mut(&transaction_id)
            .ok_or("Transaction not found")?;
        
        if transaction.state != TransactionState::Active {
            return Err("Transaction is not active".to_string());
        }

        transaction.abort();
        self.active_transactions.remove(&transaction_id);
        
        Ok(())
    }

    pub fn get_transaction(&self, transaction_id: TransactionId) -> Option<&Transaction> {
        self.active_transactions.get(&transaction_id)
    }

    pub fn get_transaction_mut(&mut self, transaction_id: TransactionId) -> Option<&mut Transaction> {
        self.active_transactions.get_mut(&transaction_id)
    }

    pub fn get_active_transactions(&self) -> &HashMap<TransactionId, Transaction> {
        &self.active_transactions
    }

    fn check_conflicts(&self, transaction: &Transaction) -> Result<(), String> {
        // Check for write-write conflicts
        for write_key in transaction.write_set.keys() {
            for (other_id, other_transaction) in &self.active_transactions {
                if other_id == &transaction.id {
                    continue;
                }
                
                if other_transaction.write_set.contains_key(write_key) {
                    return Err(format!("Write-write conflict detected on {}", write_key));
                }
                
                if other_transaction.read_set.contains(write_key) {
                    return Err(format!("Write-read conflict detected on {}", write_key));
                }
            }
        }

        // Check for read-write conflicts
        for read_key in &transaction.read_set {
            for (other_id, other_transaction) in &self.active_transactions {
                if other_id == &transaction.id {
                    continue;
                }
                
                if other_transaction.write_set.contains_key(read_key) {
                    return Err(format!("Read-write conflict detected on {}", read_key));
                }
            }
        }

        Ok(())
    }

    fn apply_changes(&mut self, transaction: &Transaction) -> Result<(), String> {
        for (key, new_row) in &transaction.write_set {
            let parts: Vec<&str> = key.split(':').collect();
            if parts.len() != 2 {
                return Err(format!("Invalid key format: {}", key));
            }
            
            let table_name = parts[0];
            let row_id: usize = parts[1].parse().map_err(|_| format!("Invalid row ID: {}", parts[1]))?;
            
            let version = RowVersion {
                transaction_id: transaction.id,
                commit_timestamp: transaction.commit_timestamp.unwrap(),
                data: new_row.clone(),
                is_deleted: false,
            };
            
            let key = format!("{}:{}", table_name, row_id);
            self.row_versions.entry(key).or_insert_with(Vec::new).push(version);
        }

        for deleted_key in &transaction.deleted_rows {
            let parts: Vec<&str> = deleted_key.split(':').collect();
            if parts.len() != 2 {
                return Err(format!("Invalid key format: {}", deleted_key));
            }
            
            let table_name = parts[0];
            let row_id: usize = parts[1].parse().map_err(|_| format!("Invalid row ID: {}", parts[1]))?;
            
            let version = RowVersion {
                transaction_id: transaction.id,
                commit_timestamp: transaction.commit_timestamp.unwrap(),
                data: Row::new(), // empty row for deleted
                is_deleted: true,
            };
            
            let key = format!("{}:{}", table_name, row_id);
            self.row_versions.entry(key).or_insert_with(Vec::new).push(version);
        }

        Ok(())
    }

    pub fn detect_deadlocks(&self) -> Vec<Vec<TransactionId>> {
        if !self.deadlock_detection_enabled {
            return vec![];
        }

        let mut wait_for_graph = HashMap::new();
        
        // Build wait-for graph
        for (tx_id, transaction) in &self.active_transactions {
            for write_key in transaction.write_set.keys() {
                for (other_id, other_transaction) in &self.active_transactions {
                    if other_id == tx_id {
                        continue;
                    }
                    
                    if other_transaction.write_set.contains_key(write_key) || 
                       other_transaction.read_set.contains(write_key) {
                        wait_for_graph.entry(*tx_id).or_insert_with(Vec::new).push(*other_id);
                    }
                }
            }
        }

        // Detect cycles using DFS
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();
        let mut cycles = Vec::new();

        for &tx_id in wait_for_graph.keys() {
            if !visited.contains(&tx_id) {
                let mut path = Vec::new();
                self.dfs_detect_cycle(tx_id, &wait_for_graph, &mut visited, &mut rec_stack, &mut path, &mut cycles);
            }
        }

        cycles
    }

    fn dfs_detect_cycle(
        &self,
        tx_id: TransactionId,
        graph: &HashMap<TransactionId, Vec<TransactionId>>,
        visited: &mut HashSet<TransactionId>,
        rec_stack: &mut HashSet<TransactionId>,
        path: &mut Vec<TransactionId>,
        cycles: &mut Vec<Vec<TransactionId>>,
    ) {
        visited.insert(tx_id);
        rec_stack.insert(tx_id);
        path.push(tx_id);

        if let Some(neighbors) = graph.get(&tx_id) {
            for &neighbor in neighbors {
                if !visited.contains(&neighbor) {
                    self.dfs_detect_cycle(neighbor, graph, visited, rec_stack, path, cycles);
                } else if rec_stack.contains(&neighbor) {
                    // Found a cycle
                    if let Some(start_idx) = path.iter().position(|&x| x == neighbor) {
                        let cycle = path[start_idx..].to_vec();
                        cycles.push(cycle);
                    }
                }
            }
        }

        rec_stack.remove(&tx_id);
        path.pop();
    }

    pub fn resolve_deadlocks(&mut self) -> Vec<TransactionId> {
        let cycles = self.detect_deadlocks();
        let mut aborted_transactions = Vec::new();

        for cycle in cycles {
            // Simple strategy: abort the transaction with the highest ID
            if let Some(&victim_id) = cycle.iter().max() {
                if let Err(_) = self.rollback_transaction(victim_id) {
                    // Transaction might already be aborted
                } else {
                    aborted_transactions.push(victim_id);
                }
            }
        }

        aborted_transactions
    }

    pub fn get_row_version(&self, table_name: &str, row_id: usize, transaction_id: TransactionId) -> Option<&RowVersion> {
        let key = format!("{}:{}", table_name, row_id);
        
        if let Some(versions) = self.row_versions.get(&key) {
            // Find the most recent version visible to this transaction
            for version in versions.iter().rev() {
                if version.transaction_id <= transaction_id && !version.is_deleted {
                    return Some(version);
                }
            }
        }
        
        None
    }

    pub fn cleanup_old_versions(&mut self, cutoff_timestamp: u64) {
        self.row_versions.retain(|_, versions| {
            versions.retain(|version| version.commit_timestamp > cutoff_timestamp);
            !versions.is_empty()
        });
    }
}

/// Transaction-aware table wrapper
pub struct TransactionalTable {
    pub table: Arc<RwLock<Table>>,
    pub transaction_manager: Arc<RwLock<TransactionManager>>,
}

impl TransactionalTable {
    pub fn new(table: Table, transaction_manager: Arc<RwLock<TransactionManager>>) -> Self {
        Self {
            table: Arc::new(RwLock::new(table)),
            transaction_manager,
        }
    }

    pub fn read_row(&self, row_id: usize, transaction_id: TransactionId) -> Result<Option<Row>, String> {
        let tx_manager = self.transaction_manager.read().unwrap();
        
        // Check if we have a version for this row
        if let Some(version) = tx_manager.get_row_version(&self.table.read().unwrap().name, row_id, transaction_id) {
            return Ok(Some(version.data.clone()));
        }
        
        // Fall back to current table state
        let table = self.table.read().unwrap();
        if row_id < table.rows.len() {
            Ok(Some(table.rows[row_id].clone()))
        } else {
            Ok(None)
        }
    }

    pub fn write_row(&self, row_id: usize, row: Row, transaction_id: TransactionId) -> Result<(), String> {
        let mut tx_manager = self.transaction_manager.write().unwrap();
        
        if let Some(transaction) = tx_manager.get_transaction_mut(transaction_id) {
            transaction.add_write(&self.table.read().unwrap().name, row_id, row);
            Ok(())
        } else {
            Err("Transaction not found".to_string())
        }
    }

    pub fn delete_row(&self, row_id: usize, transaction_id: TransactionId) -> Result<(), String> {
        let mut tx_manager = self.transaction_manager.write().unwrap();
        
        if let Some(transaction) = tx_manager.get_transaction_mut(transaction_id) {
            transaction.add_delete(&self.table.read().unwrap().name, row_id);
            Ok(())
        } else {
            Err("Transaction not found".to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Column, DataType};

    #[test]
    fn test_transaction_lifecycle() {
        let mut tx_manager = TransactionManager::new();
        
        let tx_id = tx_manager.begin_transaction(IsolationLevel::ReadCommitted);
        assert!(tx_manager.get_transaction(tx_id).is_some());
        
        assert!(tx_manager.commit_transaction(tx_id).is_ok());
        assert!(tx_manager.get_transaction(tx_id).is_none());
    }

    #[test]
    fn test_transaction_rollback() {
        let mut tx_manager = TransactionManager::new();
        
        let tx_id = tx_manager.begin_transaction(IsolationLevel::ReadCommitted);
        assert!(tx_manager.rollback_transaction(tx_id).is_ok());
        assert!(tx_manager.get_transaction(tx_id).is_none());
    }

    #[test]
    fn test_write_conflict_detection() {
        let mut tx_manager = TransactionManager::new();
        
        let tx1 = tx_manager.begin_transaction(IsolationLevel::ReadCommitted);
        let tx2 = tx_manager.begin_transaction(IsolationLevel::ReadCommitted);
        
        // Both transactions try to write to the same row
        let table_name = "test_table".to_string();
        let row = Row::new();
        
        tx_manager.get_transaction_mut(tx1).unwrap().add_write(&table_name, 0, row.clone());
        tx_manager.get_transaction_mut(tx2).unwrap().add_write(&table_name, 0, row);
        
        // First transaction should commit successfully
        assert!(tx_manager.commit_transaction(tx1).is_ok());
        
        // Second transaction should fail due to conflict
        assert!(tx_manager.commit_transaction(tx2).is_err());
    }

    #[test]
    fn test_deadlock_detection() {
        let mut tx_manager = TransactionManager::new();
        
        let tx1 = tx_manager.begin_transaction(IsolationLevel::ReadCommitted);
        let tx2 = tx_manager.begin_transaction(IsolationLevel::ReadCommitted);
        
        // Create a deadlock: tx1 waits for tx2, tx2 waits for tx1
        let table1 = "table1".to_string();
        let table2 = "table2".to_string();
        let row = Row::new();
        
        tx_manager.get_transaction_mut(tx1).unwrap().add_write(&table1, 0, row.clone());
        tx_manager.get_transaction_mut(tx1).unwrap().add_write(&table2, 0, row.clone());
        
        tx_manager.get_transaction_mut(tx2).unwrap().add_write(&table2, 0, row.clone());
        tx_manager.get_transaction_mut(tx2).unwrap().add_write(&table1, 0, row);
        
        let cycles = tx_manager.detect_deadlocks();
        assert!(!cycles.is_empty());
        
        let aborted = tx_manager.resolve_deadlocks();
        assert!(!aborted.is_empty());
    }
} 