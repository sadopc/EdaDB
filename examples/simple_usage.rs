// examples/simple_usage.rs - Basic usage example for the NoSQL database
// Bu basit örnek temel CRUD operasyonları, query'ler ve transaction'ları gösterir

use nosql_memory_db::{
    MemoryStorage, CrudDatabase, QueryableDatabase, TransactionalStorage,
    ComparisonOperator, SortDirection, IndexType, IsolationLevel
};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::init();

    println!("🚀 NoSQL Memory Database - Simple Usage Example");
    println!("================================================");

    // ================================
    // 1. BASIC CRUD OPERATIONS
    // ================================

    println!("\n📊 1. Basic CRUD Operations");
    println!("============================");

    // Create a new in-memory storage
    let db = MemoryStorage::<Value>::new();

    // Create some sample data
    let users = vec![
        json!({
            "name": "Alice Johnson",
            "age": 28,
            "department": "Engineering",
            "salary": 85000,
            "active": true
        }),
        json!({
            "name": "Bob Smith",
            "age": 34,
            "department": "Sales",
            "salary": 65000,
            "active": true
        }),
        json!({
            "name": "Carol Davis",
            "age": 31,
            "department": "Marketing",
            "salary": 70000,
            "active": false
        }),
    ];

    // Insert users
    let mut user_docs = Vec::new();
    for user in users {
        let doc = db.create(user).await?;
        user_docs.push(doc);
        println!("  ✅ Created user: {}", user_docs.last().unwrap().data["name"]);
    }

    // Read operations
    println!("\n  📖 Reading data:");
    let first_user = db.read_by_id(&user_docs[0].metadata.id).await?;
    if let Some(user) = first_user {
        println!("    • Found user by ID: {}", user.data["name"]);
    }

    // Count documents
    let count = db.count().await?;
    println!("    • Total users: {}", count);

    // Update operation
    println!("\n  ✏️  Updating data:");
    let mut updated_user = user_docs[0].data.clone();
    updated_user["salary"] = json!(90000);
    updated_user["last_promotion"] = json!("2024-12-01");

    let updated_doc = db.update(&user_docs[0].metadata.id, updated_user).await?;
    println!("    • Updated salary: ${}", updated_doc.data["salary"]);

    // ================================
    // 2. INDEX OPERATIONS
    // ================================

    println!("\n🗂️  2. Index Operations");
    println!("=======================");

    // Create indexes for better query performance
    db.create_index("salary_idx", vec!["salary"], IndexType::BTree)?;
    db.create_index("dept_idx", vec!["department"], IndexType::Hash)?;
    db.create_index("age_salary_idx", vec!["age", "salary"], IndexType::Hash)?;

    println!("  ✅ Created indexes:");
    println!("    • salary_idx (BTree) - for range queries");
    println!("    • dept_idx (Hash) - for equality queries");
    println!("    • age_salary_idx (Hash) - composite index");

    // List all indexes
    let indexes = db.list_indexes()?;
    println!("  📋 Active indexes: {}", indexes.len());

    // ================================
    // 3. ADVANCED QUERIES
    // ================================

    println!("\n🔍 3. Advanced Query Operations");
    println!("===============================");

    let db_arc = Arc::new(db);

    // Simple equality query
    let engineers = db_arc.clone().query()
        .where_eq("department", json!("Engineering"))
        .execute().await?;

    println!("  👥 Engineers found: {}", engineers.len());

    // Range query with sorting
    let high_earners = db_arc.clone().query()
        .where_field("salary", ComparisonOperator::GreaterThan, json!(70000))
        .sort_by("salary", SortDirection::Descending)
        .execute().await?;

    println!("  💰 High earners (>$70K): {}", high_earners.len());
    for user in &high_earners {
        println!("    • {}: ${}",
                user["name"].as_str().unwrap_or("Unknown"),
                user["salary"].as_u64().unwrap_or(0));
    }

    // Complex query with multiple conditions
    let active_senior_staff = db_arc.clone().query()
        .where_eq("active", json!(true))
        .where_field("age", ComparisonOperator::GreaterThan, json!(30))
        .where_field("salary", ComparisonOperator::GreaterThanOrEqual, json!(65000))
        .sort_by("salary", SortDirection::Descending)
        .limit(10)
        .execute().await?;

    println!("  🎯 Active senior staff: {}", active_senior_staff.len());

    // Projection query - select specific fields
    let names_and_salaries = db_arc.clone().query()
        .select(vec!["name", "salary"])
        .where_eq("active", json!(true))
        .execute().await?;

    println!("  📝 Name & salary projection:");
    for user in &names_and_salaries {
        println!("    • {}: ${}",
                user["name"].as_str().unwrap_or("Unknown"),
                user["salary"].as_u64().unwrap_or(0));
    }

    // ================================
    // 4. TRANSACTION OPERATIONS
    // ================================

    println!("\n🏦 4. ACID Transaction Operations");
    println!("=================================");

    // Create transactional wrapper
    let storage_for_tx = MemoryStorage::<Value>::new();
    let tx_storage = TransactionalStorage::new(storage_for_tx);

    // Begin transaction
    let tx_id = tx_storage.begin_transaction(
        IsolationLevel::ReadCommitted,
        Some(Duration::from_secs(30))
    ).await?;

    println!("  🔄 Started transaction: {}",
             tx_id.to_string().chars().take(8).collect::<String>());

    // Transactional operations
    let account_data = json!({
        "account_number": "ACC001",
        "owner": "Alice Johnson",
        "balance": 5000.0,
        "account_type": "savings"
    });

    let account_doc = tx_storage.transactional_create(&tx_id, account_data).await?;
    println!("    ➕ Created account in transaction");

    // Read within transaction
    let read_result = tx_storage.transactional_read(&tx_id, &account_doc.metadata.id).await?;
    if let Some(data) = read_result {
        println!("    👁️  Current balance: ${}", data["balance"]);
    }

    // Update within transaction
    let mut updated_account = account_doc.data.clone();
    updated_account["balance"] = json!(5500.0);
    updated_account["last_transaction"] = json!("deposit_500");

    let _ = tx_storage.transactional_update(&tx_id, &account_doc.metadata.id, updated_account).await?;
    println!("    ✏️  Updated balance: $5500");

    // Commit transaction
    tx_storage.commit_transaction(&tx_id).await?;
    println!("    ✅ Transaction committed successfully");

    // ================================
    // 5. TRANSACTION ROLLBACK DEMO
    // ================================

    println!("\n🔄 5. Transaction Rollback Demo");
    println!("===============================");

    // Start new transaction for rollback demo
    let rollback_tx = tx_storage.begin_transaction(
        IsolationLevel::ReadCommitted,
        Some(Duration::from_secs(10))
    ).await?;

    println!("  🔄 Started rollback demo transaction");

    // Create temporary data
    let temp_data = json!({
        "temp_field": "This should be rolled back",
        "amount": 999
    });

    let temp_doc = tx_storage.transactional_create(&rollback_tx, temp_data).await?;
    println!("    ➕ Created temporary data");

    // Rollback transaction
    tx_storage.rollback_transaction(&rollback_tx).await?;
    println!("    🔄 Transaction rolled back");

    // Verify rollback worked
    let verify_tx = tx_storage.begin_transaction(IsolationLevel::ReadCommitted, None).await?;
    let verify_result = tx_storage.transactional_read(&verify_tx, &temp_doc.metadata.id).await?;

    println!("    ✅ Rollback verification: data is {}",
             if verify_result.is_none() { "GONE" } else { "STILL THERE" });

    tx_storage.commit_transaction(&verify_tx).await?;

    // ================================
    // 6. PERFORMANCE STATISTICS
    // ================================

    println!("\n📊 6. Performance Statistics");
    println!("=============================");

    // Get storage statistics
    let storage_ref = Arc::try_unwrap(db_arc).expect("Failed to unwrap Arc");
    let stats = storage_ref.stats().await?;

    println!("  📈 Storage Statistics:");
    println!("    • Total documents: {}", stats.total_documents);
    println!("    • Memory usage: ~{:.2} KB", stats.estimated_memory_usage as f64 / 1024.0);
    println!("    • Average version: {:.1}", stats.average_version);

    // Get transaction statistics
    let tx_stats = tx_storage.transaction_manager().get_statistics()?;
    println!("  🏦 Transaction Statistics:");
    println!("    • Active transactions: {}", tx_stats.active_transactions);

    // ================================
    // 7. CLEANUP
    // ================================

    println!("\n🧹 7. Cleanup");
    println!("=============");

    // Cleanup any remaining transactions
    let cleaned = tx_storage.transaction_manager().cleanup_timed_out_transactions().await?;
    println!("  ✅ Cleaned up {} timed-out transactions", cleaned);

    println!("\n🎉 Example completed successfully!");
    println!("===================================");
    println!("You've seen:");
    println!("  ✅ Basic CRUD operations");
    println!("  ✅ Index creation and management");
    println!("  ✅ Advanced query operations");
    println!("  ✅ ACID transaction support");
    println!("  ✅ Transaction rollback");
    println!("  ✅ Performance monitoring");

    println!("\n💡 Next Steps:");
    println!("  • Check out the full demo with: cargo run");
    println!("  • Read the documentation for advanced features");
    println!("  • Experiment with different query patterns");
    println!("  • Try concurrent transaction scenarios");

    Ok(())
}
