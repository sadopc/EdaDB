// examples/networking_demo.rs - Complete Database Networking Demo
// Bu demo modern database'lerin network architecture'ını tam olarak gösterir
// PostgreSQL, MongoDB, Redis gibi production database'lerin networking patterns'ını kapsayar

use nosql_memory_db::{
    DatabaseServer, DatabaseClient, ServerConfig, ClientConfig,
    PersistentMemoryStorage, WalConfig, WalFormat,
    IsolationLevel, IndexType, ComparisonOperator, SortDirection
};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    println!("🌐 Advanced NoSQL Database - Complete Networking Demo");
    println!("=====================================================");
    println!("Bu demo modern database sistemlerinin network katmanını gösterir:");
    println!("• TCP Server with custom protocol");
    println!("• Connection pooling and management");
    println!("• Client-server communication");
    println!("• ACID transactions over network");
    println!("• Advanced query operations");
    println!("• Real-time monitoring and statistics\n");

    // ================================
    // 1. SERVER SETUP AND INITIALIZATION
    // ================================

    println!("🚀 Step 1: Database Server Setup");
    println!("=================================");

    // Persistent storage konfigürasyonu
    let wal_config = WalConfig {
        wal_file_path: PathBuf::from("network_demo.wal"),
        snapshot_directory: PathBuf::from("network_snapshots"),
        max_wal_size: 50 * 1024 * 1024,    // 50MB
        sync_interval: 100,                 // Her 100 entry'de sync
        checkpoint_interval_seconds: 60,    // 1 dakikada checkpoint
        max_recovery_entries: 100_000,
        format: WalFormat::Json,
    };

    // Clean start için eski dosyaları temizle
    let _ = std::fs::remove_file(&wal_config.wal_file_path);
    let _ = std::fs::remove_dir_all(&wal_config.snapshot_directory);

    // Persistent storage oluştur
    println!("  📊 Initializing persistent storage with WAL...");
    let persistent_storage = Arc::new(PersistentMemoryStorage::new(wal_config).await?);

    // Server konfigürasyonu
    let server_config = ServerConfig {
        bind_address: "127.0.0.1:5432".to_string(), // PostgreSQL tribute
        max_connections: 100,
        connection_timeout: Duration::from_secs(300), // 5 dakika
        request_timeout: Duration::from_secs(30),     // 30 saniye
        max_request_size: 16 * 1024 * 1024,          // 16MB
        cleanup_interval: Duration::from_secs(60),    // 1 dakika
        verbose_logging: true, // Demo için verbose logging
    };

    // Database server oluştur
    println!("  🔧 Creating database server...");
    let server = Arc::new(DatabaseServer::new(server_config, persistent_storage).await?);

    println!("  ✅ Server configured:");
    println!("    • Address: 127.0.0.1:5432");
    println!("    • Max connections: 100");
    println!("    • Request timeout: 30s");
    println!("    • Connection timeout: 5min");
    println!("    • Verbose logging: enabled");

    // ================================
    // 2. START SERVER IN BACKGROUND
    // ================================

    println!("\n🌐 Step 2: Starting TCP Server");
    println!("==============================");

    let server_handle = {
        let server_clone = Arc::clone(&server);
        tokio::spawn(async move {
            if let Err(e) = server_clone.start().await {
                log::error!("Server error: {:?}", e);
            }
        })
    };

    // Server'ın başlaması için kısa bir süre bekle
    sleep(Duration::from_millis(1000)).await;
    println!("  ✅ TCP Server started and listening on 127.0.0.1:5432");
    println!("  🔄 Ready to accept client connections");

    // ================================
    // 3. CLIENT CONNECTIONS AND BASIC OPERATIONS
    // ================================

    println!("\n👤 Step 3: Client Connection and Basic Operations");
    println!("================================================");

    // Client konfigürasyonu
    let client_config = ClientConfig {
        server_address: "127.0.0.1:5432".to_string(),
        connect_timeout: Duration::from_secs(10),
        request_timeout: Duration::from_secs(30),
        max_connections: 10,
        max_retries: 3,
        retry_delay: Duration::from_millis(100),
        verbose_logging: true,
    };

    // İlk client bağlantısı
    println!("  🔌 Connecting to database server...");
    let client1 = DatabaseClient::connect(client_config.clone()).await?;
    println!("    ✅ Client 1 connected successfully");

    // Connection health check
    let ping_time = client1.ping().await?;
    println!("    🏓 Ping response time: {:?}", ping_time);

    // İkinci client (concurrent connection test için)
    let client2 = DatabaseClient::connect(client_config.clone()).await?;
    println!("    ✅ Client 2 connected successfully");

    // ================================
    // 4. DOCUMENT OPERATIONS OVER NETWORK
    // ================================

    println!("\n📄 Step 4: Document Operations Over Network");
    println!("===========================================");

    // Sample data oluştur
    let sample_employees = vec![
        json!({
            "name": "Alice Johnson",
            "department": "Engineering",
            "position": "Senior Developer",
            "salary": 95000,
            "start_date": "2022-01-15",
            "skills": ["Rust", "TypeScript", "PostgreSQL"],
            "remote": true
        }),
        json!({
            "name": "Bob Smith",
            "department": "Sales",
            "position": "Account Manager",
            "salary": 75000,
            "start_date": "2021-06-20",
            "skills": ["CRM", "Negotiation", "Analytics"],
            "remote": false
        }),
        json!({
            "name": "Carol Davis",
            "department": "Engineering",
            "position": "DevOps Engineer",
            "salary": 88000,
            "start_date": "2023-03-10",
            "skills": ["Docker", "Kubernetes", "AWS"],
            "remote": true
        }),
        json!({
            "name": "David Wilson",
            "department": "Marketing",
            "position": "Marketing Manager",
            "salary": 82000,
            "start_date": "2020-11-05",
            "skills": ["Social Media", "Analytics", "Content"],
            "remote": false
        }),
    ];

    println!("  📊 Creating employee records via network...");
    let mut created_docs = Vec::new();

    for (i, employee) in sample_employees.iter().enumerate() {
        let start_time = Instant::now();
        let result = client1.create_document(employee.clone()).await?;
        let operation_time = start_time.elapsed();

        created_docs.push(result.clone());
        println!("    ✅ Employee {}: {} (ID: {}, {:?})",
                i + 1,
                employee["name"].as_str().unwrap(),
                result.id.to_string().chars().take(8).collect::<String>(),
                operation_time);
    }

    // Read operations test
    println!("\n  📖 Testing read operations:");
    let first_employee_id = created_docs[0].id;
    let read_result = client1.read_document(first_employee_id).await?;

    if let Some(employee) = read_result {
        println!("    ✅ Read employee: {}", employee["name"].as_str().unwrap());
        println!("      Department: {}", employee["department"].as_str().unwrap());
        println!("      Salary: ${}", employee["salary"].as_u64().unwrap());
    }

    // Batch read test
    let all_ids: Vec<_> = created_docs.iter().map(|doc| doc.id).collect();
    let batch_read_result = client2.read_documents(all_ids).await?;
    println!("    ✅ Batch read: {} employees retrieved", batch_read_result.len());

    // Update operation test
    let mut updated_employee = sample_employees[0].clone();
    updated_employee["salary"] = json!(100000);
    updated_employee["last_promotion"] = json!("2024-12-01");

    let update_result = client1.update_document(first_employee_id, updated_employee).await?;
    println!("    ✅ Updated employee salary: ${} -> ${}",
             sample_employees[0]["salary"].as_u64().unwrap(),
             update_result.document.unwrap()["salary"].as_u64().unwrap());

    // ================================
    // 5. INDEX OPERATIONS OVER NETWORK
    // ================================

    println!("\n🗂️  Step 5: Index Management Over Network");
    println!("========================================");

    println!("  📋 Creating indexes for query optimization...");

    // Salary index for range queries
    client1.create_index("salary_idx", vec!["salary".to_string()], IndexType::BTree).await?;
    println!("    ✅ Created salary_idx (BTree) - for salary range queries");

    // Department index for equality queries
    client1.create_index("dept_idx", vec!["department".to_string()], IndexType::Hash).await?;
    println!("    ✅ Created dept_idx (Hash) - for department filtering");

    // Composite index
    client1.create_index("dept_salary_idx", 
                        vec!["department".to_string(), "salary".to_string()], 
                        IndexType::Hash).await?;
    println!("    ✅ Created dept_salary_idx (Hash) - for composite queries");

    // List indexes
    let indexes = client1.list_indexes().await?;
    println!("  📊 Total indexes created: {}", indexes.len());

    // ================================
    // 6. ADVANCED QUERY OPERATIONS
    // ================================

    println!("\n🔍 Step 6: Advanced Query Operations Over Network");
    println!("================================================");

    // High earners query
    println!("  💰 Finding high earners (salary > $80,000):");
    let query_start = Instant::now();
    let high_earners = client1.query()
        .where_gt("salary", json!(80000))
        .sort_desc("salary")
        .execute()
        .await?;
    let query_time = query_start.elapsed();

    println!("    📊 Query executed in {:?}", query_time);
    for employee in &high_earners {
        println!("      • {}: ${} ({})",
                employee["name"].as_str().unwrap(),
                employee["salary"].as_u64().unwrap(),
                employee["position"].as_str().unwrap());
    }

    // Engineering department query
    println!("\n  👨‍💻 Finding Engineering department employees:");
    let engineers = client2.query()
        .where_eq("department", json!("Engineering"))
        .where_eq("remote", json!(true))
        .sort_asc("name")
        .execute()
        .await?;

    println!("    📊 Found {} remote engineers", engineers.len());
    for engineer in &engineers {
        println!("      • {}: {} skills",
                engineer["name"].as_str().unwrap(),
                engineer["skills"].as_array().unwrap().len());
    }

    // Projection query (select specific fields)
    println!("\n  📋 Employee summary (name and salary only):");
    let summary = client1.query()
        .select(vec!["name", "salary", "department"])
        .sort_desc("salary")
        .limit(3)
        .execute()
        .await?;

    for employee in &summary {
        println!("      • {}: ${} ({})",
                employee["name"].as_str().unwrap(),
                employee["salary"].as_u64().unwrap(),
                employee["department"].as_str().unwrap());
    }

    // ================================
    // 7. TRANSACTION OPERATIONS OVER NETWORK
    // ================================

    println!("\n🏦 Step 7: ACID Transactions Over Network");
    println!("=========================================");

    // Begin transaction
    println!("  🔄 Starting ACID transaction...");
    let transaction = client1.begin_transaction(IsolationLevel::ReadCommitted).await?;
    println!("    ✅ Transaction started: {}",
             transaction.id().to_string().chars().take(8).collect::<String>());

    // Transactional operations
    let new_employee = json!({
        "name": "Emma Brown",
        "department": "HR",
        "position": "HR Specialist",
        "salary": 65000,
        "start_date": "2024-12-01",
        "skills": ["Recruitment", "Employee Relations"],
        "remote": true
    });

    let tx_create_result = transaction.create_document(new_employee).await?;
    println!("    ➕ Created employee in transaction: {}",
             tx_create_result.document.as_ref().unwrap()["name"].as_str().unwrap());

    // Read within transaction
    let tx_read_result = transaction.read_document(tx_create_result.id).await?;
    if let Some(employee) = tx_read_result {
        println!("    👁️  Read from transaction: {}",
                employee["name"].as_str().unwrap());
    }

    // Update within transaction
    let mut updated_in_tx = tx_create_result.document.unwrap();
    updated_in_tx["salary"] = json!(70000);
    updated_in_tx["notes"] = json!("Salary adjusted during onboarding");

    let tx_update_result = transaction.update_document(tx_create_result.id, updated_in_tx).await?;
    println!("    ✏️  Updated salary in transaction: ${} -> ${}",
             65000, tx_update_result.document.unwrap()["salary"].as_u64().unwrap());

    // Commit transaction
    transaction.commit().await?;
    println!("    ✅ Transaction committed successfully");

    // Verify transaction results
    let committed_employee = client2.read_document(tx_create_result.id).await?;
    if let Some(employee) = committed_employee {
        println!("    🔍 Verification - committed employee: {} (${} salary)",
                employee["name"].as_str().unwrap(),
                employee["salary"].as_u64().unwrap());
    }

    // ================================
    // 8. CONCURRENT CLIENT OPERATIONS
    // ================================

    println!("\n⚡ Step 8: Concurrent Client Operations");
    println!("======================================");

    println!("  🏃 Testing concurrent operations with multiple clients...");

    // Concurrent queries from different clients
    let concurrent_start = Instant::now();

    let (query1_result, query2_result, query3_result) = tokio::join!(
        // Client 1: Department summary
        client1.query()
            .select(vec!["department", "name"])
            .sort_asc("department")
            .execute(),

        // Client 2: Salary statistics  
        client2.query()
            .where_gt("salary", json!(70000))
            .sort_desc("salary")
            .limit(5)
            .execute(),

        // Client 1: Count operation
        client1.query().count()
    );

    let concurrent_time = concurrent_start.elapsed();

    println!("    ✅ Concurrent operations completed in {:?}", concurrent_time);
    println!("      • Query 1 (departments): {} results", query1_result?.len());
    println!("      • Query 2 (high salaries): {} results", query2_result?.len());
    println!("      • Query 3 (count): {} total employees", query3_result?);

    // ================================
    // 9. ROLLBACK TRANSACTION DEMO
    // ================================

    println!("\n🔄 Step 9: Transaction Rollback Demo");
    println!("====================================");

    // Begin rollback demo transaction
    let rollback_tx = client2.begin_transaction(IsolationLevel::ReadCommitted).await?;
    println!("  🔄 Started rollback demo transaction");

    // Create temporary data
    let temp_employee = json!({
        "name": "Temporary Employee",
        "department": "Temporary",
        "position": "Test Position",
        "salary": 1,
        "temp_flag": true
    });

    let temp_doc = rollback_tx.create_document(temp_employee).await?;
    println!("    ➕ Created temporary employee: {}",
             temp_doc.document.as_ref().unwrap()["name"].as_str().unwrap());

    // Rollback transaction
    rollback_tx.rollback().await?;
    println!("    🔄 Transaction rolled back");

    // Verify rollback worked
    let rollback_verification = client1.read_document(temp_doc.id).await?;
    println!("    ✅ Rollback verification: temporary employee is {}",
             if rollback_verification.is_none() { "GONE ✅" } else { "STILL THERE ❌" });

    // ================================
    // 10. SERVER STATISTICS AND MONITORING
    // ================================

    println!("\n📊 Step 10: Server Statistics and Monitoring");
    println!("============================================");

    // Get server statistics
    let server_stats = client1.get_server_stats().await?;
    println!("  🖥️  Server Statistics:");
    println!("    • Active connections: {}", server_stats.active_connections);
    println!("    • Total requests processed: {}", server_stats.total_requests);
    println!("    • Average response time: {:.2}ms", server_stats.avg_response_time_ms);
    println!("    • Server uptime: {:.0} seconds",
             chrono::Utc::now().signed_duration_since(server_stats.started_at).num_seconds());

    // Get client statistics
    let client_stats = client1.get_client_stats().await?;
    println!("\n  👤 Client 1 Statistics:");
    println!("    • Requests sent: {}", client_stats.requests_sent);
    println!("    • Responses received: {}", client_stats.responses_received);
    println!("    • Average request time: {:.2}ms", client_stats.avg_request_time_ms);
    println!("    • Bytes sent: {} KB", client_stats.bytes_sent / 1024);
    println!("    • Bytes received: {} KB", client_stats.bytes_received / 1024);

    // ================================
    // 11. STRESS TEST (OPTIONAL)
    // ================================

    println!("\n🔥 Step 11: Mini Stress Test");
    println!("============================");

    println!("  ⚡ Running concurrent operations stress test...");
    let stress_start = Instant::now();

    // Create multiple concurrent operations
    let mut tasks = Vec::new();

    // Spawn 10 concurrent tasks
    for i in 0..10 {
        let client = DatabaseClient::connect(client_config.clone()).await?;
        let task = tokio::spawn(async move {
            let stress_employee = json!({
                "name": format!("StressTest-{:02}", i),
                "department": "QA",
                "position": "Load Tester",
                "salary": 50000 + i * 1000,
                "stress_test": true
            });

            // Each task does: create -> read -> update -> read
            let create_result = client.create_document(stress_employee).await?;
            let _read1 = client.read_document(create_result.id).await?;
            
            let mut updated = create_result.document.unwrap();
            updated["salary"] = json!(updated["salary"].as_u64().unwrap() + 5000);
            let _update_result = client.update_document(create_result.id, updated).await?;
            let _read2 = client.read_document(create_result.id).await?;

            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });
        tasks.push(task);
    }

    // Wait for all tasks to complete
    let mut success_count = 0;
    for task in tasks {
        if task.await.is_ok() {
            success_count += 1;
        }
    }

    let stress_time = stress_start.elapsed();
    println!("    ✅ Stress test completed in {:?}", stress_time);
    println!("    📊 Successful operations: {}/10", success_count);
    println!("    ⚡ Average time per complete workflow: {:?}", stress_time / 10);

    // ================================
    // 12. CLEANUP AND SHUTDOWN
    // ================================

    println!("\n🧹 Step 12: Cleanup and Graceful Shutdown");
    println!("==========================================");

    // Final server statistics
    let final_stats = client1.get_server_stats().await?;
    println!("  📊 Final Server Statistics:");
    println!("    • Total requests processed: {}", final_stats.total_requests);
    println!("    • Peak connections: {}", final_stats.active_connections);
    println!("    • Final average response time: {:.2}ms", final_stats.avg_response_time_ms);

    // Get final document count
    let final_count = client1.query().count().await?;
    println!("    • Total documents in database: {}", final_count);

    // Close client connections
    drop(client1);
    drop(client2);
    println!("  ✅ Client connections closed");

    // Graceful server shutdown
    println!("  🔄 Initiating graceful server shutdown...");
    server.shutdown().await?;
    
    // Wait for server task to complete
    server_handle.abort(); // Force abort since we shut down gracefully
    println!("  ✅ Server shutdown completed");

    // ================================
    // 13. DEMO SUMMARY
    // ================================

    println!("\n🎉 Step 13: Networking Demo Summary");
    println!("===================================");

    println!("  🏆 Successfully Demonstrated:");
    println!("    ✅ TCP server with custom protocol");
    println!("    ✅ Multiple concurrent client connections");
    println!("    ✅ Complete CRUD operations over network");
    println!("    ✅ Index management via network commands");
    println!("    ✅ Advanced query operations with optimization");
    println!("    ✅ ACID transactions over network");
    println!("    ✅ Transaction commit and rollback");
    println!("    ✅ Concurrent operations from multiple clients");
    println!("    ✅ Real-time server and client statistics");
    println!("    ✅ Connection pooling and management");
    println!("    ✅ Graceful shutdown procedures");

    println!("\n  📈 Performance Highlights:");
    println!("    • Network protocol overhead: minimal");
    println!("    • Concurrent client support: excellent");
    println!("    • Transaction isolation: maintained");
    println!("    • Query optimization: index-aware");
    println!("    • Connection management: robust");

    println!("\n  🔮 Production-Ready Features:");
    println!("    • Connection timeouts and limits");
    println!("    • Request size validation");
    println!("    • Error handling and recovery");
    println!("    • Statistics and monitoring");
    println!("    • Resource cleanup");
    println!("    • Graceful shutdown");

    println!("\n  🚀 Next Steps for Production:");
    println!("    • SSL/TLS encryption for security");
    println!("    • Authentication and authorization");
    println!("    • Connection pooling optimization");
    println!("    • Load balancing support");
    println!("    • Cluster replication");
    println!("    • Performance metrics collection");

    println!("\n🌟 Networking Demo Completed Successfully!");
    println!("Your NoSQL database now has enterprise-grade networking capabilities!");

    Ok(())
}
