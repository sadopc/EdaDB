// examples/sql_example.rs - Comprehensive SQL-like Query Language Demo with Performance Testing


use nosql_memory_db::*;
use serde_json::json;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use rand::Rng;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 NoSQL Database with SQL-like Query Language - COMPREHENSIVE DEMO");
    println!("{}", "=".repeat(80));
    
    // Initialize storage and query engine
    let storage = Arc::new(MemoryStorage::new());
    let mut query_engine = QueryEngine::new(storage);

    // Run all demo sections
    demo_basic_operations(&mut query_engine).await?;
    demo_performance_tests(&mut query_engine).await?;
    demo_stress_tests(&mut query_engine).await?;
    demo_complex_queries(&mut query_engine).await?;
    demo_error_handling(&mut query_engine).await?;

    println!("\n🎉 Comprehensive demo completed successfully!");
    println!("{}", "=".repeat(80));
    
    Ok(())
}

async fn demo_basic_operations(query_engine: &mut QueryEngine<serde_json::Value>) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n📚 SECTION 1: Basic Operations Demo");
    println!("{}", "-".repeat(50));

    // Create collections
    println!("\n📁 Creating collections...");
    let collections = vec!["users", "orders", "products", "reviews"];
    
    for collection in &collections {
        let start = Instant::now();
        let query = parse(&format!("CREATE COLLECTION {}", collection))?;
        let result = query_engine.execute(query).await?;
        let duration = start.elapsed();
        println!("✅ Created '{}' collection - Time: {:?}", collection, duration);
    }

    // Insert sample data
    println!("\n📝 Inserting sample data...");
    
    let users_data = vec![
        (r#"INSERT INTO users (name, age, city, email, salary, department) VALUES ("John Doe", 30, "Berlin", "john@example.com", 65000, "Engineering")"#, "John Doe"),
        (r#"INSERT INTO users (name, age, city, email, salary, department) VALUES ("Jane Smith", 25, "Munich", "jane@example.com", 58000, "Marketing")"#, "Jane Smith"),
        (r#"INSERT INTO users (name, age, city, email, salary, department) VALUES ("Bob Johnson", 35, "Berlin", "bob@example.com", 72000, "Engineering")"#, "Bob Johnson"),
        (r#"INSERT INTO users (name, age, city, email, salary, department) VALUES ("Alice Brown", 28, "Hamburg", "alice@example.com", 61000, "Sales")"#, "Alice Brown"),
        (r#"INSERT INTO users (name, age, city, email, salary, department) VALUES ("Charlie Wilson", 32, "Munich", "charlie@example.com", 68000, "Engineering")"#, "Charlie Wilson"),
        (r#"INSERT INTO users (name, age, city, email, salary, department) VALUES ("Emma Davis", 27, "Berlin", "emma@example.com", 55000, "HR")"#, "Emma Davis"),
        (r#"INSERT INTO users (name, age, city, email, salary, department) VALUES ("David Miller", 33, "Frankfurt", "david@example.com", 71000, "Finance")"#, "David Miller"),
        (r#"INSERT INTO users (name, age, city, email, salary, department) VALUES ("Sarah Wilson", 29, "Munich", "sarah@example.com", 59000, "Marketing")"#, "Sarah Wilson"),
    ];

    let mut total_insert_time = Duration::new(0, 0);
    for (sql, name) in users_data {
        let start = Instant::now();
        let query = parse(sql)?;
        let result = query_engine.execute(query).await?;
        let duration = start.elapsed();
        total_insert_time += duration;
        println!("✅ Inserted user '{}' - Time: {:?}", name, duration);
    }
    println!("📊 Total insert time: {:?}", total_insert_time);

    // Demo various SELECT queries with timing
    println!("\n🔍 Running SELECT queries with timing...");

    let queries = vec![
        ("SELECT * FROM users", "All users"),
        ("SELECT name, age FROM users", "Names and ages"),
        ("SELECT * FROM users WHERE age > 30", "Users older than 30"),
        (r#"SELECT name, salary FROM users WHERE department = "Engineering""#, "Engineering department"),
        ("SELECT name, age FROM users ORDER BY age DESC", "Users by age (desc)"),
        ("SELECT * FROM users ORDER BY salary ASC LIMIT 3", "Top 3 lowest salaries"),
        (r#"SELECT name AS full_name, salary AS annual_income FROM users WHERE city = "Berlin""#, "Berlin users (aliased)"),
    ];

    for (sql, description) in queries {
        let start = Instant::now();
        let query = parse(sql)?;
        let result = query_engine.execute(query).await?;
        let duration = start.elapsed();
        
        println!("\n--- {} ---", description);
        println!("Query: {}", sql);
        println!("⏱️  Execution time: {:?}", duration);
        println!("📄 Rows returned: {}", result.rows.len());
        
        if result.rows.len() <= 5 {
            for (i, row) in result.rows.iter().enumerate() {
                println!("  {}. {}", i + 1, format_row_compact(row));
            }
        } else {
            println!("  (Showing first 3 rows)");
            for (i, row) in result.rows.iter().take(3).enumerate() {
                println!("  {}. {}", i + 1, format_row_compact(row));
            }
        }
    }

    Ok(())
}

async fn demo_performance_tests(query_engine: &mut QueryEngine<serde_json::Value>) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n⚡ SECTION 2: Performance Tests with Large Datasets");
    println!("{}", "-".repeat(50));

    // Test with different dataset sizes
    let test_sizes = vec![100, 1000, 10000];
    
    for size in test_sizes {
        println!("\n📊 Testing with {} records", size);
        println!("{}", "-".repeat(30));

        // Create test collection
        let collection_name = format!("test_data_{}", size);
        let start = Instant::now();
        let query = parse(&format!("CREATE COLLECTION {}", collection_name))?;
        query_engine.execute(query).await?;
        println!("✅ Created collection '{}' - Time: {:?}", collection_name, start.elapsed());

        // Bulk insert test
        println!("\n📝 Bulk Insert Performance Test");
        let start = Instant::now();
        let mut total_inserts = 0;
        
        for i in 0..size {
            let age = 20 + (i % 50);
            let salary = 30000 + (i * 100);
            let cities = vec!["Berlin", "Munich", "Hamburg", "Frankfurt", "Stuttgart"];
            let departments = vec!["Engineering", "Marketing", "Sales", "HR", "Finance"];
            let city = cities[i % cities.len()];
            let department = departments[i % departments.len()];
            
            let sql = format!(
                r#"INSERT INTO {} (id, name, age, city, salary, department, score) VALUES ({}, "User{}", {}, "{}", {}, "{}", {})"#,
                collection_name, i, i, age, city, salary, department, (i % 100) + 1
            );
            
            let query = parse(&sql)?;
            let result = query_engine.execute(query).await?;
            total_inserts += result.rows_affected;
            
            if i % (size / 10).max(1) == 0 {
                println!("  Inserted {} / {} records", i + 1, size);
            }
        }
        
        let insert_duration = start.elapsed();
        let inserts_per_second = (total_inserts as f64) / insert_duration.as_secs_f64();
        
        println!("📈 Bulk Insert Results:");
        println!("  Total records: {}", total_inserts);
        println!("  Total time: {:?}", insert_duration);
        println!("  Inserts per second: {:.2}", inserts_per_second);
        println!("  Average time per insert: {:?}", insert_duration / total_inserts as u32);

        // Query Performance Tests
        println!("\n🔎 Query Performance Tests");
        
        let test_queries = vec![
            (format!("SELECT * FROM {}", collection_name), "Full table scan"),
            (format!("SELECT name, age FROM {}", collection_name), "Projection query"),
            (format!("SELECT * FROM {} WHERE age > 40", collection_name), "Filter by age"),
            (format!("SELECT * FROM {} WHERE salary > 50000", collection_name), "Filter by salary"),
            (format!(r#"SELECT * FROM {} WHERE city = "Berlin""#, collection_name), "Filter by city"),
            (format!(r#"SELECT * FROM {} WHERE department = "Engineering" AND age > 30"#, collection_name), "Complex filter"),
            (format!("SELECT * FROM {} ORDER BY salary DESC", collection_name), "Sort by salary"),
            (format!("SELECT * FROM {} ORDER BY age ASC LIMIT 100", collection_name), "Sort + limit"),
            (format!(r#"SELECT name, salary FROM {} WHERE city = "Munich" ORDER BY salary DESC LIMIT 50"#, collection_name), "Complex query"),
        ];

        for (sql, description) in test_queries {
            let start = Instant::now();
            let query = parse(&sql)?;
            let result = query_engine.execute(query).await?;
            let duration = start.elapsed();
            let rows_per_second = if duration.as_secs_f64() > 0.0 {
                result.rows.len() as f64 / duration.as_secs_f64()
            } else {
                0.0
            };
            
            println!("  📊 {} - Time: {:?}, Rows: {}, Rows/sec: {:.2}", 
                description, duration, result.rows.len(), rows_per_second);
        }

        // Update Performance Test
        println!("\n✏️  Update Performance Test");
        let start = Instant::now();
                 let update_sql = format!(r#"UPDATE {} SET salary = 75000 WHERE department = "Engineering""#, collection_name);
        let query = parse(&update_sql)?;
        let result = query_engine.execute(query).await?;
        let update_duration = start.elapsed();
        
        println!("  Updated {} records in {:?}", result.rows_affected, update_duration);
        if result.rows_affected > 0 {
            let updates_per_second = result.rows_affected as f64 / update_duration.as_secs_f64();
            println!("  Updates per second: {:.2}", updates_per_second);
        }

        // Delete Performance Test
        println!("\n🗑️  Delete Performance Test");
        let start = Instant::now();
        let delete_sql = format!("DELETE FROM {} WHERE score < 20", collection_name);
        let query = parse(&delete_sql)?;
        let result = query_engine.execute(query).await?;
        let delete_duration = start.elapsed();
        
        println!("  Deleted {} records in {:?}", result.rows_affected, delete_duration);
        if result.rows_affected > 0 {
            let deletes_per_second = result.rows_affected as f64 / delete_duration.as_secs_f64();
            println!("  Deletes per second: {:.2}", deletes_per_second);
        }

        // Final count
        let count_sql = format!("SELECT * FROM {}", collection_name);
        let query = parse(&count_sql)?;
        let result = query_engine.execute(query).await?;
        println!("  Final record count: {}", result.rows.len());

        println!("\n{}", "=".repeat(40));
    }

    Ok(())
}

async fn demo_stress_tests(query_engine: &mut QueryEngine<serde_json::Value>) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🔥 SECTION 3: Stress Tests");
    println!("{}", "-".repeat(50));

    // Create stress test collection
    let query = parse("CREATE COLLECTION stress_test")?;
    query_engine.execute(query).await?;
    println!("✅ Created stress test collection");

    // Generate large dataset
    println!("\n📊 Generating large dataset (50K records)...");
    let start = Instant::now();
    let mut rng = rand::thread_rng();
    let batch_size = 5000;
    let total_records = 50000;
    
    for batch in 0..(total_records / batch_size) {
        let batch_start = Instant::now();
        
        for i in 0..batch_size {
            let id = batch * batch_size + i;
            let age = rng.gen_range(18..80);
            let salary = rng.gen_range(25000..150000);
            let score = rng.gen_range(1..100);
            let cities = vec!["Berlin", "Munich", "Hamburg", "Frankfurt", "Stuttgart", "Cologne", "Düsseldorf"];
            let departments = vec!["Engineering", "Marketing", "Sales", "HR", "Finance", "Operations", "Support"];
            let city = cities[rng.gen_range(0..cities.len())];
            let department = departments[rng.gen_range(0..departments.len())];
            
            let sql = format!(
                r#"INSERT INTO stress_test (id, name, age, city, salary, department, score, active) VALUES ({}, "StressUser{}", {}, "{}", {}, "{}", {}, true)"#,
                id, id, age, city, salary, department, score
            );
            
            let query = parse(&sql)?;
            query_engine.execute(query).await?;
        }
        
        let batch_duration = batch_start.elapsed();
        println!("  Batch {} completed: {} records in {:?}", batch + 1, batch_size, batch_duration);
    }
    
    let total_duration = start.elapsed();
    let records_per_second = total_records as f64 / total_duration.as_secs_f64();
    println!("📈 Large dataset generation completed:");
    println!("  Total records: {}", total_records);
    println!("  Total time: {:?}", total_duration);
    println!("  Records per second: {:.2}", records_per_second);

    // Stress test queries
    println!("\n🔍 Running stress test queries...");
    
         let stress_queries = vec![
         ("SELECT * FROM stress_test", "Full scan (50K records)"),
         ("SELECT * FROM stress_test WHERE age > 50", "Filter by age"),
        ("SELECT * FROM stress_test WHERE salary > 100000", "High salary filter"),
        (r#"SELECT * FROM stress_test WHERE city = "Berlin" AND department = "Engineering""#, "Complex filter"),
                 ("SELECT * FROM stress_test ORDER BY salary DESC LIMIT 1000", "Top 1000 by salary"),
         (r#"SELECT * FROM stress_test WHERE department = "Engineering" ORDER BY age DESC LIMIT 500"#, "Complex query with conditions"),
    ];

    let mut query_stats = Vec::new();
    
    for (sql, description) in stress_queries {
        println!("\n--- {} ---", description);
        let start = Instant::now();
        let query = parse(sql)?;
        let result = query_engine.execute(query).await?;
        let duration = start.elapsed();
        
        let rows_per_second = if duration.as_secs_f64() > 0.0 {
            result.rows.len() as f64 / duration.as_secs_f64()
        } else {
            0.0
        };
        
        query_stats.push((description, duration, result.rows.len(), rows_per_second));
        
        println!("  ⏱️  Execution time: {:?}", duration);
        println!("  📄 Rows returned: {}", result.rows.len());
        println!("  📈 Rows per second: {:.2}", rows_per_second);
    }

    // Summary of stress test results
    println!("\n📊 Stress Test Summary:");
    println!("{}", "-".repeat(80));
    println!("{:<40} {:>12} {:>10} {:>15}", "Query", "Time (ms)", "Rows", "Rows/sec");
    println!("{}", "-".repeat(80));
    
    for (desc, duration, rows, rows_per_sec) in query_stats {
        println!("{:<40} {:>12} {:>10} {:>15.2}", 
            desc, 
            duration.as_millis(),
            rows,
            rows_per_sec
        );
    }

    // Concurrent operations test
    println!("\n🚀 Concurrent Operations Test");
    let concurrent_start = Instant::now();
    
    // Simulate concurrent reads
    let mut concurrent_results = Vec::new();
    for i in 0..10 {
        let query_start = Instant::now();
        let sql = format!("SELECT * FROM stress_test WHERE id >= {} AND id < {} ORDER BY salary DESC", i * 5000, (i + 1) * 5000);
        let query = parse(&sql)?;
        let result = query_engine.execute(query).await?;
        let duration = query_start.elapsed();
        concurrent_results.push((i, duration, result.rows.len()));
    }
    
    let concurrent_duration = concurrent_start.elapsed();
    println!("  Concurrent read operations completed in {:?}", concurrent_duration);
    for (i, duration, rows) in concurrent_results {
        println!("    Operation {}: {:?} - {} rows", i + 1, duration, rows);
    }

    Ok(())
}

async fn demo_complex_queries(query_engine: &mut QueryEngine<serde_json::Value>) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🎯 SECTION 4: Complex Query Scenarios");
    println!("{}", "-".repeat(50));

    // Complex nested conditions
    println!("\n📋 Complex Nested Conditions");
         let complex_queries = vec![
         (r#"SELECT * FROM users WHERE (age > 30 AND salary > 60000) OR (department = "Engineering" AND city = "Berlin")"#, "Complex OR condition"),
         (r#"SELECT name, salary FROM users WHERE department = "Engineering" AND (city = "Berlin" OR city = "Munich") ORDER BY salary DESC"#, "Multi-city engineering"),
         (r#"SELECT * FROM users WHERE age > 25 AND age < 35 AND salary > 55000 ORDER BY age ASC"#, "Age range with salary filter"),
         (r#"SELECT name, department FROM users WHERE salary > 60000 ORDER BY salary DESC"#, "High earners sorted by salary"),
     ];

    for (sql, description) in complex_queries {
        println!("\n--- {} ---", description);
        let start = Instant::now();
        let query = parse(sql)?;
        let result = query_engine.execute(query).await?;
        let duration = start.elapsed();
        
        println!("  Query: {}", sql);
        println!("  ⏱️  Execution time: {:?}", duration);
        println!("  📄 Rows returned: {}", result.rows.len());
        
        if result.rows.len() <= 3 {
            for (i, row) in result.rows.iter().enumerate() {
                println!("    {}. {}", i + 1, format_row_compact(row));
            }
        }
    }

    // Pagination scenarios
    println!("\n📄 Pagination Performance Tests");
    let page_size = 10;
    let total_pages = 5;
    
    for page in 0..total_pages {
        let offset = page * page_size;
        let sql = format!("SELECT name, age, salary FROM users ORDER BY salary DESC LIMIT {} OFFSET {}", page_size, offset);
        
        let start = Instant::now();
        let query = parse(&sql)?;
        let result = query_engine.execute(query).await?;
        let duration = start.elapsed();
        
        println!("  Page {} (offset {}): {} rows in {:?}", page + 1, offset, result.rows.len(), duration);
    }

    // Join simulation (manual implementation)
    println!("\n🔗 Join Simulation Test");
    
    // Create and populate products table
    let query = parse("CREATE COLLECTION products")?;
    query_engine.execute(query).await?;
    
    let products = vec![
        (r#"INSERT INTO products (id, name, category, price) VALUES (1, "Laptop", "Electronics", 1299.99)"#, "Laptop"),
        (r#"INSERT INTO products (id, name, category, price) VALUES (2, "Mouse", "Electronics", 29.99)"#, "Mouse"),
        (r#"INSERT INTO products (id, name, category, price) VALUES (3, "Keyboard", "Electronics", 79.99)"#, "Keyboard"),
        (r#"INSERT INTO products (id, name, category, price) VALUES (4, "Monitor", "Electronics", 299.99)"#, "Monitor"),
    ];

    for (sql, name) in products {
        let query = parse(sql)?;
        query_engine.execute(query).await?;
        println!("  ✅ Added product: {}", name);
    }

    // Simulate join by running separate queries
    println!("\n  Simulating JOIN operation:");
    let start = Instant::now();
    
    // Get users from Berlin
    let users_query = parse(r#"SELECT name, department FROM users WHERE city = "Berlin""#)?;
    let users_result = query_engine.execute(users_query).await?;
    
    // Get all products
    let products_query = parse("SELECT name, price FROM products")?;
    let products_result = query_engine.execute(products_query).await?;
    
    let join_duration = start.elapsed();
    
    println!("  Users from Berlin: {} records", users_result.rows.len());
    println!("  Products: {} records", products_result.rows.len());
    println!("  Join simulation time: {:?}", join_duration);
    
    // Cross product simulation
    let cross_product_count = users_result.rows.len() * products_result.rows.len();
    println!("  Potential cross product: {} combinations", cross_product_count);

    Ok(())
}

async fn demo_error_handling(query_engine: &mut QueryEngine<serde_json::Value>) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n❌ SECTION 5: Error Handling Tests");
    println!("{}", "-".repeat(50));

    // Parser error tests
    println!("\n🔧 Parser Error Tests");
    let invalid_queries = vec![
        ("INVALID SQL QUERY", "Invalid syntax"),
        ("SELECT * FROM", "Incomplete query"),
        ("SELECT * FROM users WHERE", "Incomplete WHERE clause"),
        ("SELECT * FROM users ORDER BY", "Incomplete ORDER BY"),
        ("INSERT INTO users VALUES", "Incomplete INSERT"),
        ("UPDATE users SET", "Incomplete UPDATE"),
        ("DELETE FROM users WHERE age >", "Incomplete DELETE"),
        ("SELECT * FROM users WHERE age > 30 AND", "Incomplete AND condition"),
        ("SELECT * FROM users LIMIT", "Missing LIMIT value"),
        ("SELECT * FROM users OFFSET 10", "OFFSET without LIMIT"),
    ];

    for (sql, description) in invalid_queries {
        match parse(sql) {
            Ok(_) => println!("  ❌ {} - Should have failed but didn't!", description),
            Err(e) => println!("  ✅ {} - Caught error: {}", description, e),
        }
    }

    // Runtime error tests
    println!("\n🚨 Runtime Error Tests");
    let runtime_errors = vec![
        ("SELECT * FROM completely_nonexistent_collection_xyz", "Non-existent table"),
        ("INSERT INTO never_created_collection_abc (name) VALUES (\"test\")", "Insert to non-existent table"),
        ("UPDATE missing_collection_def SET name = \"test\"", "Update non-existent table"),
        ("DELETE FROM phantom_collection_ghi", "Delete from non-existent table"),
    ];

    for (sql, description) in runtime_errors {
        let start = Instant::now();
        match query_engine.execute(parse(sql)?).await {
            Ok(_) => println!("  ❌ {} - Should have failed but didn't!", description),
            Err(e) => {
                let duration = start.elapsed();
                println!("  ✅ {} - Caught error in {:?}: {}", description, duration, e);
            }
        }
    }

    // Performance with error recovery
    println!("\n⚡ Error Recovery Performance");
    let start = Instant::now();
    let mut successful_queries = 0;
    let mut failed_queries = 0;
    
    let mixed_queries = vec![
        "SELECT * FROM users",
        "SELECT * FROM truly_missing_collection",
        "SELECT name FROM users WHERE age > 25",
        "INVALID QUERY",
        "SELECT * FROM users ORDER BY age DESC",
        "UPDATE another_missing_collection SET field = \"value\"",
        "SELECT * FROM users LIMIT 5",
    ];

    for (i, sql) in mixed_queries.iter().enumerate() {
        match parse(sql) {
            Ok(query) => {
                match query_engine.execute(query).await {
                    Ok(_) => successful_queries += 1,
                    Err(_) => failed_queries += 1,
                }
            }
            Err(_) => failed_queries += 1,
        }
    }
    
    let total_duration = start.elapsed();
    println!("  Processed {} queries in {:?}", mixed_queries.len(), total_duration);
    println!("  Successful: {}, Failed: {}", successful_queries, failed_queries);
    println!("  Average time per query: {:?}", total_duration / mixed_queries.len() as u32);

    Ok(())
}

/// Helper function to format a row compactly
fn format_row_compact(row: &serde_json::Value) -> String {
    if let serde_json::Value::Object(obj) = row {
        let mut parts = Vec::new();
        for (key, value) in obj.iter().take(4) { // Limit to first 4 fields
            let val_str = match value {
                serde_json::Value::String(s) => s.clone(),
                serde_json::Value::Number(n) => n.to_string(),
                serde_json::Value::Bool(b) => b.to_string(),
                _ => value.to_string(),
            };
            parts.push(format!("{}:{}", key, val_str));
        }
        if obj.len() > 4 {
            parts.push("...".to_string());
        }
        format!("{{{}}}", parts.join(", "))
    } else {
        row.to_string()
    }
}

/// Utility function to demonstrate query parsing in detail
fn _demonstrate_parser_internals() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n🔧 Parser Internals Demo");
    
    let input = r#"SELECT name, age FROM users WHERE age > 30 AND city = "Berlin""#;
    println!("Input query: {}", input);
    
    // Show tokenization
    let mut lexer = Lexer::new(input);
    let tokens = lexer.tokenize()?;
    println!("Tokens ({}):", tokens.len());
    for (i, token) in tokens.iter().enumerate() {
        println!("  {}: {:?} at position {}", i, token.token_type, token.position);
    }
    
    // Parse into AST
    let ast = parse(input)?;
    println!("Parsed AST: {:#?}", ast);
    
    Ok(())
}

/// Memory usage estimation (simplified)
fn _estimate_memory_usage(records: usize, avg_record_size: usize) -> String {
    let total_bytes = records * avg_record_size;
    if total_bytes < 1024 {
        format!("{} bytes", total_bytes)
    } else if total_bytes < 1024 * 1024 {
        format!("{:.2} KB", total_bytes as f64 / 1024.0)
    } else {
        format!("{:.2} MB", total_bytes as f64 / (1024.0 * 1024.0))
    }
} 