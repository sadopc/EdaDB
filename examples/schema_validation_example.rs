use nosql_memory_db::*;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 NoSQL Database Schema Validation System Demo");
    println!("=".repeat(60));

    // Create schema registry
    let schema_registry = Arc::new(SchemaRegistry::new());

    println!("\n📝 Creating User Schema");
    println!("-".repeat(30));

    // Create a comprehensive user schema
    let user_schema = SchemaBuilder::new("user_schema_v1")
        .version("1.0.0")
        .title("User Management Schema")
        .description("Comprehensive schema for user documents with validation")
        .field("name", FieldSchemaBuilder::string()
            .required()
            .min_length(2)
            .max_length(50)
            .description("User's full name")
            .example(json!("John Doe"))
            .build())
        .field("email", FieldSchemaBuilder::string()
            .required()
            .email()
            .description("User's email address")
            .example(json!("john@example.com"))
            .build())
        .field("age", FieldSchemaBuilder::integer()
            .required()
            .minimum(0.0)
            .maximum(150.0)
            .description("User's age in years")
            .example(json!(30))
            .build())
        .field("phone", FieldSchemaBuilder::string()
            .optional()
            .phone()
            .description("User's phone number")
            .example(json!("+1-234-567-8900"))
            .build())
        .field("website", FieldSchemaBuilder::string()
            .optional()
            .url()
            .description("User's personal website")
            .example(json!("https://johndoe.com"))
            .build())
        .field("preferences", FieldSchemaBuilder::object()
            .optional()
            .description("User preferences")
            .build())
        .field("tags", FieldSchemaBuilder::array()
            .optional()
            .min_items(0)
            .max_items(10)
            .unique_items()
            .items_schema(FieldSchemaBuilder::string().min_length(1).build())
            .description("User tags")
            .build())
        .additional_properties(false)
        .build();

    // Register schema
    schema_registry.register_schema("users", user_schema, None)?;
    println!("✅ User schema registered successfully");

    println!("\n🔍 Testing Valid Documents");
    println!("-".repeat(30));

    // Test valid documents
    let valid_users = vec![
        json!({
            "name": "John Doe",
            "email": "john@example.com", 
            "age": 30,
            "phone": "+1-234-567-8900",
            "website": "https://johndoe.com",
            "tags": ["developer", "rust", "database"]
        }),
        json!({
            "name": "Jane Smith",
            "email": "jane@company.com",
            "age": 28,
            "preferences": {
                "theme": "dark",
                "notifications": true
            }
        }),
        json!({
            "name": "Bob Wilson",
            "email": "bob@email.com",
            "age": 35
        })
    ];

    for (i, user) in valid_users.iter().enumerate() {
        match schema_registry.validate_document("users", user, Some(format!("user_{}", i + 1))) {
            Ok(()) => println!("✅ User {} validation passed", i + 1),
            Err(e) => println!("❌ User {} validation failed: {}", i + 1, e),
        }
    }

    println!("\n❌ Testing Invalid Documents");
    println!("-".repeat(30));

    // Test invalid documents
    let invalid_users = vec![
        (
            "Missing required field (email)",
            json!({
                "name": "Invalid User",
                "age": 25
            })
        ),
        (
            "Invalid email format",
            json!({
                "name": "Invalid User",
                "email": "not-an-email",
                "age": 25
            })
        ),
        (
            "Age out of range",
            json!({
                "name": "Invalid User", 
                "email": "user@example.com",
                "age": 200
            })
        ),
        (
            "Name too short",
            json!({
                "name": "X",
                "email": "user@example.com",
                "age": 25
            })
        ),
        (
            "Invalid phone format",
            json!({
                "name": "Invalid User",
                "email": "user@example.com", 
                "age": 25,
                "phone": "invalid-phone"
            })
        ),
        (
            "Additional property not allowed",
            json!({
                "name": "Invalid User",
                "email": "user@example.com",
                "age": 25,
                "unexpected_field": "value"
            })
        )
    ];

    for (desc, user) in invalid_users.iter() {
        match schema_registry.validate_document("users", user, None) {
            Ok(()) => println!("⚠️  {} - unexpectedly passed", desc),
            Err(e) => {
                println!("✅ {} - correctly failed:", desc);
                if let DatabaseError::SchemaValidationError { validation_errors, .. } = &e {
                    for validation_error in validation_errors {
                        println!("   → {}", validation_error.user_friendly_message());
                    }
                } else {
                    println!("   → {}", e);
                }
            }
        }
    }

    println!("\n🏢 Creating Product Schema for E-commerce");
    println!("-".repeat(40));

    // Create product schema for e-commerce example
    let product_schema = SchemaBuilder::new("product_schema_v1")
        .version("1.0.0")
        .title("Product Catalog Schema")
        .description("Schema for e-commerce product documents")
        .field("name", FieldSchemaBuilder::string()
            .required()
            .min_length(1)
            .max_length(200)
            .description("Product name")
            .build())
        .field("sku", FieldSchemaBuilder::string()
            .required()
            .min_length(3)
            .max_length(50)
            .description("Stock Keeping Unit")
            .build())
        .field("price", FieldSchemaBuilder::number()
            .required()
            .minimum(0.0)
            .description("Product price")
            .build())
        .field("currency", FieldSchemaBuilder::string()
            .required()
            .min_length(3)
            .max_length(3)
            .description("Currency code (ISO 4217)")
            .example(json!("USD"))
            .build())
        .field("category", FieldSchemaBuilder::string()
            .required()
            .min_length(1)
            .description("Product category")
            .build())
        .field("description", FieldSchemaBuilder::string()
            .optional()
            .max_length(2000)
            .description("Product description")
            .build())
        .field("inventory", FieldSchemaBuilder::object()
            .required()
            .property("quantity", FieldSchemaBuilder::integer().required().minimum(0.0).build())
            .property("warehouse", FieldSchemaBuilder::string().required().build())
            .build())
        .field("images", FieldSchemaBuilder::array()
            .optional()
            .min_items(0)
            .max_items(10)
            .items_schema(FieldSchemaBuilder::string().url().build())
            .description("Product image URLs")
            .build())
        .field("ratings", FieldSchemaBuilder::object()
            .optional()
            .property("average", FieldSchemaBuilder::number().minimum(0.0).maximum(5.0).build())
            .property("count", FieldSchemaBuilder::integer().minimum(0.0).build())
            .build())
        .additional_properties(false)
        .build();

    schema_registry.register_schema("products", product_schema, None)?;
    println!("✅ Product schema registered successfully");

    // Test product validation
    let valid_product = json!({
        "name": "Wireless Bluetooth Headphones",
        "sku": "WBH-001",
        "price": 99.99,
        "currency": "USD",
        "category": "Electronics",
        "description": "High-quality wireless headphones with noise cancellation",
        "inventory": {
            "quantity": 50,
            "warehouse": "US-WEST-01"
        },
        "images": [
            "https://example.com/images/headphones1.jpg",
            "https://example.com/images/headphones2.jpg"
        ],
        "ratings": {
            "average": 4.5,
            "count": 127
        }
    });

    match schema_registry.validate_document("products", &valid_product, Some("product_001".to_string())) {
        Ok(()) => println!("✅ Product validation passed"),
        Err(e) => println!("❌ Product validation failed: {}", e),
    }

    println!("\n💳 Creating Financial Transaction Schema");
    println!("-".repeat(40));

    // Create financial transaction schema
    let transaction_schema = SchemaBuilder::new("transaction_schema_v1")
        .version("1.0.0")
        .title("Financial Transaction Schema")
        .description("Schema for financial transaction validation")
        .field("transaction_id", FieldSchemaBuilder::string()
            .required()
            .uuid()
            .description("Unique transaction identifier")
            .build())
        .field("amount", FieldSchemaBuilder::number()
            .required()
            .minimum(0.01)
            .maximum(1000000.0)
            .description("Transaction amount")
            .build())
        .field("currency", FieldSchemaBuilder::string()
            .required()
            .min_length(3)
            .max_length(3)
            .description("Currency code")
            .build())
        .field("from_account", FieldSchemaBuilder::string()
            .required()
            .min_length(8)
            .max_length(20)
            .description("Source account number")
            .build())
        .field("to_account", FieldSchemaBuilder::string()
            .required()
            .min_length(8)
            .max_length(20)
            .description("Destination account number")
            .build())
        .field("description", FieldSchemaBuilder::string()
            .optional()
            .max_length(500)
            .description("Transaction description")
            .build())
        .field("metadata", FieldSchemaBuilder::object()
            .optional()
            .property("ip_address", FieldSchemaBuilder::string().ipv4().build())
            .property("user_agent", FieldSchemaBuilder::string().max_length(500).build())
            .build())
        .additional_properties(false)
        .build();

    // Configure strict validation for financial transactions
    let strict_config = SchemaConfig::new()
        .enabled(true)
        .strict_mode(true)
        .fail_fast(true)
        .validate_formats(true)
        .allow_type_coercion(false);

    schema_registry.register_schema("transactions", transaction_schema, Some(strict_config))?;
    println!("✅ Financial transaction schema registered with strict validation");

    // Test transaction validation
    let valid_transaction = json!({
        "transaction_id": "550e8400-e29b-41d4-a716-446655440000",
        "amount": 1250.50,
        "currency": "USD",
        "from_account": "1234567890",
        "to_account": "0987654321",
        "description": "Payment for services",
        "metadata": {
            "ip_address": "192.168.1.100",
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
    });

    match schema_registry.validate_document("transactions", &valid_transaction, None) {
        Ok(()) => println!("✅ Transaction validation passed"),
        Err(e) => println!("❌ Transaction validation failed: {}", e),
    }

    println!("\n📊 Schema Registry Statistics");
    println!("-".repeat(30));

    let stats = schema_registry.get_stats()?;
    println!("Total collections: {}", stats.total_collections);
    println!("Enabled collections: {}", stats.enabled_collections);
    println!("Disabled collections: {}", stats.disabled_collections);

    if let Some(oldest) = stats.oldest_schema_date {
        println!("Oldest schema: {}", oldest.format("%Y-%m-%d %H:%M:%S UTC"));
    }

    if let Some(newest) = stats.newest_schema_date {
        println!("Newest schema: {}", newest.format("%Y-%m-%d %H:%M:%S UTC"));
    }

    println!("\n🎯 Testing Schema Validation with Memory Storage");
    println!("-".repeat(45));

    // Create memory storage with collection name and shared registry
    let user_storage: MemoryStorage<Value> = MemoryStorage::with_collection_and_registry(
        "users",
        Arc::clone(&schema_registry)
    );

    // Try to create valid document
    let valid_user_data = json!({
        "name": "Alice Johnson",
        "email": "alice@example.com",
        "age": 32,
        "tags": ["manager", "team-lead"]
    });

    match user_storage.create(valid_user_data).await {
        Ok(document) => {
            println!("✅ Document created successfully:");
            println!("   ID: {}", document.metadata.id);
            println!("   Name: {}", document.data["name"]);
            println!("   Email: {}", document.data["email"]);
        }
        Err(e) => println!("❌ Document creation failed: {}", e),
    }

    // Try to create invalid document
    let invalid_user_data = json!({
        "name": "X", // Too short
        "email": "invalid-email", // Invalid format
        "age": 200 // Out of range
    });

    match user_storage.create(invalid_user_data).await {
        Ok(_) => println!("⚠️ Invalid document was unexpectedly created"),
        Err(e) => {
            println!("✅ Invalid document correctly rejected:");
            println!("   Error: {}", e);
        }
    }

    println!("\n🔧 Testing Schema Configuration Changes");
    println!("-".repeat(40));

    // Disable validation for users collection
    schema_registry.set_validation_enabled("users", false)?;
    println!("✅ Validation disabled for users collection");

    // Now the invalid document should be accepted
    match user_storage.create(json!({"invalid": "data"})).await {
        Ok(_) => println!("✅ Document created successfully (validation disabled)"),
        Err(e) => println!("❌ Document creation failed: {}", e),
    }

    // Re-enable validation
    schema_registry.set_validation_enabled("users", true)?;
    println!("✅ Validation re-enabled for users collection");

    println!("\n🎉 Schema Validation Demo Completed Successfully!");
    println!("=".repeat(60));

    Ok(())
} 