use serde_json::{json, Value};
use nosql_memory_db::{
    MemoryStorage, CrudDatabase, DatabaseError,
    schema::{
        SchemaBuilder, FieldSchemaBuilder, Format, SchemaRegistry, SchemaConfig
    }
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Rust NoSQL Database - Schema Validation Demo");
    println!("=============================================\n");
    
    demo_basic_usage().await?;
    demo_advanced_features().await?;
    demo_error_handling().await?;
    
    println!("\n✅ Demo completed successfully!");
    Ok(())
}

async fn demo_basic_usage() -> Result<(), Box<dyn std::error::Error>> {
    println!("1️⃣ Basic Schema Validation Demo");
    println!("--------------------------------");
    
    // Create a simple user schema
    let user_schema = SchemaBuilder::new("user_v1")
        .field("name", FieldSchemaBuilder::string()
            .min_length(2)
            .max_length(50)
            .required()
            .build())
        .field("email", FieldSchemaBuilder::string()
            .format(Format::Email)
            .required()
            .build())
        .field("age", FieldSchemaBuilder::number()
            .minimum(0.0)
            .maximum(150.0)
            .required()
            .build())
        .build();

    // Set up storage with schema
    let registry = Arc::new(SchemaRegistry::new());
    registry.register_schema("users", user_schema, None)?;
    
    let storage: MemoryStorage<Value> = MemoryStorage::with_collection_and_registry(
        "users",
        registry.clone()
    );

    // Create valid user
    let user = json!({
        "name": "Alice Johnson",
        "email": "alice@example.com",
        "age": 28
    });

    match storage.create(user).await {
        Ok(doc) => println!("✅ User created: {} (ID: {})", 
                          doc.data["name"], doc.metadata.id),
        Err(e) => println!("❌ Error: {}", e),
    }

    println!();
    Ok(())
}

async fn demo_advanced_features() -> Result<(), Box<dyn std::error::Error>> {
    println!("2️⃣ Advanced Features Demo");
    println!("-------------------------");
    
    // Create complex schema with nested objects and arrays
    let company_schema = SchemaBuilder::new("company_v1")
        .field("name", FieldSchemaBuilder::string()
            .min_length(1)
            .required()
            .build())
        .field("address", FieldSchemaBuilder::object()
            .property("street", FieldSchemaBuilder::string().min_length(5).required().build())
            .property("city", FieldSchemaBuilder::string().min_length(2).required().build())
            .property("country", FieldSchemaBuilder::string().min_length(2).required().build())
            .required()
            .build())
        .field("employees", FieldSchemaBuilder::array()
            .items_schema(FieldSchemaBuilder::object()
                .property("name", FieldSchemaBuilder::string().min_length(2).required().build())
                .property("role", FieldSchemaBuilder::string().required().build())
                .build())
            .min_items(1)
            .max_items(100)
            .required()
            .build())
        .field("website", FieldSchemaBuilder::string()
            .format(Format::Url)
            .optional()
            .build())
        .build();

    let registry = Arc::new(SchemaRegistry::new());
    registry.register_schema("companies", company_schema, Some(
        SchemaConfig::new()
            .strict_mode(false)
            .validate_formats(true)
    ))?;
    
    let storage: MemoryStorage<Value> = MemoryStorage::with_collection_and_registry(
        "companies",
        registry.clone()
    );

    let company = json!({
        "name": "TechCorp Inc.",
        "address": {
            "street": "123 Innovation Drive",
            "city": "San Francisco",
            "country": "USA"
        },
        "employees": [
            {
                "name": "John Smith",
                "role": "CEO"
            },
            {
                "name": "Jane Doe", 
                "role": "CTO"
            }
        ],
        "website": "https://techcorp.com"
    });

    match storage.create(company).await {
        Ok(doc) => println!("✅ Company created: {} (ID: {})", 
                          doc.data["name"], doc.metadata.id),
        Err(e) => println!("❌ Error: {}", e),
    }

    // Show registry stats
    let stats = registry.get_stats()?;
    println!("📊 Registry Stats: {} collections", stats.total_collections);
    
    println!();
    Ok(())
}

async fn demo_error_handling() -> Result<(), Box<dyn std::error::Error>> {
    println!("3️⃣ Error Handling Demo");
    println!("----------------------");
    
    // Create strict schema
    let product_schema = SchemaBuilder::new("product_v1")
        .field("name", FieldSchemaBuilder::string()
            .min_length(3)
            .max_length(100)
            .required()
            .build())
        .field("price", FieldSchemaBuilder::number()
            .minimum(0.01)
            .required()
            .build())
        .field("sku", FieldSchemaBuilder::string()
            .pattern(r"^[A-Z]{2}-\d{4}$").unwrap() // Format: AB-1234
            .required()
            .build())
        .build();

    let registry = Arc::new(SchemaRegistry::new());
    registry.register_schema("products", product_schema, Some(
        SchemaConfig::new()
            .strict_mode(true)
            .fail_fast(false) // Collect all errors
    ))?;
    
    let storage: MemoryStorage<Value> = MemoryStorage::with_collection_and_registry(
        "products",
        registry.clone()
    );

    // Test various invalid documents
    let test_cases = vec![
        ("Missing required field", json!({
            "name": "Widget"
            // missing price and sku
        })),
        ("Invalid price", json!({
            "name": "Gadget",
            "price": -10.0,
            "sku": "AB-1234"
        })),
        ("Invalid SKU format", json!({
            "name": "Device",
            "price": 99.99,
            "sku": "invalid-sku"
        })),
        ("Name too short", json!({
            "name": "A",
            "price": 50.0,
            "sku": "CD-5678"
        })),
    ];

    for (test_name, invalid_product) in test_cases {
        println!("Testing: {}", test_name);
        match storage.create(invalid_product).await {
            Ok(_) => println!("  ❌ Should have failed"),
            Err(DatabaseError::SchemaValidationError { message, field_path, validation_errors }) => {
                println!("  ✅ Validation failed as expected");
                println!("     Message: {}", message);
                if let Some(path) = field_path {
                    println!("     Field: {}", path);
                }
                println!("     Errors: {}", validation_errors.len());
            }
            Err(e) => println!("  ❌ Unexpected error: {}", e),
        }
    }

    // Test valid product
    let valid_product = json!({
        "name": "Super Widget",
        "price": 29.99,
        "sku": "SW-1001"
    });

    match storage.create(valid_product).await {
        Ok(doc) => println!("✅ Valid product created: {} (ID: {})", 
                          doc.data["name"], doc.metadata.id),
        Err(e) => println!("❌ Error: {}", e),
    }

    println!();
    Ok(())
} 