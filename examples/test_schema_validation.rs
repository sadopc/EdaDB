use serde_json::{json, Value};
use nosql_memory_db::{
    MemoryStorage, CrudDatabase, DatabaseError,
    schema::{
        SchemaBuilder, FieldSchemaBuilder, FieldType, Format, SchemaRegistry, SchemaConfig,
        ValidationError
    }
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    
    println!("🚀 Schema Validation Test Examples");
    println!("====================================\n");
    
    // Test 1: Basic Schema Validation
    test_basic_validation().await?;
    
    // Test 2: Format Validation
    test_format_validation().await?;
    
    // Test 3: Complex Object Validation
    test_complex_object_validation().await?;
    
    // Test 4: Array Validation
    test_array_validation().await?;
    
    // Test 5: Schema Registry Management
    test_schema_registry().await?;

    println!("\n✅ All schema validation tests completed successfully!");
    Ok(())
}

async fn test_basic_validation() -> Result<(), Box<dyn std::error::Error>> {
    println!("1️⃣ Testing Basic Schema Validation");
    println!("-----------------------------------");
    
    // Create a user schema
    let user_schema = SchemaBuilder::new("user_v1")
        .field("name", FieldSchemaBuilder::string()
            .min_length(2)
            .max_length(50)
            .required()
            .build())
        .field("age", FieldSchemaBuilder::number()
            .minimum(0.0)
            .maximum(150.0)
            .required()
            .build())
        .field("email", FieldSchemaBuilder::string()
            .format(Format::Email)
            .required()
            .build())
        .field("active", FieldSchemaBuilder::boolean()
            .optional()
            .build())
        .build();

    // Create storage with collection and schema
    let registry = Arc::new(SchemaRegistry::new());
    registry.register_schema("users", user_schema, None)?;
    
    let storage: MemoryStorage<Value> = MemoryStorage::with_collection_and_registry(
        "users",
        registry.clone()
    );

    // Test valid document
    let valid_user = json!({
        "name": "John Doe",
        "age": 30,
        "email": "john@example.com",
        "active": true
    });

    match storage.create(valid_user.clone()).await {
        Ok(document) => println!("✅ Valid user created with ID: {}", document.metadata.id),
        Err(e) => println!("❌ Failed to create valid user: {}", e),
    }

    // Test invalid document - missing required field
    let invalid_user1 = json!({
        "name": "Jane Doe",
        "age": 25
        // missing required email field
    });

    match storage.create(invalid_user1).await {
        Ok(_) => println!("❌ Should have failed - missing required field"),
        Err(DatabaseError::SchemaValidationError { message, .. }) => {
            println!("✅ Correctly caught missing required field: {}", message);
        }
        Err(e) => println!("❌ Unexpected error: {}", e),
    }

    // Test invalid document - invalid email format
    let invalid_user2 = json!({
        "name": "Bob Smith",
        "age": 40,
        "email": "invalid-email"
    });

    match storage.create(invalid_user2).await {
        Ok(_) => println!("❌ Should have failed - invalid email format"),
        Err(DatabaseError::SchemaValidationError { message, .. }) => {
            println!("✅ Correctly caught invalid email format: {}", message);
        }
        Err(e) => println!("❌ Unexpected error: {}", e),
    }

    println!();
    Ok(())
}

async fn test_format_validation() -> Result<(), Box<dyn std::error::Error>> {
    println!("2️⃣ Testing Format Validation");
    println!("-----------------------------");
    
    let contact_schema = SchemaBuilder::new("contact_v1")
        .field("website", FieldSchemaBuilder::string()
            .format(Format::Url)
            .optional()
            .build())
        .field("phone", FieldSchemaBuilder::string()
            .format(Format::Phone)
            .optional()
            .build())
        .field("created_at", FieldSchemaBuilder::string()
            .format(Format::DateTime)
            .optional()
            .build())
        .field("uuid", FieldSchemaBuilder::string()
            .format(Format::Uuid)
            .optional()
            .build())
        .build();

    let registry = Arc::new(SchemaRegistry::new());
    registry.register_schema("contacts", contact_schema, None)?;
    
    let storage: MemoryStorage<Value> = MemoryStorage::with_collection_and_registry(
        "contacts",
        registry.clone()
    );

    // Test valid formats
    let valid_contact = json!({
        "website": "https://example.com",
        "phone": "+1-555-123-4567",
        "created_at": "2023-12-01T10:30:00Z",
        "uuid": "550e8400-e29b-41d4-a716-446655440000"
    });

    match storage.create(valid_contact).await {
        Ok(document) => println!("✅ Valid contact created with ID: {}", document.metadata.id),
        Err(e) => println!("❌ Failed to create valid contact: {}", e),
    }

    // Test invalid URL
    let invalid_contact = json!({
        "website": "not-a-valid-url",
        "phone": "+1-555-123-4567"
    });

    match storage.create(invalid_contact).await {
        Ok(_) => println!("❌ Should have failed - invalid URL"),
        Err(DatabaseError::SchemaValidationError { message, .. }) => {
            println!("✅ Correctly caught invalid URL: {}", message);
        }
        Err(e) => println!("❌ Unexpected error: {}", e),
    }

    println!();
    Ok(())
}

async fn test_complex_object_validation() -> Result<(), Box<dyn std::error::Error>> {
    println!("3️⃣ Testing Complex Object Validation");
    println!("-------------------------------------");
    
    // Create nested address schema
    let address_schema = SchemaBuilder::new("address_v1")
        .field("street", FieldSchemaBuilder::string()
            .min_length(5)
            .required()
            .build())
        .field("city", FieldSchemaBuilder::string()
            .min_length(2)
            .required()
            .build())
        .field("zipcode", FieldSchemaBuilder::string()
            .pattern(r"^\d{5}(-\d{4})?$").unwrap()
            .required()
            .build())
        .build();

    // Create person schema with nested address
    let person_schema = SchemaBuilder::new("person_v1")
        .field("name", FieldSchemaBuilder::string()
            .min_length(2)
            .required()
            .build())
        .field("address", FieldSchemaBuilder::object()
            .property("street", FieldSchemaBuilder::string().min_length(5).required().build())
            .property("city", FieldSchemaBuilder::string().min_length(2).required().build())
            .property("zipcode", FieldSchemaBuilder::string().pattern(r"^\d{5}(-\d{4})?$").unwrap().required().build())
            .required()
            .build())
        .field("tags", FieldSchemaBuilder::array()
            .items_schema(FieldSchemaBuilder::string().build())
            .min_items(0)
            .max_items(10)
            .unique_items()
            .optional()
            .build())
        .build();

    let registry = Arc::new(SchemaRegistry::new());
    registry.register_schema("persons", person_schema, None)?;
    
    let storage: MemoryStorage<Value> = MemoryStorage::with_collection_and_registry(
        "persons",
        registry.clone()
    );

    // Test valid nested object
    let valid_person = json!({
        "name": "Alice Johnson",
        "address": {
            "street": "123 Main Street",
            "city": "Springfield",
            "zipcode": "12345"
        },
        "tags": ["developer", "rust", "database"]
    });

    match storage.create(valid_person).await {
        Ok(document) => println!("✅ Valid person created with ID: {}", document.metadata.id),
        Err(e) => println!("❌ Failed to create valid person: {}", e),
    }

    // Test invalid nested object - invalid zipcode
    let invalid_person = json!({
        "name": "Bob Wilson",
        "address": {
            "street": "456 Oak Ave",
            "city": "Portland",
            "zipcode": "invalid-zip"
        }
    });

    match storage.create(invalid_person).await {
        Ok(_) => println!("❌ Should have failed - invalid zipcode"),
        Err(DatabaseError::SchemaValidationError { message, .. }) => {
            println!("✅ Correctly caught invalid zipcode: {}", message);
        }
        Err(e) => println!("❌ Unexpected error: {}", e),
    }

    println!();
    Ok(())
}

async fn test_array_validation() -> Result<(), Box<dyn std::error::Error>> {
    println!("4️⃣ Testing Array Validation");
    println!("----------------------------");
    
    let product_schema = SchemaBuilder::new("product_v1")
        .field("name", FieldSchemaBuilder::string()
            .min_length(2)
            .required()
            .build())
        .field("prices", FieldSchemaBuilder::array()
            .items_schema(FieldSchemaBuilder::number().build())
            .min_items(1)
            .max_items(5)
            .required()
            .build())
        .field("categories", FieldSchemaBuilder::array()
            .items_schema(FieldSchemaBuilder::string().build())
            .unique_items()
            .optional()
            .build())
        .build();

    let registry = Arc::new(SchemaRegistry::new());
    registry.register_schema("products", product_schema, None)?;
    
    let storage: MemoryStorage<Value> = MemoryStorage::with_collection_and_registry(
        "products",
        registry.clone()
    );

    // Test valid array
    let valid_product = json!({
        "name": "Laptop",
        "prices": [999.99, 899.99, 1099.99],
        "categories": ["electronics", "computers", "laptops"]
    });

    match storage.create(valid_product).await {
        Ok(document) => println!("✅ Valid product created with ID: {}", document.metadata.id),
        Err(e) => println!("❌ Failed to create valid product: {}", e),
    }

    // Test invalid array - too many items
    let invalid_product = json!({
        "name": "Phone",
        "prices": [599.99, 649.99, 699.99, 749.99, 799.99, 849.99], // 6 items, max is 5
        "categories": ["electronics"]
    });

    match storage.create(invalid_product).await {
        Ok(_) => println!("❌ Should have failed - too many array items"),
        Err(DatabaseError::SchemaValidationError { message, .. }) => {
            println!("✅ Correctly caught array max items violation: {}", message);
        }
        Err(e) => println!("❌ Unexpected error: {}", e),
    }

    println!();
    Ok(())
}

async fn test_schema_registry() -> Result<(), Box<dyn std::error::Error>> {
    println!("5️⃣ Testing Schema Registry Management");
    println!("--------------------------------------");
    
    let registry = Arc::new(SchemaRegistry::new());
    
    // Register multiple schemas
    let user_schema = SchemaBuilder::new("user_v2")
        .field("username", FieldSchemaBuilder::string()
            .min_length(3)
            .max_length(20)
            .pattern(r"^[a-zA-Z0-9_]+$").unwrap()
            .required()
            .build())
        .build();
    
    let post_schema = SchemaBuilder::new("post_v1")
        .field("title", FieldSchemaBuilder::string()
            .min_length(5)
            .max_length(100)
            .required()
            .build())
        .field("content", FieldSchemaBuilder::string()
            .min_length(10)
            .required()
            .build())
        .build();

    registry.register_schema("users", user_schema, Some(
        SchemaConfig::new()
            .strict_mode(true)
            .fail_fast(false)
    ))?;
    
    registry.register_schema("posts", post_schema, Some(
        SchemaConfig::new()
            .strict_mode(false)
            .validate_formats(true)
    ))?;

    // Check registry stats
    let stats = registry.get_stats()?;
    println!("📊 Registry Stats:");
    println!("   Total collections: {}", stats.total_collections);
    println!("   Enabled collections: {}", stats.enabled_collections);
    println!("   Disabled collections: {}", stats.disabled_collections);

    // List all collections
    let collections = registry.list_collections()?;
    println!("📁 Collections: {:?}", collections);

    // Test validation with different configs
    let storage1: MemoryStorage<Value> = MemoryStorage::with_collection_and_registry(
        "users",
        registry.clone()
    );
    
    let storage2: MemoryStorage<Value> = MemoryStorage::with_collection_and_registry(
        "posts",
        registry.clone()
    );

    // Valid documents for both collections
    let valid_user = json!({
        "username": "john_doe_123"
    });

    let valid_post = json!({
        "title": "Hello World",
        "content": "This is my first post in the database!"
    });

    match storage1.create(valid_user).await {
        Ok(document) => println!("✅ User created with ID: {}", document.metadata.id),
        Err(e) => println!("❌ Failed to create user: {}", e),
    }

    match storage2.create(valid_post).await {
        Ok(document) => println!("✅ Post created with ID: {}", document.metadata.id),
        Err(e) => println!("❌ Failed to create post: {}", e),
    }

    // Test disabling validation for a collection
    registry.set_validation_enabled("posts", false)?;
    println!("🔒 Disabled validation for 'posts' collection");

    // This should now succeed even with invalid data
    let invalid_post = json!({
        "title": "Hi", // too short
        "content": "Short" // too short
    });

    match storage2.create(invalid_post).await {
        Ok(document) => println!("✅ Invalid post created (validation disabled) with ID: {}", document.metadata.id),
        Err(e) => println!("❌ Unexpected error: {}", e),
    }

    println!();
    Ok(())
} 