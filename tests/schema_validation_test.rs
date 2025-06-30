use nosql_memory_db::*;
use serde_json::{json, Value};
use std::sync::Arc;

#[tokio::test]
async fn test_basic_schema_validation() {
    let registry = SchemaRegistry::new();
    
    // Create a simple schema
    let schema = SchemaBuilder::new("test_schema")
        .field("name", FieldSchemaBuilder::string().required().min_length(2).build())
        .field("age", FieldSchemaBuilder::integer().required().minimum(0.0).build())
        .build();
    
    registry.register_schema("users", schema, None).unwrap();
    
    // Valid document
    let valid_doc = json!({
        "name": "John",
        "age": 25
    });
    
    assert!(registry.validate_document("users", &valid_doc, None).is_ok());
    
    // Invalid document - missing required field
    let invalid_doc = json!({
        "name": "John"
    });
    
    assert!(registry.validate_document("users", &invalid_doc, None).is_err());
}

#[tokio::test]
async fn test_format_validators() {
    let registry = SchemaRegistry::new();
    
    let schema = SchemaBuilder::new("format_test")
        .field("email", FieldSchemaBuilder::string().required().email().build())
        .field("website", FieldSchemaBuilder::string().optional().url().build())
        .field("phone", FieldSchemaBuilder::string().optional().phone().build())
        .field("uuid", FieldSchemaBuilder::string().optional().uuid().build())
        .build();
    
    registry.register_schema("contacts", schema, None).unwrap();
    
    // Valid formats
    let valid_doc = json!({
        "email": "test@example.com",
        "website": "https://example.com",
        "phone": "+1-234-567-8900",
        "uuid": "550e8400-e29b-41d4-a716-446655440000"
    });
    
    assert!(registry.validate_document("contacts", &valid_doc, None).is_ok());
    
    // Invalid email format
    let invalid_email = json!({
        "email": "invalid-email"
    });
    
    let result = registry.validate_document("contacts", &invalid_email, None);
    assert!(result.is_err());
    
    // Invalid URL format
    let invalid_url = json!({
        "email": "test@example.com",
        "website": "not-a-url"
    });
    
    assert!(registry.validate_document("contacts", &invalid_url, None).is_err());
}

#[tokio::test]
async fn test_numeric_constraints() {
    let registry = SchemaRegistry::new();
    
    let schema = SchemaBuilder::new("numeric_test")
        .field("price", FieldSchemaBuilder::number()
            .required()
            .minimum(0.0)
            .maximum(1000.0)
            .build())
        .field("quantity", FieldSchemaBuilder::integer()
            .required()
            .minimum(1.0)
            .multiple_of(1.0)
            .build())
        .build();
    
    registry.register_schema("products", schema, None).unwrap();
    
    // Valid numbers
    let valid_doc = json!({
        "price": 99.99,
        "quantity": 5
    });
    
    assert!(registry.validate_document("products", &valid_doc, None).is_ok());
    
    // Price too high
    let invalid_price = json!({
        "price": 1500.0,
        "quantity": 5
    });
    
    assert!(registry.validate_document("products", &invalid_price, None).is_err());
    
    // Quantity too low
    let invalid_quantity = json!({
        "price": 99.99,
        "quantity": 0
    });
    
    assert!(registry.validate_document("products", &invalid_quantity, None).is_err());
}

#[tokio::test]
async fn test_array_validation() {
    let registry = SchemaRegistry::new();
    
    let item_schema = FieldSchemaBuilder::string().min_length(1).build();
    let schema = SchemaBuilder::new("array_test")
        .field("tags", FieldSchemaBuilder::array()
            .required()
            .min_items(1)
            .max_items(5)
            .unique_items()
            .items_schema(item_schema)
            .build())
        .build();
    
    registry.register_schema("documents", schema, None).unwrap();
    
    // Valid array
    let valid_doc = json!({
        "tags": ["tag1", "tag2", "tag3"]
    });
    
    assert!(registry.validate_document("documents", &valid_doc, None).is_ok());
    
    // Too many items
    let too_many_items = json!({
        "tags": ["tag1", "tag2", "tag3", "tag4", "tag5", "tag6"]
    });
    
    assert!(registry.validate_document("documents", &too_many_items, None).is_err());
    
    // Empty array (violates min_items)
    let empty_array = json!({
        "tags": []
    });
    
    assert!(registry.validate_document("documents", &empty_array, None).is_err());
    
    // Duplicate items (violates unique_items)
    let duplicate_items = json!({
        "tags": ["tag1", "tag2", "tag1"]
    });
    
    assert!(registry.validate_document("documents", &duplicate_items, None).is_err());
}

#[tokio::test]
async fn test_nested_object_validation() {
    let registry = SchemaRegistry::new();
    
    let address_schema = FieldSchemaBuilder::object()
        .required()
        .property("street", FieldSchemaBuilder::string().required().build())
        .property("city", FieldSchemaBuilder::string().required().build())
        .property("zipcode", FieldSchemaBuilder::string().required().min_length(5).build())
        .build();
    
    let schema = SchemaBuilder::new("nested_test")
        .field("name", FieldSchemaBuilder::string().required().build())
        .field("address", address_schema)
        .build();
    
    registry.register_schema("users", schema, None).unwrap();
    
    // Valid nested object
    let valid_doc = json!({
        "name": "John Doe",
        "address": {
            "street": "123 Main St",
            "city": "Anytown",
            "zipcode": "12345"
        }
    });
    
    assert!(registry.validate_document("users", &valid_doc, None).is_ok());
    
    // Missing nested required field
    let missing_nested = json!({
        "name": "John Doe",
        "address": {
            "street": "123 Main St",
            "city": "Anytown"
            // Missing zipcode
        }
    });
    
    assert!(registry.validate_document("users", &missing_nested, None).is_err());
    
    // Invalid nested field constraint
    let invalid_nested = json!({
        "name": "John Doe",
        "address": {
            "street": "123 Main St",
            "city": "Anytown",
            "zipcode": "123" // Too short
        }
    });
    
    assert!(registry.validate_document("users", &invalid_nested, None).is_err());
}

#[tokio::test]
async fn test_additional_properties() {
    let registry = SchemaRegistry::new();
    
    let schema = SchemaBuilder::new("strict_test")
        .field("name", FieldSchemaBuilder::string().required().build())
        .field("age", FieldSchemaBuilder::integer().required().build())
        .additional_properties(false)
        .build();
    
    registry.register_schema("users", schema, None).unwrap();
    
    // Valid document with only defined fields
    let valid_doc = json!({
        "name": "John",
        "age": 25
    });
    
    assert!(registry.validate_document("users", &valid_doc, None).is_ok());
    
    // Invalid document with additional property
    let additional_prop = json!({
        "name": "John",
        "age": 25,
        "extra": "not allowed"
    });
    
    assert!(registry.validate_document("users", &additional_prop, None).is_err());
}

#[tokio::test]
async fn test_schema_config_options() {
    let registry = SchemaRegistry::new();
    
    let schema = SchemaBuilder::new("config_test")
        .field("name", FieldSchemaBuilder::string().required().build())
        .build();
    
    // Register with strict config
    let strict_config = SchemaConfig::new()
        .enabled(true)
        .strict_mode(true)
        .fail_fast(true);
    
    registry.register_schema("strict_collection", schema.clone(), Some(strict_config)).unwrap();
    
    // Register with lenient config
    let lenient_config = SchemaConfig::new()
        .enabled(true)
        .strict_mode(false)
        .fail_fast(false);
    
    registry.register_schema("lenient_collection", schema, Some(lenient_config)).unwrap();
    
    let test_doc = json!({
        "name": "Test",
        "extra": "field"
    });
    
    // Should pass for lenient collection (additional properties allowed)
    assert!(registry.validate_document("lenient_collection", &test_doc, None).is_ok());
    
    // Should fail for strict collection
    assert!(registry.validate_document("strict_collection", &test_doc, None).is_err());
}

#[tokio::test]
async fn test_validation_enable_disable() {
    let registry = SchemaRegistry::new();
    
    let schema = SchemaBuilder::new("toggle_test")
        .field("name", FieldSchemaBuilder::string().required().build())
        .build();
    
    registry.register_schema("test_collection", schema, None).unwrap();
    
    let invalid_doc = json!({
        "age": 25
        // Missing required name field
    });
    
    // Should fail validation when enabled
    assert!(registry.validate_document("test_collection", &invalid_doc, None).is_err());
    
    // Disable validation
    registry.set_validation_enabled("test_collection", false).unwrap();
    
    // Should pass validation when disabled
    assert!(registry.validate_document("test_collection", &invalid_doc, None).is_ok());
    
    // Re-enable validation
    registry.set_validation_enabled("test_collection", true).unwrap();
    
    // Should fail validation when re-enabled
    assert!(registry.validate_document("test_collection", &invalid_doc, None).is_err());
}

#[tokio::test]
async fn test_memory_storage_integration() {
    let registry = Arc::new(SchemaRegistry::new());
    
    let schema = SchemaBuilder::new("integration_test")
        .field("name", FieldSchemaBuilder::string().required().min_length(2).build())
        .field("email", FieldSchemaBuilder::string().required().email().build())
        .build();
    
    registry.register_schema("users", schema, None).unwrap();
    
    let storage: MemoryStorage<Value> = MemoryStorage::with_collection_and_registry(
        "users",
        Arc::clone(&registry)
    );
    
    // Valid document should be created successfully
    let valid_data = json!({
        "name": "John Doe",
        "email": "john@example.com"
    });
    
    let result = storage.create(valid_data).await;
    assert!(result.is_ok());
    
    // Invalid document should be rejected
    let invalid_data = json!({
        "name": "X", // Too short
        "email": "invalid-email" // Invalid format
    });
    
    let result = storage.create(invalid_data).await;
    assert!(result.is_err());
    
    if let Err(DatabaseError::SchemaValidationError { .. }) = result {
        // Expected error type
    } else {
        panic!("Expected SchemaValidationError");
    }
}

#[tokio::test]
async fn test_union_types() {
    let registry = SchemaRegistry::new();
    
    let schema = SchemaBuilder::new("union_test")
        .field("value", FieldSchemaBuilder::union(vec![
            FieldType::String,
            FieldType::Number,
            FieldType::Boolean
        ]).required().build())
        .build();
    
    registry.register_schema("flexible", schema, None).unwrap();
    
    // String value
    let string_doc = json!({
        "value": "hello"
    });
    assert!(registry.validate_document("flexible", &string_doc, None).is_ok());
    
    // Number value
    let number_doc = json!({
        "value": 42
    });
    assert!(registry.validate_document("flexible", &number_doc, None).is_ok());
    
    // Boolean value
    let boolean_doc = json!({
        "value": true
    });
    assert!(registry.validate_document("flexible", &boolean_doc, None).is_ok());
    
    // Invalid type (array not in union)
    let invalid_doc = json!({
        "value": ["not", "allowed"]
    });
    assert!(registry.validate_document("flexible", &invalid_doc, None).is_err());
}

#[tokio::test]
async fn test_registry_stats() {
    let registry = SchemaRegistry::new();
    
    // Initially empty
    let stats = registry.get_stats().unwrap();
    assert_eq!(stats.total_collections, 0);
    assert_eq!(stats.enabled_collections, 0);
    assert_eq!(stats.disabled_collections, 0);
    
    // Add some schemas
    let schema1 = SchemaBuilder::new("schema1").build();
    let schema2 = SchemaBuilder::new("schema2").build();
    let schema3 = SchemaBuilder::new("schema3").build();
    
    registry.register_schema("collection1", schema1, None).unwrap();
    registry.register_schema("collection2", schema2, None).unwrap();
    registry.register_schema("collection3", schema3, None).unwrap();
    
    let stats = registry.get_stats().unwrap();
    assert_eq!(stats.total_collections, 3);
    assert_eq!(stats.enabled_collections, 3);
    assert_eq!(stats.disabled_collections, 0);
    
    // Disable one collection
    registry.set_validation_enabled("collection2", false).unwrap();
    
    let stats = registry.get_stats().unwrap();
    assert_eq!(stats.total_collections, 3);
    assert_eq!(stats.enabled_collections, 2);
    assert_eq!(stats.disabled_collections, 1);
    
    // Test collection listing
    let collections = registry.list_collections().unwrap();
    assert_eq!(collections.len(), 3);
    assert!(collections.contains(&"collection1".to_string()));
    assert!(collections.contains(&"collection2".to_string()));
    assert!(collections.contains(&"collection3".to_string()));
}

#[tokio::test]
async fn test_error_messages() {
    let registry = SchemaRegistry::new();
    
    let schema = SchemaBuilder::new("error_test")
        .field("name", FieldSchemaBuilder::string().required().min_length(3).build())
        .field("age", FieldSchemaBuilder::integer().required().minimum(18.0).build())
        .field("email", FieldSchemaBuilder::string().required().email().build())
        .build();
    
    registry.register_schema("users", schema, None).unwrap();
    
    let invalid_doc = json!({
        "name": "Jo", // Too short
        "age": 16,    // Too young
        "email": "invalid" // Invalid format
    });
    
    let result = registry.validate_document("users", &invalid_doc, None);
    assert!(result.is_err());
    
    if let Err(error) = result {
        let error_message = error.to_string();
        assert!(error_message.contains("validation"));
        
        // Check that error has proper structure
        if let DatabaseError::SchemaValidationError { validation_errors, .. } = error {
            assert!(!validation_errors.is_empty());
            
            // Each validation error should have a user-friendly message
            for validation_error in validation_errors {
                let friendly_message = validation_error.user_friendly_message();
                assert!(!friendly_message.is_empty());
            }
        }
    }
} 