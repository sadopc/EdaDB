use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::schema::definition::SchemaDefinition;
use crate::schema::validation::{ValidationEngine, ValidationOptions};
use crate::schema::error::{ValidationError, ValidationResult, ErrorContext};

/// Schema configuration for a collection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaConfig {
    /// Whether schema validation is enabled for this collection
    pub enabled: bool,
    /// Whether to enforce strict validation (fail on unknown fields)
    pub strict_mode: bool,
    /// Whether to allow type coercion where possible
    pub allow_type_coercion: bool,
    /// Whether to validate format constraints
    pub validate_formats: bool,
    /// Whether to fail fast on first error or collect all errors
    pub fail_fast: bool,
    /// Maximum validation depth for nested objects
    pub max_depth: usize,
}

impl Default for SchemaConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            strict_mode: false,
            allow_type_coercion: false,
            validate_formats: true,
            fail_fast: false,
            max_depth: 50,
        }
    }
}

impl SchemaConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    pub fn strict_mode(mut self, strict: bool) -> Self {
        self.strict_mode = strict;
        self
    }

    pub fn allow_type_coercion(mut self, allow: bool) -> Self {
        self.allow_type_coercion = allow;
        self
    }

    pub fn validate_formats(mut self, validate: bool) -> Self {
        self.validate_formats = validate;
        self
    }

    pub fn fail_fast(mut self, fail_fast: bool) -> Self {
        self.fail_fast = fail_fast;
        self
    }

    pub fn max_depth(mut self, depth: usize) -> Self {
        self.max_depth = depth;
        self
    }

    /// Convert to ValidationOptions
    pub fn to_validation_options(&self) -> ValidationOptions {
        ValidationOptions::new()
            .max_depth(self.max_depth)
            .fail_fast(self.fail_fast)
            .validate_formats(self.validate_formats)
            .allow_type_coercion(self.allow_type_coercion)
            .strict_mode(self.strict_mode)
    }
}

/// Represents a collection's schema definition and configuration
#[derive(Debug, Clone)]
pub struct CollectionSchema {
    pub collection_name: String,
    pub schema: SchemaDefinition,
    pub config: SchemaConfig,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
}

impl CollectionSchema {
    pub fn new(collection_name: String, schema: SchemaDefinition, config: SchemaConfig) -> Self {
        let now = chrono::Utc::now();
        Self {
            collection_name,
            schema,
            config,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn is_validation_enabled(&self) -> bool {
        self.config.enabled
    }

    pub fn update_schema(&mut self, schema: SchemaDefinition) {
        self.schema = schema;
        self.updated_at = chrono::Utc::now();
    }

    pub fn update_config(&mut self, config: SchemaConfig) {
        self.config = config;
        self.updated_at = chrono::Utc::now();
    }
}

/// Thread-safe registry for managing collection schemas
#[derive(Debug)]
pub struct SchemaRegistry {
    /// Collection name -> CollectionSchema mapping
    schemas: Arc<RwLock<HashMap<String, CollectionSchema>>>,
    /// Default configuration for new collections
    default_config: SchemaConfig,
    /// Validation engine instance
    validation_engine: Arc<ValidationEngine>,
}

impl SchemaRegistry {
    pub fn new() -> Self {
        Self {
            schemas: Arc::new(RwLock::new(HashMap::new())),
            default_config: SchemaConfig::default(),
            validation_engine: Arc::new(ValidationEngine::with_default_options()),
        }
    }

    pub fn with_default_config(default_config: SchemaConfig) -> Self {
        let validation_options = default_config.to_validation_options();
        Self {
            schemas: Arc::new(RwLock::new(HashMap::new())),
            default_config,
            validation_engine: Arc::new(ValidationEngine::new(validation_options)),
        }
    }

    /// Register a schema for a collection
    pub fn register_schema(
        &self,
        collection_name: &str,
        schema: SchemaDefinition,
        config: Option<SchemaConfig>,
    ) -> ValidationResult<()> {
        let config = config.unwrap_or_else(|| self.default_config.clone());
        let collection_schema = CollectionSchema::new(collection_name.to_string(), schema, config);

        let mut schemas = self.schemas.write().map_err(|_| {
            ValidationError::SchemaError {
                message: "Failed to acquire write lock on schema registry".to_string(),
                context: ErrorContext::new(),
            }
        })?;

        schemas.insert(collection_name.to_string(), collection_schema);
        Ok(())
    }

    /// Get schema for a collection
    pub fn get_schema(&self, collection_name: &str) -> ValidationResult<Option<CollectionSchema>> {
        let schemas = self.schemas.read().map_err(|_| {
            ValidationError::SchemaError {
                message: "Failed to acquire read lock on schema registry".to_string(),
                context: ErrorContext::new(),
            }
        })?;

        Ok(schemas.get(collection_name).cloned())
    }

    /// Update schema for a collection
    pub fn update_schema(
        &self,
        collection_name: &str,
        schema: SchemaDefinition,
    ) -> ValidationResult<()> {
        let mut schemas = self.schemas.write().map_err(|_| {
            ValidationError::SchemaError {
                message: "Failed to acquire write lock on schema registry".to_string(),
                context: ErrorContext::new(),
            }
        })?;

        if let Some(collection_schema) = schemas.get_mut(collection_name) {
            collection_schema.update_schema(schema);
            Ok(())
        } else {
            Err(ValidationError::SchemaError {
                message: format!("Schema not found for collection: {}", collection_name),
                context: ErrorContext::new(),
            })
        }
    }

    /// Update configuration for a collection
    pub fn update_config(
        &self,
        collection_name: &str,
        config: SchemaConfig,
    ) -> ValidationResult<()> {
        let mut schemas = self.schemas.write().map_err(|_| {
            ValidationError::SchemaError {
                message: "Failed to acquire write lock on schema registry".to_string(),
                context: ErrorContext::new(),
            }
        })?;

        if let Some(collection_schema) = schemas.get_mut(collection_name) {
            collection_schema.update_config(config);
            Ok(())
        } else {
            Err(ValidationError::SchemaError {
                message: format!("Schema not found for collection: {}", collection_name),
                context: ErrorContext::new(),
            })
        }
    }

    /// Remove schema for a collection
    pub fn remove_schema(&self, collection_name: &str) -> ValidationResult<bool> {
        let mut schemas = self.schemas.write().map_err(|_| {
            ValidationError::SchemaError {
                message: "Failed to acquire write lock on schema registry".to_string(),
                context: ErrorContext::new(),
            }
        })?;

        Ok(schemas.remove(collection_name).is_some())
    }

    /// List all registered collections
    pub fn list_collections(&self) -> ValidationResult<Vec<String>> {
        let schemas = self.schemas.read().map_err(|_| {
            ValidationError::SchemaError {
                message: "Failed to acquire read lock on schema registry".to_string(),
                context: ErrorContext::new(),
            }
        })?;

        Ok(schemas.keys().cloned().collect())
    }

    /// Get collection count
    pub fn collection_count(&self) -> ValidationResult<usize> {
        let schemas = self.schemas.read().map_err(|_| {
            ValidationError::SchemaError {
                message: "Failed to acquire read lock on schema registry".to_string(),
                context: ErrorContext::new(),
            }
        })?;

        Ok(schemas.len())
    }

    /// Check if a collection has a registered schema
    pub fn has_schema(&self, collection_name: &str) -> ValidationResult<bool> {
        let schemas = self.schemas.read().map_err(|_| {
            ValidationError::SchemaError {
                message: "Failed to acquire read lock on schema registry".to_string(),
                context: ErrorContext::new(),
            }
        })?;

        Ok(schemas.contains_key(collection_name))
    }

    /// Validate a document against a collection's schema
    pub fn validate_document(
        &self,
        collection_name: &str,
        document: &Value,
        document_id: Option<String>,
    ) -> ValidationResult<()> {
        let collection_schema = match self.get_schema(collection_name)? {
            Some(schema) => schema,
            None => {
                // No schema registered for this collection - validation passes
                return Ok(());
            }
        };

        // Skip validation if disabled
        if !collection_schema.is_validation_enabled() {
            return Ok(());
        }

        // Create error context
        let mut error_context = ErrorContext::new()
            .with_collection(collection_name.to_string())
            .with_schema(collection_schema.schema.schema_id.clone());

        if let Some(doc_id) = document_id {
            error_context = error_context.with_document_id(doc_id);
        }

        // Create validation engine with collection-specific options
        let validation_options = collection_schema.config.to_validation_options();
        let engine = ValidationEngine::new(validation_options);

        // Validate document
        engine.validate_document(document, &collection_schema.schema, Some(error_context))
    }

    /// Validate multiple documents against a collection's schema
    pub fn validate_documents(
        &self,
        collection_name: &str,
        documents: &[(Value, Option<String>)],
    ) -> ValidationResult<Vec<ValidationResult<()>>> {
        let collection_schema = match self.get_schema(collection_name)? {
            Some(schema) => schema,
            None => {
                // No schema registered - all validations pass
                return Ok(documents.iter().map(|_| Ok(())).collect());
            }
        };

        // Skip validation if disabled
        if !collection_schema.is_validation_enabled() {
            return Ok(documents.iter().map(|_| Ok(())).collect());
        }

        // Create validation engine with collection-specific options
        let validation_options = collection_schema.config.to_validation_options();
        let engine = ValidationEngine::new(validation_options);

        let results = documents
            .iter()
            .map(|(document, document_id)| {
                let mut error_context = ErrorContext::new()
                    .with_collection(collection_name.to_string())
                    .with_schema(collection_schema.schema.schema_id.clone());

                if let Some(doc_id) = document_id {
                    error_context = error_context.with_document_id(doc_id.clone());
                }

                engine.validate_document(document, &collection_schema.schema, Some(error_context))
            })
            .collect();

        Ok(results)
    }

    /// Enable or disable validation for a collection
    pub fn set_validation_enabled(
        &self,
        collection_name: &str,
        enabled: bool,
    ) -> ValidationResult<()> {
        let mut schemas = self.schemas.write().map_err(|_| {
            ValidationError::SchemaError {
                message: "Failed to acquire write lock on schema registry".to_string(),
                context: ErrorContext::new(),
            }
        })?;

        if let Some(collection_schema) = schemas.get_mut(collection_name) {
            collection_schema.config.enabled = enabled;
            collection_schema.updated_at = chrono::Utc::now();
            Ok(())
        } else {
            Err(ValidationError::SchemaError {
                message: format!("Schema not found for collection: {}", collection_name),
                context: ErrorContext::new(),
            })
        }
    }

    /// Get registry statistics
    pub fn get_stats(&self) -> ValidationResult<RegistryStats> {
        let schemas = self.schemas.read().map_err(|_| {
            ValidationError::SchemaError {
                message: "Failed to acquire read lock on schema registry".to_string(),
                context: ErrorContext::new(),
            }
        })?;

        let total_collections = schemas.len();
        let enabled_collections = schemas.values().filter(|s| s.config.enabled).count();
        let disabled_collections = total_collections - enabled_collections;

        let oldest_schema = schemas.values().min_by_key(|s| s.created_at);
        let newest_schema = schemas.values().max_by_key(|s| s.created_at);

        Ok(RegistryStats {
            total_collections,
            enabled_collections,
            disabled_collections,
            oldest_schema_date: oldest_schema.map(|s| s.created_at),
            newest_schema_date: newest_schema.map(|s| s.created_at),
        })
    }
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about the schema registry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryStats {
    pub total_collections: usize,
    pub enabled_collections: usize,
    pub disabled_collections: usize,
    pub oldest_schema_date: Option<chrono::DateTime<chrono::Utc>>,
    pub newest_schema_date: Option<chrono::DateTime<chrono::Utc>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::builder::{SchemaBuilder, FieldSchemaBuilder};
    use serde_json::json;

    #[test]
    fn test_schema_registry_basic_operations() {
        let registry = SchemaRegistry::new();

        // Create a test schema
        let schema = SchemaBuilder::new("user_schema")
            .field("name", FieldSchemaBuilder::string().required().build())
            .field("age", FieldSchemaBuilder::integer().required().minimum(0.0).build())
            .build();

        // Register schema
        registry.register_schema("users", schema, None).unwrap();

        // Check if schema exists
        assert!(registry.has_schema("users").unwrap());
        assert!(!registry.has_schema("nonexistent").unwrap());

        // Get schema
        let retrieved_schema = registry.get_schema("users").unwrap();
        assert!(retrieved_schema.is_some());

        // List collections
        let collections = registry.list_collections().unwrap();
        assert_eq!(collections.len(), 1);
        assert!(collections.contains(&"users".to_string()));
    }

    #[test]
    fn test_document_validation() {
        let registry = SchemaRegistry::new();

        // Create a test schema
        let schema = SchemaBuilder::new("user_schema")
            .field("name", FieldSchemaBuilder::string().required().min_length(1).build())
            .field("age", FieldSchemaBuilder::integer().required().minimum(0.0).build())
            .build();

        registry.register_schema("users", schema, None).unwrap();

        // Valid document
        let valid_doc = json!({
            "name": "John Doe",
            "age": 30
        });

        let result = registry.validate_document("users", &valid_doc, None);
        assert!(result.is_ok());

        // Invalid document (missing required field)
        let invalid_doc = json!({
            "name": "John Doe"
            // Missing age
        });

        let result = registry.validate_document("users", &invalid_doc, None);
        assert!(result.is_err());

        // Invalid document (wrong type)
        let invalid_doc2 = json!({
            "name": "John Doe",
            "age": "thirty"  // Should be integer
        });

        let result = registry.validate_document("users", &invalid_doc2, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_validation_can_be_disabled() {
        let registry = SchemaRegistry::new();

        // Create a strict schema
        let schema = SchemaBuilder::new("user_schema")
            .field("name", FieldSchemaBuilder::string().required().build())
            .field("age", FieldSchemaBuilder::integer().required().build())
            .build();

        registry.register_schema("users", schema, None).unwrap();

        // Invalid document
        let invalid_doc = json!({
            "name": "John Doe"
            // Missing required age
        });

        // Should fail validation
        let result = registry.validate_document("users", &invalid_doc, None);
        assert!(result.is_err());

        // Disable validation
        registry.set_validation_enabled("users", false).unwrap();

        // Should pass validation now
        let result = registry.validate_document("users", &invalid_doc, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_registry_stats() {
        let registry = SchemaRegistry::new();

        // Initially empty
        let stats = registry.get_stats().unwrap();
        assert_eq!(stats.total_collections, 0);

        // Add some schemas
        let schema1 = SchemaBuilder::new("schema1").build();
        let schema2 = SchemaBuilder::new("schema2").build();

        registry.register_schema("collection1", schema1, None).unwrap();
        registry.register_schema("collection2", schema2, None).unwrap();

        let stats = registry.get_stats().unwrap();
        assert_eq!(stats.total_collections, 2);
        assert_eq!(stats.enabled_collections, 2);
        assert_eq!(stats.disabled_collections, 0);

        // Disable one collection
        registry.set_validation_enabled("collection1", false).unwrap();

        let stats = registry.get_stats().unwrap();
        assert_eq!(stats.total_collections, 2);
        assert_eq!(stats.enabled_collections, 1);
        assert_eq!(stats.disabled_collections, 1);
    }
} 