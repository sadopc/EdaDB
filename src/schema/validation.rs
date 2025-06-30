use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

use crate::schema::error::{
    ValidationError, ValidationResult, ErrorContext, ValidationErrorCollector
};
use crate::schema::definition::{
    SchemaDefinition, FieldSchema, FieldType, Constraint,
    NumericConstraint, StringConstraint, ArrayConstraint, ObjectConstraint
};
use crate::schema::format::validate_format;

/// Validation context for tracking current validation state
#[derive(Debug, Clone)]
pub struct ValidationContext {
    /// Current field path being validated
    pub current_path: String,
    /// Maximum validation depth to prevent infinite recursion
    pub max_depth: usize,
    /// Current validation depth
    pub current_depth: usize,
    /// Error context for all validation errors
    pub error_context: ErrorContext,
    /// Custom validation configuration
    pub options: ValidationOptions,
}

impl ValidationContext {
    pub fn new(options: ValidationOptions) -> Self {
        Self {
            current_path: String::new(),
            max_depth: options.max_depth,
            current_depth: 0,
            error_context: ErrorContext::new(),
            options,
        }
    }

    pub fn with_path(&self, path: &str) -> Self {
        let new_path = if self.current_path.is_empty() {
            path.to_string()
        } else {
            format!("{}.{}", self.current_path, path)
        };

        Self {
            current_path: new_path,
            max_depth: self.max_depth,
            current_depth: self.current_depth,
            error_context: self.error_context.clone(),
            options: self.options.clone(),
        }
    }

    pub fn with_array_index(&self, index: usize) -> Self {
        let new_path = format!("{}[{}]", self.current_path, index);
        Self {
            current_path: new_path,
            max_depth: self.max_depth,
            current_depth: self.current_depth,
            error_context: self.error_context.clone(),
            options: self.options.clone(),
        }
    }

    pub fn increment_depth(&self) -> Self {
        Self {
            current_path: self.current_path.clone(),
            max_depth: self.max_depth,
            current_depth: self.current_depth + 1,
            error_context: self.error_context.clone(),
            options: self.options.clone(),
        }
    }

    pub fn is_max_depth_reached(&self) -> bool {
        self.current_depth >= self.max_depth
    }

    pub fn with_error_context(mut self, context: ErrorContext) -> Self {
        self.error_context = context;
        self
    }
}

/// Configuration options for validation
#[derive(Clone)]
pub struct ValidationOptions {
    /// Maximum depth for nested object validation
    pub max_depth: usize,
    /// Whether to stop on first error or collect all errors
    pub fail_fast: bool,
    /// Whether to validate format constraints
    pub validate_formats: bool,
    /// Whether to allow coercion of compatible types
    pub allow_type_coercion: bool,
    /// Whether to use strict validation mode
    pub strict_mode: bool,
    /// Custom format validators
    pub custom_validators: HashMap<String, Arc<dyn Fn(&Value, &ValidationContext) -> ValidationResult<()> + Send + Sync>>,
}

impl std::fmt::Debug for ValidationOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValidationOptions")
            .field("max_depth", &self.max_depth)
            .field("fail_fast", &self.fail_fast)
            .field("validate_formats", &self.validate_formats)
            .field("allow_type_coercion", &self.allow_type_coercion)
            .field("strict_mode", &self.strict_mode)
            .field("custom_validators", &format!("{} custom validators", self.custom_validators.len()))
            .finish()
    }
}

impl Default for ValidationOptions {
    fn default() -> Self {
        Self {
            max_depth: 50,
            fail_fast: false,
            validate_formats: true,
            allow_type_coercion: false,
            strict_mode: false,
            custom_validators: HashMap::new(),
        }
    }
}

impl ValidationOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn max_depth(mut self, depth: usize) -> Self {
        self.max_depth = depth;
        self
    }

    pub fn fail_fast(mut self, fail_fast: bool) -> Self {
        self.fail_fast = fail_fast;
        self
    }

    pub fn validate_formats(mut self, validate: bool) -> Self {
        self.validate_formats = validate;
        self
    }

    pub fn allow_type_coercion(mut self, allow: bool) -> Self {
        self.allow_type_coercion = allow;
        self
    }

    pub fn strict_mode(mut self, strict: bool) -> Self {
        self.strict_mode = strict;
        self
    }
}

/// Core validation engine
#[derive(Debug)]
pub struct ValidationEngine {
    options: ValidationOptions,
}

impl ValidationEngine {
    pub fn new(options: ValidationOptions) -> Self {
        Self { options }
    }

    pub fn with_default_options() -> Self {
        Self::new(ValidationOptions::default())
    }

    /// Validate a JSON value against a schema definition
    pub fn validate_document(
        &self,
        value: &Value,
        schema: &SchemaDefinition,
        context: Option<ErrorContext>,
    ) -> ValidationResult<()> {
        let error_context = context.unwrap_or_else(|| {
            ErrorContext::new().with_schema(schema.schema_id.clone())
        });

        let validation_context = ValidationContext::new(self.options.clone())
            .with_error_context(error_context);

        let mut collector = ValidationErrorCollector::new();

        // Validate document structure
        if let Value::Object(obj) = value {
            self.validate_object_against_schema(obj, schema, &validation_context, &mut collector);
        } else {
            collector.add_type_mismatch(
                &validation_context.current_path,
                "object",
                &FieldType::from_value(value),
                validation_context.error_context.clone(),
            );
        }

        collector.into_result(())
    }

    /// Validate a single field value against its schema
    pub fn validate_field(
        &self,
        value: &Value,
        field_schema: &FieldSchema,
        context: &ValidationContext,
    ) -> ValidationResult<()> {
        let mut collector = ValidationErrorCollector::new();

        // Handle null values
        if value.is_null() {
            if field_schema.nullable {
                return Ok(());
            } else if field_schema.required {
                collector.add_required_field_missing(&context.current_path, context.error_context.clone());
                return collector.into_result(());
            }
        }

        // Check max depth
        if context.is_max_depth_reached() {
            collector.add_error(ValidationError::SchemaError {
                message: "Maximum validation depth exceeded".to_string(),
                context: context.error_context.clone(),
            });
            return collector.into_result(());
        }

        // Type validation
        if !field_schema.field_type.matches_value(value) && !self.can_coerce_type(value, &field_schema.field_type) {
            collector.add_type_mismatch(
                &context.current_path,
                field_schema.field_type.as_str(),
                &FieldType::from_value(value),
                context.error_context.clone(),
            );
            
            if self.options.fail_fast {
                return collector.into_result(());
            }
        }

        // Constraint validation
        for constraint in &field_schema.constraints {
            if let Err(error) = self.validate_constraint(value, constraint, context) {
                collector.add_error(error);
                if self.options.fail_fast {
                    return collector.into_result(());
                }
            }
        }

        // Cross-field dependency validation would go here
        // (requires access to the full document)

        collector.into_result(())
    }

    fn validate_object_against_schema(
        &self,
        obj: &serde_json::Map<String, Value>,
        schema: &SchemaDefinition,
        context: &ValidationContext,
        collector: &mut ValidationErrorCollector,
    ) {
        // Check required fields
        for required_field in &schema.required_fields {
            if !obj.contains_key(required_field) {
                let field_context = context.with_path(required_field);
                collector.add_required_field_missing(&field_context.current_path, context.error_context.clone());
                
                if self.options.fail_fast {
                    return;
                }
            }
        }

        // Validate each field present in the object
        for (field_name, field_value) in obj {
            let field_context = context.with_path(field_name).increment_depth();
            
            if let Some(field_schema) = schema.get_field_schema(field_name) {
                // Field has a defined schema
                if let Err(error) = self.validate_field(field_value, field_schema, &field_context) {
                    collector.add_error(error);
                    if self.options.fail_fast {
                        return;
                    }
                }
            } else if !schema.additional_properties {
                // Additional properties not allowed
                collector.add_error(ValidationError::ObjectError {
                    path: field_context.current_path.clone(),
                    message: format!("Additional property '{}' is not allowed", field_name),
                    field: Some(field_name.clone()),
                    context: context.error_context.clone(),
                });
                
                if self.options.fail_fast {
                    return;
                }
            }
        }
    }

    fn validate_constraint(
        &self,
        value: &Value,
        constraint: &Constraint,
        context: &ValidationContext,
    ) -> ValidationResult<()> {
        match constraint {
            Constraint::String(string_constraint) => {
                self.validate_string_constraint(value, string_constraint, context)
            }
            Constraint::Numeric(numeric_constraint) => {
                self.validate_numeric_constraint(value, numeric_constraint, context)
            }
            Constraint::Array(array_constraint) => {
                self.validate_array_constraint(value, array_constraint, context)
            }
            Constraint::Object(object_constraint) => {
                self.validate_object_constraint(value, object_constraint, context)
            }
        }
    }

    fn validate_string_constraint(
        &self,
        value: &Value,
        constraint: &StringConstraint,
        context: &ValidationContext,
    ) -> ValidationResult<()> {
        if let Some(string_value) = value.as_str() {
            let errors = constraint.validate(string_value);
            if !errors.is_empty() {
                return Err(ValidationError::ConstraintViolation {
                    path: context.current_path.clone(),
                    constraint: "string".to_string(),
                    message: errors.join("; "),
                    actual_value: value.clone(),
                    expected_value: None,
                    context: context.error_context.clone(),
                });
            }

            // Format validation
            if let Some(format) = &constraint.format {
                if self.options.validate_formats {
                    if let Err(mut format_error) = validate_format(format.as_str(), string_value, &context.error_context) {
                        // Update the path in the format error
                        if let ValidationError::FormatError { path, .. } = &mut format_error {
                            *path = context.current_path.clone();
                        }
                        return Err(format_error);
                    }
                }
            }

            Ok(())
        } else {
            Err(ValidationError::TypeMismatch {
                path: context.current_path.clone(),
                expected: "string".to_string(),
                actual: FieldType::from_value(value),
                context: context.error_context.clone(),
            })
        }
    }

    fn validate_numeric_constraint(
        &self,
        value: &Value,
        constraint: &NumericConstraint,
        context: &ValidationContext,
    ) -> ValidationResult<()> {
        if let Some(number_value) = value.as_f64() {
            let errors = constraint.validate(number_value);
            if !errors.is_empty() {
                return Err(ValidationError::ConstraintViolation {
                    path: context.current_path.clone(),
                    constraint: "numeric".to_string(),
                    message: errors.join("; "),
                    actual_value: value.clone(),
                    expected_value: None,
                    context: context.error_context.clone(),
                });
            }
            Ok(())
        } else {
            Err(ValidationError::TypeMismatch {
                path: context.current_path.clone(),
                expected: "number".to_string(),
                actual: FieldType::from_value(value),
                context: context.error_context.clone(),
            })
        }
    }

    fn validate_array_constraint(
        &self,
        value: &Value,
        constraint: &ArrayConstraint,
        context: &ValidationContext,
    ) -> ValidationResult<()> {
        if let Some(array_value) = value.as_array() {
            let errors = constraint.validate(array_value);
            if !errors.is_empty() {
                return Err(ValidationError::ConstraintViolation {
                    path: context.current_path.clone(),
                    constraint: "array".to_string(),
                    message: errors.join("; "),
                    actual_value: value.clone(),
                    expected_value: None,
                    context: context.error_context.clone(),
                });
            }

            // Validate each item if items schema is provided
            if let Some(items_schema) = &constraint.items_schema {
                for (index, item) in array_value.iter().enumerate() {
                    let item_context = context.with_array_index(index);
                    self.validate_field(item, items_schema, &item_context)?;
                }
            }

            Ok(())
        } else {
            Err(ValidationError::TypeMismatch {
                path: context.current_path.clone(),
                expected: "array".to_string(),
                actual: FieldType::from_value(value),
                context: context.error_context.clone(),
            })
        }
    }

    fn validate_object_constraint(
        &self,
        value: &Value,
        constraint: &ObjectConstraint,
        context: &ValidationContext,
    ) -> ValidationResult<()> {
        if let Some(object_value) = value.as_object() {
            let errors = constraint.validate(object_value);
            if !errors.is_empty() {
                return Err(ValidationError::ConstraintViolation {
                    path: context.current_path.clone(),
                    constraint: "object".to_string(),
                    message: errors.join("; "),
                    actual_value: value.clone(),
                    expected_value: None,
                    context: context.error_context.clone(),
                });
            }

            // Validate nested properties
            for (prop_name, prop_schema) in &constraint.properties {
                if let Some(prop_value) = object_value.get(prop_name) {
                    let prop_context = context.with_path(prop_name);
                    self.validate_field(prop_value, prop_schema, &prop_context)?;
                }
            }

            Ok(())
        } else {
            Err(ValidationError::TypeMismatch {
                path: context.current_path.clone(),
                expected: "object".to_string(),
                actual: FieldType::from_value(value),
                context: context.error_context.clone(),
            })
        }
    }

    fn can_coerce_type(&self, value: &Value, target_type: &FieldType) -> bool {
        if !self.options.allow_type_coercion {
            return false;
        }

        match (value, target_type) {
            // String to number coercion
            (Value::String(s), FieldType::Number) => s.parse::<f64>().is_ok(),
            (Value::String(s), FieldType::Integer) => s.parse::<i64>().is_ok(),
            
            // Number to string coercion
            (Value::Number(_), FieldType::String) => true,
            
            // Integer to number coercion (always valid)
            (Value::Number(n), FieldType::Number) if n.is_i64() => true,
            (Value::Number(n), FieldType::Integer) if n.is_f64() => {
                if let Some(f) = n.as_f64() {
                    f.fract() == 0.0
                } else {
                    false
                }
            },
            
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::builder::{SchemaBuilder, FieldSchemaBuilder};
    use crate::schema::format::Format;
    use serde_json::json;

    #[test]
    fn test_validate_valid_document() {
        let schema = SchemaBuilder::new("test_schema")
            .field("name", FieldSchemaBuilder::string().required().min_length(1).build())
            .field("age", FieldSchemaBuilder::integer().required().minimum(0.0).build())
            .field("email", FieldSchemaBuilder::string().required().email().build())
            .build();

        let document = json!({
            "name": "John Doe",
            "age": 30,
            "email": "john@example.com"
        });

        let engine = ValidationEngine::with_default_options();
        let result = engine.validate_document(&document, &schema, None);
        
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_missing_required_field() {
        let schema = SchemaBuilder::new("test_schema")
            .field("name", FieldSchemaBuilder::string().required().build())
            .field("age", FieldSchemaBuilder::integer().required().build())
            .build();

        let document = json!({
            "name": "John Doe"
            // Missing required field "age"
        });

        let engine = ValidationEngine::with_default_options();
        let result = engine.validate_document(&document, &schema, None);
        
        assert!(result.is_err());
        
        if let Err(ValidationError::RequiredFieldMissing { path, .. }) = result {
            assert_eq!(path, "age");
        } else {
            panic!("Expected RequiredFieldMissing error");
        }
    }

    #[test]
    fn test_validate_type_mismatch() {
        let schema = SchemaBuilder::new("test_schema")
            .field("age", FieldSchemaBuilder::integer().required().build())
            .build();

        let document = json!({
            "age": "not_a_number"
        });

        let engine = ValidationEngine::with_default_options();
        let result = engine.validate_document(&document, &schema, None);
        
        assert!(result.is_err());
        
        if let Err(ValidationError::TypeMismatch { expected, actual, .. }) = result {
            assert_eq!(expected, "integer");
            assert_eq!(actual, "string");
        } else {
            panic!("Expected TypeMismatch error");
        }
    }

    #[test]
    fn test_validate_constraint_violation() {
        let schema = SchemaBuilder::new("test_schema")
            .field("name", FieldSchemaBuilder::string().required().min_length(5).build())
            .build();

        let document = json!({
            "name": "Jo"  // Too short
        });

        let engine = ValidationEngine::with_default_options();
        let result = engine.validate_document(&document, &schema, None);
        
        assert!(result.is_err());
        
        if let Err(ValidationError::ConstraintViolation { .. }) = result {
            // Expected
        } else {
            panic!("Expected ConstraintViolation error");
        }
    }

    #[test]
    fn test_validate_array_with_items_schema() {
        let items_schema = FieldSchemaBuilder::string().min_length(1).build();
        let schema = SchemaBuilder::new("test_schema")
            .field("tags", FieldSchemaBuilder::array()
                .required()
                .min_items(1)
                .items_schema(items_schema)
                .build())
            .build();

        let document = json!({
            "tags": ["tag1", "tag2", "tag3"]
        });

        let engine = ValidationEngine::with_default_options();
        let result = engine.validate_document(&document, &schema, None);
        
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_nested_object() {
        let address_schema = FieldSchemaBuilder::object()
            .property("street", FieldSchemaBuilder::string().required().build())
            .property("city", FieldSchemaBuilder::string().required().build())
            .build();

        let schema = SchemaBuilder::new("test_schema")
            .field("name", FieldSchemaBuilder::string().required().build())
            .field("address", address_schema)
            .build();

        let document = json!({
            "name": "John Doe",
            "address": {
                "street": "123 Main St",
                "city": "Anytown"
            }
        });

        let engine = ValidationEngine::with_default_options();
        let result = engine.validate_document(&document, &schema, None);
        
        assert!(result.is_ok());
    }
} 