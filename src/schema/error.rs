use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;
use thiserror::Error;

/// Primary validation error type with comprehensive error information
#[derive(Error, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ValidationError {
    /// Type mismatch error
    #[error("Type validation failed at '{path}': expected {expected}, found {actual}")]
    TypeMismatch {
        path: String,
        expected: String,
        actual: String,
        context: ErrorContext,
    },

    /// Required field missing
    #[error("Required field missing: '{path}'")]
    RequiredFieldMissing {
        path: String,
        context: ErrorContext,
    },

    /// Format validation failed
    #[error("Format validation failed at '{path}': {message}")]
    FormatError {
        path: String,
        format: String,
        message: String,
        value: String,
        context: ErrorContext,
    },

    /// Constraint violation (min/max, length, etc.)
    #[error("Constraint violation at '{path}': {message}")]  
    ConstraintViolation {
        path: String,
        constraint: String,
        message: String,
        actual_value: Value,
        expected_value: Option<Value>,
        context: ErrorContext,
    },

    /// Pattern/regex validation failed
    #[error("Pattern validation failed at '{path}': value doesn't match pattern '{pattern}'")]
    PatternMismatch {
        path: String,
        pattern: String,
        value: String,
        context: ErrorContext,
    },

    /// Array validation errors
    #[error("Array validation failed at '{path}': {message}")]
    ArrayError {
        path: String,
        message: String,
        index: Option<usize>,
        context: ErrorContext,
    },

    /// Object validation errors  
    #[error("Object validation failed at '{path}': {message}")]
    ObjectError {
        path: String,
        message: String,
        field: Option<String>,
        context: ErrorContext,
    },

    /// Cross-field dependency errors
    #[error("Cross-field validation failed: {message}")]
    DependencyError {
        dependent_field: String,
        dependency_field: String,
        message: String,
        context: ErrorContext,
    },

    /// Conditional validation errors (if-then-else)
    #[error("Conditional validation failed at '{path}': {message}")]
    ConditionalError {
        path: String,
        condition: String,
        message: String,
        context: ErrorContext,
    },

    /// Custom validator errors
    #[error("Custom validation failed at '{path}': {message}")]
    CustomError {
        path: String,
        validator_name: String,
        message: String,
        context: ErrorContext,
    },

    /// Schema definition errors
    #[error("Schema definition error: {message}")]
    SchemaError {
        message: String,
        context: ErrorContext,
    },

    /// Multiple validation errors grouped together
    #[error("Multiple validation errors: {count} errors found")]
    MultipleErrors {
        errors: Vec<ValidationError>,
        count: usize,
    },
}

/// Additional context information for validation errors
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorContext {
    /// The schema that was being validated against
    pub schema_name: Option<String>,
    
    /// The collection being validated
    pub collection: Option<String>,
    
    /// Document ID if available
    pub document_id: Option<String>,
    
    /// Validation timestamp
    pub timestamp: chrono::DateTime<chrono::Utc>,
    
    /// Additional metadata
    pub metadata: std::collections::HashMap<String, Value>,
}

impl ErrorContext {
    pub fn new() -> Self {
        Self {
            schema_name: None,
            collection: None,
            document_id: None,
            timestamp: chrono::Utc::now(),
            metadata: std::collections::HashMap::new(),
        }
    }

    pub fn with_schema(mut self, schema_name: String) -> Self {
        self.schema_name = Some(schema_name);
        self
    }

    pub fn with_collection(mut self, collection: String) -> Self {
        self.collection = Some(collection);
        self
    }

    pub fn with_document_id(mut self, document_id: String) -> Self {
        self.document_id = Some(document_id);
        self
    }

    pub fn with_metadata(mut self, key: String, value: Value) -> Self {
        self.metadata.insert(key, value);
        self
    }
}

impl Default for ErrorContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Represents a validation error for a specific field
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldError {
    pub path: String,
    pub error: ValidationError,
}

impl FieldError {
    pub fn new(path: String, error: ValidationError) -> Self {
        Self { path, error }
    }
}

/// Result type for validation operations
pub type ValidationResult<T> = Result<T, ValidationError>;

/// Helper for collecting multiple validation errors
#[derive(Debug, Clone, Default)]
pub struct ValidationErrorCollector {
    errors: Vec<ValidationError>,
}

impl ValidationErrorCollector {
    pub fn new() -> Self {
        Self {
            errors: Vec::new(),
        }
    }

    pub fn add_error(&mut self, error: ValidationError) {
        self.errors.push(error);
    }

    pub fn add_type_mismatch(&mut self, path: &str, expected: &str, actual: &str, context: ErrorContext) {
        self.errors.push(ValidationError::TypeMismatch {
            path: path.to_string(),
            expected: expected.to_string(),
            actual: actual.to_string(),
            context,
        });
    }

    pub fn add_required_field_missing(&mut self, path: &str, context: ErrorContext) {
        self.errors.push(ValidationError::RequiredFieldMissing {
            path: path.to_string(),
            context,
        });
    }

    pub fn add_format_error(&mut self, path: &str, format: &str, message: &str, value: &str, context: ErrorContext) {
        self.errors.push(ValidationError::FormatError {
            path: path.to_string(),
            format: format.to_string(),
            message: message.to_string(),
            value: value.to_string(),
            context,
        });
    }

    pub fn add_constraint_violation(
        &mut self, 
        path: &str, 
        constraint: &str, 
        message: &str, 
        actual_value: Value,
        expected_value: Option<Value>,
        context: ErrorContext
    ) {
        self.errors.push(ValidationError::ConstraintViolation {
            path: path.to_string(),
            constraint: constraint.to_string(),
            message: message.to_string(),
            actual_value,
            expected_value,
            context,
        });
    }

    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    pub fn error_count(&self) -> usize {
        self.errors.len()
    }

    pub fn into_result<T>(self, success_value: T) -> ValidationResult<T> {
        if self.errors.is_empty() {
            Ok(success_value)
        } else if self.errors.len() == 1 {
            Err(self.errors.into_iter().next().unwrap())
        } else {
            let count = self.errors.len();
            Err(ValidationError::MultipleErrors {
                errors: self.errors,
                count,
            })
        }
    }

    pub fn into_errors(self) -> Vec<ValidationError> {
        self.errors
    }
}

/// Utility functions for error handling
impl ValidationError {
    /// Get the field path from any validation error
    pub fn get_path(&self) -> Option<&str> {
        match self {
            ValidationError::TypeMismatch { path, .. } => Some(path),
            ValidationError::RequiredFieldMissing { path, .. } => Some(path),
            ValidationError::FormatError { path, .. } => Some(path),
            ValidationError::ConstraintViolation { path, .. } => Some(path),
            ValidationError::PatternMismatch { path, .. } => Some(path),
            ValidationError::ArrayError { path, .. } => Some(path),
            ValidationError::ObjectError { path, .. } => Some(path),
            ValidationError::ConditionalError { path, .. } => Some(path),
            ValidationError::CustomError { path, .. } => Some(path),
            _ => None,
        }
    }

    /// Get the error context from any validation error
    pub fn get_context(&self) -> Option<&ErrorContext> {
        match self {
            ValidationError::TypeMismatch { context, .. } => Some(context),
            ValidationError::RequiredFieldMissing { context, .. } => Some(context),
            ValidationError::FormatError { context, .. } => Some(context),
            ValidationError::ConstraintViolation { context, .. } => Some(context),
            ValidationError::PatternMismatch { context, .. } => Some(context),
            ValidationError::ArrayError { context, .. } => Some(context),
            ValidationError::ObjectError { context, .. } => Some(context),
            ValidationError::DependencyError { context, .. } => Some(context),
            ValidationError::ConditionalError { context, .. } => Some(context),
            ValidationError::CustomError { context, .. } => Some(context),
            ValidationError::SchemaError { context, .. } => Some(context),
            _ => None,
        }
    }

    /// Check if this is a critical error that should stop processing
    pub fn is_critical(&self) -> bool {
        matches!(self, ValidationError::SchemaError { .. })
    }

    /// Get user-friendly error message with suggestions
    pub fn user_friendly_message(&self) -> String {
        match self {
            ValidationError::TypeMismatch { path, expected, actual, .. } => {
                format!("The field '{}' should be of type '{}', but received '{}'. Please check the data type.", path, expected, actual)
            }
            ValidationError::RequiredFieldMissing { path, .. } => {
                format!("The required field '{}' is missing. Please provide this field.", path)
            }
            ValidationError::FormatError { path, format, message, .. } => {
                format!("The field '{}' has an invalid {} format: {}. Please check the format requirements.", path, format, message)
            }
            ValidationError::ConstraintViolation { path, message, .. } => {
                format!("The field '{}' violates a constraint: {}. Please adjust the value.", path, message)
            }
            ValidationError::PatternMismatch { path, pattern, .. } => {
                format!("The field '{}' doesn't match the required pattern '{}'. Please check the format.", path, pattern)
            }
            _ => self.to_string(),
        }
    }
}

/// Custom Display implementation for pretty error formatting
impl fmt::Display for ErrorContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ValidationContext[")?;
        
        if let Some(schema) = &self.schema_name {
            write!(f, "schema:{}, ", schema)?;
        }
        
        if let Some(collection) = &self.collection {
            write!(f, "collection:{}, ", collection)?;
        }
        
        if let Some(doc_id) = &self.document_id {
            write!(f, "doc_id:{}, ", doc_id)?;
        }
        
        write!(f, "time:{}]", self.timestamp.format("%Y-%m-%d %H:%M:%S UTC"))
    }
} 