//! # JSON Schema Validation System
//! 
//! Enterprise-grade JSON Schema validation system for NoSQL database.
//! Supports JSON Schema Draft 7+ features with Rust-native performance.
//!
//! ## Features
//! - Type validation (string, number, boolean, array, object, null)
//! - Format validation (email, URL, phone, date, UUID)
//! - Constraint validation (min/max, length, pattern)
//! - Nested object and array validation
//! - Cross-field dependencies and conditional validation
//! - Collection-level schema management
//! - Comprehensive error reporting with field paths
//! - Thread-safe concurrent validation
//! - Performance-optimized validation engine

pub mod error;
pub mod definition;
pub mod format;
pub mod builder;
pub mod validation;
pub mod registry;

// Public API exports
pub use error::{
    ValidationError, ValidationResult, FieldError, ErrorContext
};

pub use definition::{
    SchemaDefinition, FieldSchema, FieldType, Constraint, 
    NumericConstraint, StringConstraint, ArrayConstraint, ObjectConstraint
};

pub use format::{
    FormatValidator, Format, EmailValidator, UrlValidator, 
    PhoneValidator, DateValidator, UuidValidator, CreditCardValidator,
    PasswordValidator, CustomFormatValidator
};

pub use builder::{
    SchemaBuilder, FieldSchemaBuilder
};

pub use validation::{
    ValidationEngine, ValidationContext, ValidationOptions
};

pub use registry::{
    SchemaRegistry, CollectionSchema, SchemaConfig
};

// Convenience re-exports
pub type ValidationErrors = Vec<ValidationError>; 