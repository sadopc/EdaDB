use regex::Regex;
use std::collections::HashMap;
use std::sync::Arc;
use std::str::FromStr;
use once_cell::sync::Lazy;
use crate::schema::error::{ValidationError, ErrorContext, ValidationResult};

/// Standard format types supported by the validation system
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum Format {
    Email,
    Url,
    Phone,
    Date,
    DateTime,
    Time,
    Uuid,
    CreditCard,
    Ipv4,
    Ipv6,
    Mac,
    Base64,
    Hex,
    Custom(String),
}

impl Format {
    pub fn as_str(&self) -> &str {
        match self {
            Format::Email => "email",
            Format::Url => "url", 
            Format::Phone => "phone",
            Format::Date => "date",
            Format::DateTime => "date-time",
            Format::Time => "time",
            Format::Uuid => "uuid",
            Format::CreditCard => "credit-card",
            Format::Ipv4 => "ipv4",
            Format::Ipv6 => "ipv6",
            Format::Mac => "mac",
            Format::Base64 => "base64",
            Format::Hex => "hex",
            Format::Custom(name) => name,
        }
    }
}

/// Trait for implementing custom format validators
pub trait FormatValidator: Send + Sync {
    fn validate(&self, value: &str, context: &ErrorContext) -> ValidationResult<()>;
    fn format_name(&self) -> &str;
    fn get_suggestion(&self, _value: &str) -> Option<String> {
        None
    }
}

/// Email format validator
pub struct EmailValidator;

impl FormatValidator for EmailValidator {
    fn validate(&self, value: &str, context: &ErrorContext) -> ValidationResult<()> {
        use std::str::FromStr;
        if email_address::EmailAddress::from_str(value).is_ok() {
            Ok(())
        } else {
            Err(ValidationError::FormatError {
                path: "".to_string(), // Path will be set by caller
                format: "email".to_string(),
                message: "Invalid email address format".to_string(),
                value: value.to_string(),
                context: context.clone(),
            })
        }
    }

    fn format_name(&self) -> &str {
        "email"
    }

    fn get_suggestion(&self, value: &str) -> Option<String> {
        if !value.contains('@') {
            Some("Email must contain '@' symbol".to_string())
        } else if !value.contains('.') {
            Some("Email must contain domain extension (e.g., .com)".to_string())
        } else {
            Some("Please check email format: example@domain.com".to_string())
        }
    }
}

/// URL format validator
pub struct UrlValidator;

impl FormatValidator for UrlValidator {
    fn validate(&self, value: &str, context: &ErrorContext) -> ValidationResult<()> {
        match url::Url::parse(value) {
            Ok(parsed_url) => {
                // Additional checks for valid URL
                if parsed_url.scheme().is_empty() {
                    Err(ValidationError::FormatError {
                        path: "".to_string(),
                        format: "url".to_string(),
                        message: "URL must have a scheme (http, https, etc.)".to_string(),
                        value: value.to_string(),
                        context: context.clone(),
                    })
                } else {
                    Ok(())
                }
            }
            Err(_) => Err(ValidationError::FormatError {
                path: "".to_string(),
                format: "url".to_string(),
                message: "Invalid URL format".to_string(),
                value: value.to_string(),
                context: context.clone(),
            })
        }
    }

    fn format_name(&self) -> &str {
        "url"
    }

    fn get_suggestion(&self, value: &str) -> Option<String> {
        if !value.starts_with("http://") && !value.starts_with("https://") {
            Some("URL should start with http:// or https://".to_string())
        } else {
            Some("Please check URL format: https://example.com".to_string())
        }
    }
}

/// Phone number format validator
pub struct PhoneValidator;

impl PhoneValidator {
    /// Basic regex patterns for common phone number formats
    fn is_basic_phone_format(&self, value: &str) -> bool {
        let patterns = [
            // E.164 format: +1234567890
            r"^\+[1-9]\d{1,14}$",
            // International with separators: +1-123-456-7890, +1 123 456 7890
            r"^\+[1-9][\d\s\-()]{1,18}\d$",
            // US format: (123) 456-7890, 123-456-7890, 123.456.7890
            r"^(\+1[\s\-]?)?\(?[2-9]\d{2}\)?[\s\-]?[2-9]\d{2}[\s\-]?\d{4}$",
            // Simple international: 1234567890 (10-15 digits)
            r"^\d{10,15}$",
        ];
        
        let cleaned = value.chars().filter(|c| c.is_ascii_digit() || *c == '+').collect::<String>();
        
        patterns.iter().any(|pattern| {
            if let Ok(regex) = regex::Regex::new(pattern) {
                regex.is_match(value) || regex.is_match(&cleaned)
            } else {
                false
            }
        })
    }
}

impl FormatValidator for PhoneValidator {
    fn validate(&self, value: &str, context: &ErrorContext) -> ValidationResult<()> {
        // First try basic format validation (most reliable)
        if self.is_basic_phone_format(value) {
            return Ok(());
        }
        
        // Try with phonenumber crate as fallback
        if let Ok(number) = phonenumber::parse(None, value) {
            if phonenumber::is_valid(&number) {
                return Ok(());
            }
        }
        
        // Final check: if it looks like a phone number (contains digits and allowed chars)
        let clean_value = value.chars()
            .filter(|c| c.is_ascii_digit() || "+-() ".contains(*c))
            .collect::<String>();
        
        let digit_count = clean_value.chars().filter(|c| c.is_ascii_digit()).count();
        
        if digit_count >= 7 && digit_count <= 15 && !clean_value.is_empty() {
            return Ok(());
        }
        
        Err(ValidationError::FormatError {
            path: "".to_string(),
            format: "phone".to_string(),
            message: "Invalid phone number format".to_string(),
            value: value.to_string(),
            context: context.clone(),
        })
    }

    fn format_name(&self) -> &str {
        "phone"
    }

    fn get_suggestion(&self, _value: &str) -> Option<String> {
        Some("Phone number formats: +1-234-567-8900, (123) 456-7890, +44 20 7946 0958".to_string())
    }
}

/// Date format validator (ISO 8601)
pub struct DateValidator;

impl FormatValidator for DateValidator {
    fn validate(&self, value: &str, context: &ErrorContext) -> ValidationResult<()> {
        if iso8601::date(value).is_ok() {
            Ok(())
        } else {
            Err(ValidationError::FormatError {
                path: "".to_string(),
                format: "date".to_string(),
                message: "Invalid date format, expected ISO 8601 (YYYY-MM-DD)".to_string(),
                value: value.to_string(),
                context: context.clone(),
            })
        }
    }

    fn format_name(&self) -> &str {
        "date"
    }

    fn get_suggestion(&self, _value: &str) -> Option<String> {
        Some("Date should be in ISO 8601 format: YYYY-MM-DD (e.g., 2024-01-15)".to_string())
    }
}

/// DateTime format validator (ISO 8601)
pub struct DateTimeValidator;

impl FormatValidator for DateTimeValidator {
    fn validate(&self, value: &str, context: &ErrorContext) -> ValidationResult<()> {
        if iso8601::datetime(value).is_ok() {
            Ok(())
        } else {
            Err(ValidationError::FormatError {
                path: "".to_string(),
                format: "date-time".to_string(),
                message: "Invalid datetime format, expected ISO 8601".to_string(),
                value: value.to_string(),
                context: context.clone(),
            })
        }
    }

    fn format_name(&self) -> &str {
        "date-time"
    }

    fn get_suggestion(&self, _value: &str) -> Option<String> {
        Some("DateTime should be in ISO 8601 format: YYYY-MM-DDTHH:mm:ssZ".to_string())
    }
}

/// UUID format validator
pub struct UuidValidator;

static UUID_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$").unwrap()
});

impl FormatValidator for UuidValidator {
    fn validate(&self, value: &str, context: &ErrorContext) -> ValidationResult<()> {
        if UUID_REGEX.is_match(&value.to_lowercase()) {
            Ok(())
        } else {
            Err(ValidationError::FormatError {
                path: "".to_string(),
                format: "uuid".to_string(),
                message: "Invalid UUID format".to_string(),
                value: value.to_string(),
                context: context.clone(),
            })
        }
    }

    fn format_name(&self) -> &str {
        "uuid"
    }

    fn get_suggestion(&self, _value: &str) -> Option<String> {
        Some("UUID should be in format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx".to_string())
    }
}

/// Credit card format validator (basic Luhn algorithm)
pub struct CreditCardValidator;

impl FormatValidator for CreditCardValidator {
    fn validate(&self, value: &str, context: &ErrorContext) -> ValidationResult<()> {
        let digits: String = value.chars().filter(|c| c.is_ascii_digit()).collect();
        
        if digits.len() < 13 || digits.len() > 19 {
            return Err(ValidationError::FormatError {
                path: "".to_string(),
                format: "credit-card".to_string(),
                message: "Credit card number must be 13-19 digits".to_string(),
                value: value.to_string(),
                context: context.clone(),
            });
        }

        // Luhn algorithm check
        if self.luhn_check(&digits) {
            Ok(())
        } else {
            Err(ValidationError::FormatError {
                path: "".to_string(),
                format: "credit-card".to_string(),
                message: "Invalid credit card number (failed checksum)".to_string(),
                value: value.to_string(),
                context: context.clone(),
            })
        }
    }

    fn format_name(&self) -> &str {
        "credit-card"
    }

    fn get_suggestion(&self, _value: &str) -> Option<String> {
        Some("Credit card number should be 13-19 digits, spaces and dashes are allowed".to_string())
    }
}

impl CreditCardValidator {
    fn luhn_check(&self, number: &str) -> bool {
        let digits: Vec<u32> = number.chars()
            .filter_map(|c| c.to_digit(10))
            .collect();
        
        if digits.is_empty() {
            return false;
        }

        let mut sum = 0;
        let mut is_even = false;

        for &digit in digits.iter().rev() {
            let mut d = digit;
            if is_even {
                d *= 2;
                if d > 9 {
                    d -= 9;
                }
            }
            sum += d;
            is_even = !is_even;
        }

        sum % 10 == 0
    }
}

/// Password strength validator
pub struct PasswordValidator {
    min_length: usize,
    require_uppercase: bool,
    require_lowercase: bool,
    require_digit: bool,
    require_special: bool,
}

impl PasswordValidator {
    pub fn new() -> Self {
        Self {
            min_length: 8,
            require_uppercase: true,
            require_lowercase: true,
            require_digit: true,
            require_special: true,
        }
    }

    pub fn with_min_length(mut self, length: usize) -> Self {
        self.min_length = length;
        self
    }

    pub fn require_uppercase(mut self, required: bool) -> Self {
        self.require_uppercase = required;
        self
    }

    pub fn require_lowercase(mut self, required: bool) -> Self {
        self.require_lowercase = required;
        self
    }

    pub fn require_digit(mut self, required: bool) -> Self {
        self.require_digit = required;
        self
    }

    pub fn require_special_char(mut self, required: bool) -> Self {
        self.require_special = required;
        self
    }
}

impl FormatValidator for PasswordValidator {
    fn validate(&self, value: &str, context: &ErrorContext) -> ValidationResult<()> {
        let mut errors = Vec::new();

        if value.len() < self.min_length {
            errors.push(format!("Must be at least {} characters long", self.min_length));
        }

        if self.require_uppercase && !value.chars().any(|c| c.is_uppercase()) {
            errors.push("Must contain at least one uppercase letter".to_string());
        }

        if self.require_lowercase && !value.chars().any(|c| c.is_lowercase()) {
            errors.push("Must contain at least one lowercase letter".to_string());
        }

        if self.require_digit && !value.chars().any(|c| c.is_ascii_digit()) {
            errors.push("Must contain at least one digit".to_string());
        }

        if self.require_special && !value.chars().any(|c| "!@#$%^&*()_+-=[]{}|;:,.<>?".contains(c)) {
            errors.push("Must contain at least one special character".to_string());
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(ValidationError::FormatError {
                path: "".to_string(),
                format: "password".to_string(),
                message: errors.join("; "),
                value: "<redacted>".to_string(), // Don't log actual passwords
                context: context.clone(),
            })
        }
    }

    fn format_name(&self) -> &str {
        "password"
    }

    fn get_suggestion(&self, _value: &str) -> Option<String> {
        Some(format!(
            "Password must be at least {} characters with {}{}{}{}",
            self.min_length,
            if self.require_uppercase { "uppercase, " } else { "" },
            if self.require_lowercase { "lowercase, " } else { "" },
            if self.require_digit { "digits, " } else { "" },
            if self.require_special { "special characters" } else { "" }
        ))
    }
}

/// IPv4 address validator
pub struct Ipv4Validator;

static IPV4_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^((25[0-5]|(2[0-4]|1\d|[1-9]|)\d)\.?\b){4}$").unwrap()
});

impl FormatValidator for Ipv4Validator {
    fn validate(&self, value: &str, context: &ErrorContext) -> ValidationResult<()> {
        if IPV4_REGEX.is_match(value) {
            Ok(())
        } else {
            Err(ValidationError::FormatError {
                path: "".to_string(),
                format: "ipv4".to_string(),
                message: "Invalid IPv4 address format".to_string(),
                value: value.to_string(),
                context: context.clone(),
            })
        }
    }

    fn format_name(&self) -> &str {
        "ipv4"
    }

    fn get_suggestion(&self, _value: &str) -> Option<String> {
        Some("IPv4 address should be in format: xxx.xxx.xxx.xxx (e.g., 192.168.1.1)".to_string())
    }
}

/// Custom format validator for user-defined formats
pub struct CustomFormatValidator {
    name: String,
    regex: Regex,
    message: String,
    suggestion: Option<String>,
}

impl CustomFormatValidator {
    pub fn new(name: String, pattern: &str, message: String) -> Result<Self, regex::Error> {
        Ok(Self {
            name,
            regex: Regex::new(pattern)?,
            message,
            suggestion: None,
        })
    }

    pub fn with_suggestion(mut self, suggestion: String) -> Self {
        self.suggestion = Some(suggestion);
        self
    }
}

impl FormatValidator for CustomFormatValidator {
    fn validate(&self, value: &str, context: &ErrorContext) -> ValidationResult<()> {
        if self.regex.is_match(value) {
            Ok(())
        } else {
            Err(ValidationError::FormatError {
                path: "".to_string(),
                format: self.name.clone(),
                message: self.message.clone(),
                value: value.to_string(),
                context: context.clone(),
            })
        }
    }

    fn format_name(&self) -> &str {
        &self.name
    }

    fn get_suggestion(&self, _value: &str) -> Option<String> {
        self.suggestion.clone()
    }
}

/// Format validator registry for managing all validators
pub struct FormatValidatorRegistry {
    validators: HashMap<String, Arc<dyn FormatValidator>>,
}

impl FormatValidatorRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            validators: HashMap::new(),
        };

        // Register built-in validators
        registry.register_validator(Arc::new(EmailValidator));
        registry.register_validator(Arc::new(UrlValidator));
        registry.register_validator(Arc::new(PhoneValidator));
        registry.register_validator(Arc::new(DateValidator));
        registry.register_validator(Arc::new(DateTimeValidator));
        registry.register_validator(Arc::new(UuidValidator));
        registry.register_validator(Arc::new(CreditCardValidator));
        registry.register_validator(Arc::new(Ipv4Validator));
        registry.register_validator(Arc::new(PasswordValidator::new()));

        registry
    }

    pub fn register_validator(&mut self, validator: Arc<dyn FormatValidator>) {
        self.validators.insert(validator.format_name().to_string(), validator);
    }

    pub fn get_validator(&self, format: &str) -> Option<Arc<dyn FormatValidator>> {
        self.validators.get(format).cloned()
    }

    pub fn validate_format(&self, format: &str, value: &str, context: &ErrorContext) -> ValidationResult<()> {
        if let Some(validator) = self.get_validator(format) {
            validator.validate(value, context)
        } else {
            Err(ValidationError::FormatError {
                path: "".to_string(),
                format: format.to_string(),
                message: format!("Unknown format validator: {}", format),
                value: value.to_string(),
                context: context.clone(),
            })
        }
    }

    pub fn list_formats(&self) -> Vec<&String> {
        self.validators.keys().collect()
    }
}

impl Default for FormatValidatorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Global format validator registry
static GLOBAL_FORMAT_REGISTRY: Lazy<std::sync::RwLock<FormatValidatorRegistry>> = 
    Lazy::new(|| std::sync::RwLock::new(FormatValidatorRegistry::new()));

/// Convenience functions for global format validation
pub fn validate_format(format: &str, value: &str, context: &ErrorContext) -> ValidationResult<()> {
    GLOBAL_FORMAT_REGISTRY
        .read()
        .unwrap()
        .validate_format(format, value, context)
}

pub fn register_custom_format_validator(validator: Arc<dyn FormatValidator>) {
    GLOBAL_FORMAT_REGISTRY
        .write()
        .unwrap()
        .register_validator(validator);
}

pub fn list_available_formats() -> Vec<String> {
    GLOBAL_FORMAT_REGISTRY
        .read()
        .unwrap()
        .list_formats()
        .into_iter()
        .cloned()
        .collect()
} 