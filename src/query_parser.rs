// query_parser.rs - SQL-like query language parser with tokenizer and recursive descent parser

use crate::ast::*;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use thiserror::Error;

/// Parser error types for comprehensive error handling
#[derive(Error, Debug, Clone, PartialEq)]
pub enum ParserError {
    #[error("Unexpected token: expected {expected}, found {found} at position {position}")]
    UnexpectedToken {
        expected: String,
        found: String,
        position: usize,
    },
    #[error("Unexpected end of input, expected {expected}")]
    UnexpectedEOF { expected: String },
    #[error("Invalid syntax: {message} at position {position}")]
    InvalidSyntax { message: String, position: usize },
    #[error("Invalid number format: {value} at position {position}")]
    InvalidNumber { value: String, position: usize },
    #[error("Invalid string literal: {message} at position {position}")]
    InvalidString { message: String, position: usize },
    #[error("Unknown identifier: {identifier} at position {position}")]
    UnknownIdentifier { identifier: String, position: usize },
    #[error("Lexer error: {message} at position {position}")]
    LexerError { message: String, position: usize },
}

/// Token types for the lexer
#[derive(Debug, Clone, PartialEq)]
pub enum TokenType {
    // Keywords
    Select,
    Insert,
    Update,
    Delete,
    Create,
    From,
    Into,
    Set,
    Where,
    And,
    Or,
    Not,
    OrderBy,
    GroupBy,
    Having,
    Limit,
    Offset,
    As,
    Collection,
    Values,
    In,
    Like,
    Is,
    Null,
    True,
    False,
    Asc,
    Desc,

    // Operators
    Equal,              // =
    NotEqual,           // !=, <>
    GreaterThan,        // >
    GreaterThanOrEqual, // >=
    LessThan,           // <
    LessThanOrEqual,    // <=
    Plus,               // +
    Minus,              // -
    Multiply,           // *
    Divide,             // /
    Modulo,             // %

    // Punctuation
    LeftParen,    // (
    RightParen,   // )
    Comma,        // ,
    Semicolon,    // ;
    Dot,          // .

    // Literals
    String(String),
    Number(f64),
    Identifier(String),

    // Special
    EOF,
}

/// Token with position information
#[derive(Debug, Clone)]
pub struct Token {
    pub token_type: TokenType,
    pub position: usize,
}

impl Token {
    pub fn new(token_type: TokenType, position: usize) -> Self {
        Self { token_type, position }
    }
}

/// Lexer/Tokenizer for SQL-like queries
pub struct Lexer {
    input: Vec<char>,
    position: usize,
    current_char: Option<char>,
}

impl Lexer {
    /// Creates a new lexer instance
    pub fn new(input: &str) -> Self {
        let chars: Vec<char> = input.chars().collect();
        let current_char = chars.get(0).copied();
        
        Self {
            input: chars,
            position: 0,
            current_char,
        }
    }

    /// Advances to the next character
    fn advance(&mut self) {
        self.position += 1;
        self.current_char = self.input.get(self.position).copied();
    }

    /// Peeks at the next character without advancing
    fn peek(&self) -> Option<char> {
        self.input.get(self.position + 1).copied()
    }

    /// Skips whitespace characters
    fn skip_whitespace(&mut self) {
        while let Some(ch) = self.current_char {
            if ch.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }

    /// Reads a string literal (quoted string)
    fn read_string(&mut self) -> Result<String, ParserError> {
        let start_pos = self.position;
        let quote_char = self.current_char.unwrap(); // Either ' or "
        self.advance(); // Skip opening quote

        let mut value = String::new();
        let mut escaped = false;

        while let Some(ch) = self.current_char {
            if escaped {
                match ch {
                    'n' => value.push('\n'),
                    't' => value.push('\t'),
                    'r' => value.push('\r'),
                    '\\' => value.push('\\'),
                    '\'' => value.push('\''),
                    '"' => value.push('"'),
                    _ => {
                        value.push('\\');
                        value.push(ch);
                    }
                }
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == quote_char {
                self.advance(); // Skip closing quote
                return Ok(value);
            } else {
                value.push(ch);
            }
            self.advance();
        }

        Err(ParserError::InvalidString {
            message: "Unterminated string literal".to_string(),
            position: start_pos,
        })
    }

    /// Reads a number (integer or float)
    fn read_number(&mut self) -> Result<f64, ParserError> {
        let start_pos = self.position;
        let mut value = String::new();

        // Handle negative numbers
        if self.current_char == Some('-') {
            value.push('-');
            self.advance();
        }

        // Read integer part
        while let Some(ch) = self.current_char {
            if ch.is_ascii_digit() {
                value.push(ch);
                self.advance();
            } else {
                break;
            }
        }

        // Read decimal part
        if self.current_char == Some('.') && self.peek().map_or(false, |c| c.is_ascii_digit()) {
            value.push('.');
            self.advance();

            while let Some(ch) = self.current_char {
                if ch.is_ascii_digit() {
                    value.push(ch);
                    self.advance();
                } else {
                    break;
                }
            }
        }

        value.parse::<f64>().map_err(|_| ParserError::InvalidNumber {
            value: value.clone(),
            position: start_pos,
        })
    }

    /// Reads an identifier or keyword
    fn read_identifier(&mut self) -> String {
        let mut value = String::new();

        while let Some(ch) = self.current_char {
            if ch.is_alphanumeric() || ch == '_' {
                value.push(ch);
                self.advance();
            } else {
                break;
            }
        }

        value
    }

    /// Converts string to keyword token type or returns identifier
    fn keyword_or_identifier(&self, value: &str) -> TokenType {
        match value.to_uppercase().as_str() {
            "SELECT" => TokenType::Select,
            "INSERT" => TokenType::Insert,
            "UPDATE" => TokenType::Update,
            "DELETE" => TokenType::Delete,
            "CREATE" => TokenType::Create,
            "FROM" => TokenType::From,
            "INTO" => TokenType::Into,
            "SET" => TokenType::Set,
            "WHERE" => TokenType::Where,
            "AND" => TokenType::And,
            "OR" => TokenType::Or,
            "NOT" => TokenType::Not,
            "ORDER" => TokenType::OrderBy,
            "GROUP" => TokenType::GroupBy,
            "HAVING" => TokenType::Having,
            "LIMIT" => TokenType::Limit,
            "OFFSET" => TokenType::Offset,
            "AS" => TokenType::As,
            "COLLECTION" => TokenType::Collection,
            "VALUES" => TokenType::Values,
            "IN" => TokenType::In,
            "LIKE" => TokenType::Like,
            "IS" => TokenType::Is,
            "NULL" => TokenType::Null,
            "TRUE" => TokenType::True,
            "FALSE" => TokenType::False,
            "ASC" => TokenType::Asc,
            "DESC" => TokenType::Desc,
            "BY" => TokenType::OrderBy, // Handle "ORDER BY" and "GROUP BY"
            _ => TokenType::Identifier(value.to_string()),
        }
    }

    /// Tokenizes the input and returns all tokens
    pub fn tokenize(&mut self) -> Result<Vec<Token>, ParserError> {
        let mut tokens = Vec::new();

        while let Some(ch) = self.current_char {
            let token_pos = self.position;

            match ch {
                // Skip whitespace
                c if c.is_whitespace() => {
                    self.skip_whitespace();
                    continue;
                }

                // String literals
                '"' | '\'' => {
                    let value = self.read_string()?;
                    tokens.push(Token::new(TokenType::String(value), token_pos));
                }

                // Numbers
                c if c.is_ascii_digit() => {
                    let value = self.read_number()?;
                    tokens.push(Token::new(TokenType::Number(value), token_pos));
                }

                // Negative numbers
                '-' if self.peek().map_or(false, |c| c.is_ascii_digit()) => {
                    let value = self.read_number()?;
                    tokens.push(Token::new(TokenType::Number(value), token_pos));
                }

                // Identifiers and keywords
                c if c.is_alphabetic() || c == '_' => {
                    let value = self.read_identifier();
                    let token_type = self.keyword_or_identifier(&value);
                    tokens.push(Token::new(token_type, token_pos));
                }

                // Operators and punctuation
                '=' => {
                    tokens.push(Token::new(TokenType::Equal, token_pos));
                    self.advance();
                }
                '!' => {
                    if self.peek() == Some('=') {
                        tokens.push(Token::new(TokenType::NotEqual, token_pos));
                        self.advance();
                        self.advance();
                    } else {
                        return Err(ParserError::LexerError {
                            message: format!("Unexpected character: {}", ch),
                            position: token_pos,
                        });
                    }
                }
                '<' => {
                    if self.peek() == Some('=') {
                        tokens.push(Token::new(TokenType::LessThanOrEqual, token_pos));
                        self.advance();
                        self.advance();
                    } else if self.peek() == Some('>') {
                        tokens.push(Token::new(TokenType::NotEqual, token_pos));
                        self.advance();
                        self.advance();
                    } else {
                        tokens.push(Token::new(TokenType::LessThan, token_pos));
                        self.advance();
                    }
                }
                '>' => {
                    if self.peek() == Some('=') {
                        tokens.push(Token::new(TokenType::GreaterThanOrEqual, token_pos));
                        self.advance();
                        self.advance();
                    } else {
                        tokens.push(Token::new(TokenType::GreaterThan, token_pos));
                        self.advance();
                    }
                }
                '+' => {
                    tokens.push(Token::new(TokenType::Plus, token_pos));
                    self.advance();
                }
                '-' => {
                    tokens.push(Token::new(TokenType::Minus, token_pos));
                    self.advance();
                }
                '*' => {
                    tokens.push(Token::new(TokenType::Multiply, token_pos));
                    self.advance();
                }
                '/' => {
                    tokens.push(Token::new(TokenType::Divide, token_pos));
                    self.advance();
                }
                '%' => {
                    tokens.push(Token::new(TokenType::Modulo, token_pos));
                    self.advance();
                }
                '(' => {
                    tokens.push(Token::new(TokenType::LeftParen, token_pos));
                    self.advance();
                }
                ')' => {
                    tokens.push(Token::new(TokenType::RightParen, token_pos));
                    self.advance();
                }
                ',' => {
                    tokens.push(Token::new(TokenType::Comma, token_pos));
                    self.advance();
                }
                ';' => {
                    tokens.push(Token::new(TokenType::Semicolon, token_pos));
                    self.advance();
                }
                '.' => {
                    tokens.push(Token::new(TokenType::Dot, token_pos));
                    self.advance();
                }

                // Unknown character
                _ => {
                    return Err(ParserError::LexerError {
                        message: format!("Unexpected character: {}", ch),
                        position: token_pos,
                    });
                }
            }
        }

        tokens.push(Token::new(TokenType::EOF, self.position));
        Ok(tokens)
    }
}

/// Recursive descent parser for SQL-like queries
pub struct Parser {
    tokens: Vec<Token>,
    position: usize,
    eof_token: Token,
}

impl Parser {
    /// Creates a new parser instance
    pub fn new(tokens: Vec<Token>) -> Self {
        Self { 
            tokens, 
            position: 0,
            eof_token: Token::new(TokenType::EOF, 0)
        }
    }

    /// Returns the current token
    fn current_token(&self) -> &Token {
        self.tokens.get(self.position).unwrap_or(&self.eof_token)
    }

    /// Advances to the next token
    fn advance(&mut self) {
        if self.position < self.tokens.len() - 1 {
            self.position += 1;
        }
    }

    /// Checks if current token matches expected type
    fn match_token(&self, expected: &TokenType) -> bool {
        std::mem::discriminant(&self.current_token().token_type) == std::mem::discriminant(expected)
    }

    /// Consumes expected token or returns error
    fn expect_token(&mut self, expected: TokenType) -> Result<Token, ParserError> {
        let current = self.current_token().clone();
        
        if self.match_token(&expected) {
            let token = current;
            self.advance();
            Ok(token)
        } else {
            Err(ParserError::UnexpectedToken {
                expected: format!("{:?}", expected),
                found: format!("{:?}", current.token_type),
                position: current.position,
            })
        }
    }

    /// Parses the main query
    pub fn parse(&mut self) -> Result<Query, ParserError> {
        match &self.current_token().token_type {
            TokenType::Select => self.parse_select(),
            TokenType::Insert => self.parse_insert(),
            TokenType::Update => self.parse_update(),
            TokenType::Delete => self.parse_delete(),
            TokenType::Create => self.parse_create(),
            TokenType::EOF => Err(ParserError::UnexpectedEOF {
                expected: "SQL query".to_string(),
            }),
            _ => Err(ParserError::UnexpectedToken {
                expected: "SELECT, INSERT, UPDATE, DELETE, or CREATE".to_string(),
                found: format!("{:?}", self.current_token().token_type),
                position: self.current_token().position,
            }),
        }
    }

    /// Parses SELECT query
    fn parse_select(&mut self) -> Result<Query, ParserError> {
        self.expect_token(TokenType::Select)?;

        let fields = self.parse_field_list()?;
        
        self.expect_token(TokenType::From)?;
        let from = self.parse_identifier()?;

        let mut query = SelectQuery::new(fields, from);

        // Optional WHERE clause
        if self.match_token(&TokenType::Where) {
            self.advance();
            let condition = self.parse_condition()?;
            query = query.with_where(condition);
        }

        // Optional ORDER BY clause
        if self.match_token(&TokenType::OrderBy) {
            self.advance();
            // Handle "ORDER BY" as two separate tokens
            if self.match_token(&TokenType::OrderBy) {
                // Already consumed ORDER, now expect BY
                self.advance();
            }
            let order_by = self.parse_order_by_list()?;
            query = query.with_order_by(order_by);
        }

        // Optional LIMIT clause
        let mut has_limit = false;
        if self.match_token(&TokenType::Limit) {
            self.advance();
            let limit = self.parse_number()? as usize;
            query = query.with_limit(limit);
            has_limit = true;
        }

        // Optional OFFSET clause (only allowed with LIMIT)
        if self.match_token(&TokenType::Offset) {
            if !has_limit {
                return Err(ParserError::InvalidSyntax {
                    message: "OFFSET can only be used with LIMIT".to_string(),
                    position: self.current_token().position,
                });
            }
            self.advance();
            let offset = self.parse_number()? as usize;
            query = query.with_offset(offset);
        }

        Ok(Query::Select(query))
    }

    /// Parses INSERT query
    fn parse_insert(&mut self) -> Result<Query, ParserError> {
        self.expect_token(TokenType::Insert)?;
        self.expect_token(TokenType::Into)?;
        
        let into = self.parse_identifier()?;
        
        self.expect_token(TokenType::LeftParen)?;
        let fields = self.parse_identifier_list()?;
        self.expect_token(TokenType::RightParen)?;
        
        self.expect_token(TokenType::Values)?;
        self.expect_token(TokenType::LeftParen)?;
        let values = self.parse_value_list()?;
        self.expect_token(TokenType::RightParen)?;

        Ok(Query::Insert(InsertQuery::new(into, fields, values)))
    }

    /// Parses UPDATE query
    fn parse_update(&mut self) -> Result<Query, ParserError> {
        self.expect_token(TokenType::Update)?;
        
        let table = self.parse_identifier()?;
        
        self.expect_token(TokenType::Set)?;
        let assignments = self.parse_assignment_list()?;
        
        let mut query = UpdateQuery::new(table, assignments);

        // Optional WHERE clause
        if self.match_token(&TokenType::Where) {
            self.advance();
            let condition = self.parse_condition()?;
            query = query.with_where(condition);
        }

        Ok(Query::Update(query))
    }

    /// Parses DELETE query
    fn parse_delete(&mut self) -> Result<Query, ParserError> {
        self.expect_token(TokenType::Delete)?;
        self.expect_token(TokenType::From)?;
        
        let from = self.parse_identifier()?;
        let mut query = DeleteQuery::new(from);

        // Optional WHERE clause
        if self.match_token(&TokenType::Where) {
            self.advance();
            let condition = self.parse_condition()?;
            query = query.with_where(condition);
        }

        Ok(Query::Delete(query))
    }

    /// Parses CREATE query
    fn parse_create(&mut self) -> Result<Query, ParserError> {
        self.expect_token(TokenType::Create)?;
        self.expect_token(TokenType::Collection)?;
        
        let collection_name = self.parse_identifier()?;
        
        Ok(Query::Create(CreateQuery::new(collection_name)))
    }

    /// Parses field list for SELECT
    fn parse_field_list(&mut self) -> Result<Vec<Field>, ParserError> {
        let mut fields = Vec::new();

        // Handle SELECT *
        if self.match_token(&TokenType::Multiply) {
            self.advance();
            fields.push(Field::All);
            return Ok(fields);
        }

        // Parse first field
        fields.push(self.parse_field()?);

        // Parse additional fields
        while self.match_token(&TokenType::Comma) {
            self.advance();
            fields.push(self.parse_field()?);
        }

        Ok(fields)
    }

    /// Parses a single field
    fn parse_field(&mut self) -> Result<Field, ParserError> {
        let field_name = self.parse_identifier()?;

        // Check for alias
        if self.match_token(&TokenType::As) {
            self.advance();
            let alias = self.parse_identifier()?;
            Ok(Field::aliased(field_name, alias))
        } else {
            Ok(Field::named(field_name))
        }
    }

    /// Parses identifier list
    fn parse_identifier_list(&mut self) -> Result<Vec<String>, ParserError> {
        let mut identifiers = Vec::new();

        identifiers.push(self.parse_identifier()?);

        while self.match_token(&TokenType::Comma) {
            self.advance();
            identifiers.push(self.parse_identifier()?);
        }

        Ok(identifiers)
    }

    /// Parses value list
    fn parse_value_list(&mut self) -> Result<Vec<Value>, ParserError> {
        let mut values = Vec::new();

        values.push(self.parse_value()?);

        while self.match_token(&TokenType::Comma) {
            self.advance();
            values.push(self.parse_value()?);
        }

        Ok(values)
    }

    /// Parses assignment list for UPDATE SET
    fn parse_assignment_list(&mut self) -> Result<Vec<Assignment>, ParserError> {
        let mut assignments = Vec::new();

        assignments.push(self.parse_assignment()?);

        while self.match_token(&TokenType::Comma) {
            self.advance();
            assignments.push(self.parse_assignment()?);
        }

        Ok(assignments)
    }

    /// Parses a single assignment
    fn parse_assignment(&mut self) -> Result<Assignment, ParserError> {
        let field = self.parse_identifier()?;
        self.expect_token(TokenType::Equal)?;
        let value = self.parse_value()?;

        Ok(Assignment::new(field, value))
    }

    /// Parses ORDER BY list
    fn parse_order_by_list(&mut self) -> Result<Vec<OrderBy>, ParserError> {
        let mut order_by = Vec::new();

        order_by.push(self.parse_order_by()?);

        while self.match_token(&TokenType::Comma) {
            self.advance();
            order_by.push(self.parse_order_by()?);
        }

        Ok(order_by)
    }

    /// Parses a single ORDER BY clause
    fn parse_order_by(&mut self) -> Result<OrderBy, ParserError> {
        let field = self.parse_identifier()?;

        let direction = if self.match_token(&TokenType::Asc) {
            self.advance();
            SortDirection::Asc
        } else if self.match_token(&TokenType::Desc) {
            self.advance();
            SortDirection::Desc
        } else {
            SortDirection::Asc // Default
        };

        Ok(OrderBy::new(field, direction))
    }

    /// Parses WHERE condition with logical operators
    fn parse_condition(&mut self) -> Result<Condition, ParserError> {
        self.parse_or_condition()
    }

    /// Parses OR condition
    fn parse_or_condition(&mut self) -> Result<Condition, ParserError> {
        let mut left = self.parse_and_condition()?;

        while self.match_token(&TokenType::Or) {
            self.advance();
            let right = self.parse_and_condition()?;
            left = Condition::or(left, right);
        }

        Ok(left)
    }

    /// Parses AND condition
    fn parse_and_condition(&mut self) -> Result<Condition, ParserError> {
        let mut left = self.parse_not_condition()?;

        while self.match_token(&TokenType::And) {
            self.advance();
            let right = self.parse_not_condition()?;
            left = Condition::and(left, right);
        }

        Ok(left)
    }

    /// Parses NOT condition
    fn parse_not_condition(&mut self) -> Result<Condition, ParserError> {
        if self.match_token(&TokenType::Not) {
            self.advance();
            let condition = self.parse_primary_condition()?;
            Ok(Condition::not(condition))
        } else {
            self.parse_primary_condition()
        }
    }

    /// Parses primary condition (comparison or parenthesized)
    fn parse_primary_condition(&mut self) -> Result<Condition, ParserError> {
        if self.match_token(&TokenType::LeftParen) {
            self.advance();
            let condition = self.parse_condition()?;
            self.expect_token(TokenType::RightParen)?;
            Ok(Condition::parenthesized(condition))
        } else {
            self.parse_comparison_condition()
        }
    }

    /// Parses comparison condition
    fn parse_comparison_condition(&mut self) -> Result<Condition, ParserError> {
        let field = self.parse_identifier()?;

        let operator = match &self.current_token().token_type {
            TokenType::Equal => {
                self.advance();
                ComparisonOperator::Equal
            }
            TokenType::NotEqual => {
                self.advance();
                ComparisonOperator::NotEqual
            }
            TokenType::GreaterThan => {
                self.advance();
                ComparisonOperator::GreaterThan
            }
            TokenType::GreaterThanOrEqual => {
                self.advance();
                ComparisonOperator::GreaterThanOrEqual
            }
            TokenType::LessThan => {
                self.advance();
                ComparisonOperator::LessThan
            }
            TokenType::LessThanOrEqual => {
                self.advance();
                ComparisonOperator::LessThanOrEqual
            }
            TokenType::Like => {
                self.advance();
                ComparisonOperator::Like
            }
            TokenType::In => {
                self.advance();
                ComparisonOperator::In
            }
            TokenType::Is => {
                self.advance();
                if self.match_token(&TokenType::Not) {
                    self.advance();
                    if self.match_token(&TokenType::Null) {
                        self.advance();
                        return Ok(Condition::comparison(field, ComparisonOperator::IsNotNull, Value::Null));
                    } else {
                        return Err(ParserError::UnexpectedToken {
                            expected: "NULL".to_string(),
                            found: format!("{:?}", self.current_token().token_type),
                            position: self.current_token().position,
                        });
                    }
                } else if self.match_token(&TokenType::Null) {
                    self.advance();
                    return Ok(Condition::comparison(field, ComparisonOperator::IsNull, Value::Null));
                } else {
                    return Err(ParserError::UnexpectedToken {
                        expected: "NULL or NOT NULL".to_string(),
                        found: format!("{:?}", self.current_token().token_type),
                        position: self.current_token().position,
                    });
                }
            }
            _ => {
                return Err(ParserError::UnexpectedToken {
                    expected: "comparison operator".to_string(),
                    found: format!("{:?}", self.current_token().token_type),
                    position: self.current_token().position,
                });
            }
        };

        let value = if operator == ComparisonOperator::In {
            // Handle IN (value1, value2, ...)
            self.expect_token(TokenType::LeftParen)?;
            let values = self.parse_value_list()?;
            self.expect_token(TokenType::RightParen)?;
            Value::Array(values)
        } else {
            self.parse_value()?
        };

        Ok(Condition::comparison(field, operator, value))
    }

    /// Parses a value (string, number, boolean, null)
    fn parse_value(&mut self) -> Result<Value, ParserError> {
        match &self.current_token().token_type {
            TokenType::String(s) => {
                let value = Value::String(s.clone());
                self.advance();
                Ok(value)
            }
            TokenType::Number(n) => {
                let value = Value::Number(serde_json::Number::from_f64(*n).unwrap());
                self.advance();
                Ok(value)
            }
            TokenType::True => {
                self.advance();
                Ok(Value::Bool(true))
            }
            TokenType::False => {
                self.advance();
                Ok(Value::Bool(false))
            }
            TokenType::Null => {
                self.advance();
                Ok(Value::Null)
            }
            _ => Err(ParserError::UnexpectedToken {
                expected: "value (string, number, boolean, or null)".to_string(),
                found: format!("{:?}", self.current_token().token_type),
                position: self.current_token().position,
            }),
        }
    }

    /// Parses an identifier
    fn parse_identifier(&mut self) -> Result<String, ParserError> {
        match &self.current_token().token_type {
            TokenType::Identifier(name) => {
                let value = name.clone();
                self.advance();
                Ok(value)
            }
            _ => Err(ParserError::UnexpectedToken {
                expected: "identifier".to_string(),
                found: format!("{:?}", self.current_token().token_type),
                position: self.current_token().position,
            }),
        }
    }

    /// Parses a number
    fn parse_number(&mut self) -> Result<f64, ParserError> {
        match &self.current_token().token_type {
            TokenType::Number(n) => {
                let value = *n;
                self.advance();
                Ok(value)
            }
            _ => Err(ParserError::UnexpectedToken {
                expected: "number".to_string(),
                found: format!("{:?}", self.current_token().token_type),
                position: self.current_token().position,
            }),
        }
    }
}

/// Main parse function - entry point for parsing SQL queries
pub fn parse(input: &str) -> Result<Query, ParserError> {
    let mut lexer = Lexer::new(input);
    let tokens = lexer.tokenize()?;
    let mut parser = Parser::new(tokens);
    parser.parse()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lexer_simple_select() {
        let mut lexer = Lexer::new("SELECT * FROM users");
        let tokens = lexer.tokenize().unwrap();
        
        assert_eq!(tokens.len(), 5); // SELECT, *, FROM, users, EOF
        assert!(matches!(tokens[0].token_type, TokenType::Select));
        assert!(matches!(tokens[1].token_type, TokenType::Multiply));
        assert!(matches!(tokens[2].token_type, TokenType::From));
        assert!(matches!(tokens[3].token_type, TokenType::Identifier(_)));
        assert!(matches!(tokens[4].token_type, TokenType::EOF));
    }

    #[test]
    fn test_lexer_string_literal() {
        let mut lexer = Lexer::new(r#"SELECT name FROM users WHERE city = "Berlin""#);
        let tokens = lexer.tokenize().unwrap();
        
        // Find the string token
        let string_token = tokens.iter().find(|t| matches!(t.token_type, TokenType::String(_)));
        assert!(string_token.is_some());
        
        if let TokenType::String(s) = &string_token.unwrap().token_type {
            assert_eq!(s, "Berlin");
        }
    }

    #[test]
    fn test_lexer_number() {
        let mut lexer = Lexer::new("SELECT * FROM users WHERE age > 30");
        let tokens = lexer.tokenize().unwrap();
        
        // Find the number token
        let number_token = tokens.iter().find(|t| matches!(t.token_type, TokenType::Number(_)));
        assert!(number_token.is_some());
        
        if let TokenType::Number(n) = &number_token.unwrap().token_type {
            assert_eq!(*n, 30.0);
        }
    }

    #[test]
    fn test_parser_simple_select() {
        let query = parse("SELECT * FROM users").unwrap();
        
        match query {
            Query::Select(select_query) => {
                assert_eq!(select_query.from, "users");
                assert_eq!(select_query.fields.len(), 1);
                assert!(matches!(select_query.fields[0], Field::All));
                assert!(select_query.where_clause.is_none());
            }
            _ => panic!("Expected SELECT query"),
        }
    }

    #[test]
    fn test_parser_select_with_fields() {
        let query = parse("SELECT name, age FROM users").unwrap();
        
        match query {
            Query::Select(select_query) => {
                assert_eq!(select_query.from, "users");
                assert_eq!(select_query.fields.len(), 2);
                
                if let Field::Named(name) = &select_query.fields[0] {
                    assert_eq!(name, "name");
                }
                if let Field::Named(name) = &select_query.fields[1] {
                    assert_eq!(name, "age");
                }
            }
            _ => panic!("Expected SELECT query"),
        }
    }

    #[test]
    fn test_parser_select_with_where() {
        let query = parse(r#"SELECT * FROM users WHERE age > 30"#).unwrap();
        
        match query {
            Query::Select(select_query) => {
                assert!(select_query.where_clause.is_some());
                
                if let Some(Condition::Comparison { field, operator, value }) = &select_query.where_clause {
                    assert_eq!(field, "age");
                    assert_eq!(*operator, ComparisonOperator::GreaterThan);
                    assert_eq!(*value, Value::Number(serde_json::Number::from_f64(30.0).unwrap()));
                }
            }
            _ => panic!("Expected SELECT query"),
        }
    }

    #[test]
    fn test_parser_insert() {
        let query = parse(r#"INSERT INTO users (name, age) VALUES ("John", 30)"#).unwrap();
        
        match query {
            Query::Insert(insert_query) => {
                assert_eq!(insert_query.into, "users");
                assert_eq!(insert_query.fields, vec!["name", "age"]);
                assert_eq!(insert_query.values.len(), 2);
                assert_eq!(insert_query.values[0], Value::String("John".to_string()));
                assert_eq!(insert_query.values[1], Value::Number(serde_json::Number::from_f64(30.0).unwrap()));
            }
            _ => panic!("Expected INSERT query"),
        }
    }

    #[test]
    fn test_parser_update() {
        let query = parse(r#"UPDATE users SET name = "Jane" WHERE id = "123""#).unwrap();
        
        match query {
            Query::Update(update_query) => {
                assert_eq!(update_query.table, "users");
                assert_eq!(update_query.set.len(), 1);
                assert_eq!(update_query.set[0].field, "name");
                assert_eq!(update_query.set[0].value, Value::String("Jane".to_string()));
                assert!(update_query.where_clause.is_some());
            }
            _ => panic!("Expected UPDATE query"),
        }
    }

    #[test]
    fn test_parser_delete() {
        let query = parse("DELETE FROM users WHERE age < 18").unwrap();
        
        match query {
            Query::Delete(delete_query) => {
                assert_eq!(delete_query.from, "users");
                assert!(delete_query.where_clause.is_some());
            }
            _ => panic!("Expected DELETE query"),
        }
    }

    #[test]
    fn test_parser_create() {
        let query = parse("CREATE COLLECTION users").unwrap();
        
        match query {
            Query::Create(create_query) => {
                assert_eq!(create_query.collection_name, "users");
            }
            _ => panic!("Expected CREATE query"),
        }
    }

    #[test]
    fn test_parser_complex_where() {
        let query = parse(r#"SELECT * FROM users WHERE age > 30 AND city = "Berlin""#).unwrap();
        
        match query {
            Query::Select(select_query) => {
                assert!(select_query.where_clause.is_some());
                
                if let Some(Condition::And(left, right)) = &select_query.where_clause {
                    // Check left condition
                    if let Condition::Comparison { field, operator, .. } = left.as_ref() {
                        assert_eq!(field, "age");
                        assert_eq!(*operator, ComparisonOperator::GreaterThan);
                    }
                    
                    // Check right condition
                    if let Condition::Comparison { field, operator, .. } = right.as_ref() {
                        assert_eq!(field, "city");
                        assert_eq!(*operator, ComparisonOperator::Equal);
                    }
                }
            }
            _ => panic!("Expected SELECT query"),
        }
    }

    #[test]
    fn test_parser_order_by() {
        let query = parse("SELECT * FROM users ORDER BY name ASC, age DESC").unwrap();
        
        match query {
            Query::Select(select_query) => {
                assert!(select_query.order_by.is_some());
                
                if let Some(order_by) = &select_query.order_by {
                    assert_eq!(order_by.len(), 2);
                    assert_eq!(order_by[0].field, "name");
                    assert_eq!(order_by[0].direction, SortDirection::Asc);
                    assert_eq!(order_by[1].field, "age");
                    assert_eq!(order_by[1].direction, SortDirection::Desc);
                }
            }
            _ => panic!("Expected SELECT query"),
        }
    }

    #[test]
    fn test_parser_limit_offset() {
        let query = parse("SELECT * FROM users LIMIT 10 OFFSET 20").unwrap();
        
        match query {
            Query::Select(select_query) => {
                assert_eq!(select_query.limit, Some(10));
                assert_eq!(select_query.offset, Some(20));
            }
            _ => panic!("Expected SELECT query"),
        }
    }

    #[test]
    fn test_parser_error_handling() {
        // Test various error conditions
        assert!(parse("INVALID QUERY").is_err());
        assert!(parse("SELECT").is_err());
        assert!(parse("SELECT * FROM").is_err());
        assert!(parse("SELECT * FROM users WHERE").is_err());
        assert!(parse("INSERT INTO users").is_err());
    }
} 