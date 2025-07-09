# SQL-like Database Engine

A modern, high-performance database engine written in Rust, featuring parallel query processing, columnar storage, and MVCC transactions.

## 🚀 Features

### Core Database Engine
- **In-Memory Storage**: HashMap-based row storage with optional columnar format
- **SQL Parser**: Complete SQL parser built with nom parser combinators
- **Query Execution**: AST-based query executor with optimization
- **Data Types**: Support for INT, TEXT, BOOL, and NULL values
- **Persistence**: JSON-based disk persistence with export/import capabilities

### Advanced Features
- **Parallel Processing**: Automatic parallel query execution using Rayon
- **Indexing**: Hash and B-tree indexes for optimized queries
- **Columnar Storage**: Column-oriented storage for analytical workloads
- **MVCC Transactions**: Multi-version concurrency control with isolation levels
- **Query Planning**: Cost-based query optimization with statistics
- **Web Interface**: REST API with modern web frontend

### Performance Optimizations
- **Intelligent Parallelization**: Automatic parallel processing for large datasets
- **Index-Aware Queries**: Automatic index selection for optimal performance
- **Query Caching**: LRU cache for frequently executed queries
- **Statistics Collection**: Table and column statistics for query optimization
- **Compression**: Basic compression for columnar data

## 🏗️ Architecture

The database engine follows a layered architecture that separates concerns and enables modularity. Each layer builds upon the previous one, creating a robust and maintainable system.

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Web Frontend  │    │   CLI Interface │    │   REST API      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
         ┌───────────────────────▼───────────────────────┐
         │              Database Layer                   │
         └───────────────────────┬───────────────────────┘
                                 │
    ┌────────────┬───────────────┼───────────────┬────────────┐
    │            │               │               │            │
┌───▼────┐  ┌───▼────┐  ┌───────▼───────┐  ┌───▼────┐  ┌───▼────┐
│Parser  │  │Executor│  │Query Planner  │  │Storage │  │Indexes │
└────────┘  └────────┘  └───────────────┘  └────────┘  └────────┘
    │            │               │               │            │
    └────────────┴───────────────┼───────────────┴────────────┘
                                 │
         ┌───────────────────────▼───────────────────────┐
         │         Parallel Execution Engine            │
         └───────────────────────────────────────────────┘
```

### Understanding the Architecture

The **presentation layer** provides multiple interfaces for user interaction. The web frontend offers a modern browser-based experience, while the CLI provides command-line access. The REST API enables programmatic integration with other systems.

The **database layer** serves as the central coordinator, managing connections between all components and handling high-level operations like transaction management and query routing.

The **processing layer** contains the core database logic. The parser transforms SQL strings into abstract syntax trees, the executor runs these operations against the data, and the query planner optimizes performance by choosing the best execution strategy.

The **storage layer** handles data persistence and retrieval, supporting both row-based and columnar storage formats depending on the workload requirements.

The **parallel execution engine** automatically distributes work across multiple CPU cores when processing large datasets, dramatically improving query performance.

## 📦 Installation

### Prerequisites
- Rust 1.70 or higher
- Cargo package manager

### Building from Source
```bash
git clone https://github.com/yourusername/sql-database-engine.git
cd sql-database-engine
cargo build --release
```

### Running Tests
```bash
cargo test
```

The test suite includes unit tests for individual components and integration tests that verify the entire system works together correctly.

## 🚦 Quick Start

### 1. Interactive CLI Mode

The CLI mode provides an interactive environment similar to MySQL or PostgreSQL command-line tools. This is perfect for learning SQL or testing queries.

```bash
cargo run cli
```

Example session:
```sql
sql> CREATE TABLE users (id INT, name TEXT, email TEXT)
✅ Table 'users' created successfully

sql> INSERT INTO users VALUES (1, 'John Doe', 'john@example.com')
✅ 1 row inserted into 'users'

sql> SELECT * FROM users
┌─────┬──────────┬─────────────────┐
│ id  │ name     │ email           │
├─────┼──────────┼─────────────────┤
│ 1   │ John Doe │ john@example.com│
└─────┴──────────┴─────────────────┘
```

### 2. Web Interface

The web interface provides a modern, browser-based way to interact with the database. It includes syntax highlighting, query history, and performance monitoring.

```bash
cargo run web 3000
```

Navigate to `http://localhost:3000` for the web interface.

### 3. Programmatic Usage

You can embed the database engine directly in your Rust applications. This approach gives you the best performance and allows for custom integration.

```rust
use musterirapor::Database;

fn main() {
    let mut db = Database::new();
    
    // Create table
    db.execute_sql("CREATE TABLE products (id INT, name TEXT, price INT)").unwrap();
    
    // Insert data
    db.execute_sql("INSERT INTO products VALUES (1, 'Laptop', 999)").unwrap();
    
    // Query data
    let result = db.execute_sql("SELECT * FROM products").unwrap();
    println!("{:?}", result);
}
```

## 📖 SQL Reference

### Understanding SQL in This Engine

This database engine implements a subset of SQL that covers the most common operations. The parser uses a recursive descent approach with nom combinators, which allows for precise error messages and extensible grammar.

### Data Definition Language (DDL)

#### CREATE TABLE

The CREATE TABLE statement defines the structure of your data. Each column must have a name and a type, and the engine enforces these types strictly.

```sql
CREATE TABLE table_name (
    column1 TYPE,
    column2 TYPE,
    ...
);
```

**Supported Types:**
- `INT` / `INTEGER`: 64-bit signed integers (-9,223,372,036,854,775,808 to 9,223,372,036,854,775,807)
- `TEXT` / `VARCHAR`: UTF-8 strings of any length
- `BOOL` / `BOOLEAN`: True/false values

#### CREATE INDEX

Indexes dramatically improve query performance by creating fast lookup structures. The engine supports two types of indexes, each optimized for different query patterns.

```sql
CREATE INDEX table_name (column_name) [HASH|BTREE]
```

**Hash indexes** provide O(1) lookup time for equality queries (WHERE column = value). **B-tree indexes** support both equality and range queries (WHERE column > value) with O(log n) complexity.

#### DROP TABLE

Removes a table and all its data permanently. This operation cannot be undone.

```sql
DROP TABLE table_name
```

### Data Manipulation Language (DML)

#### INSERT

Adds new rows to a table. Values must be provided in the same order as the columns were defined in CREATE TABLE.

```sql
INSERT INTO table_name VALUES (value1, value2, ...)
```

#### SELECT

Retrieves data from tables. The engine supports column selection, filtering with WHERE clauses, and automatic parallel processing for large datasets.

```sql
SELECT column1, column2 FROM table_name [WHERE condition]
SELECT * FROM table_name [WHERE condition]
```

#### UPDATE

Modifies existing rows in a table. The WHERE clause determines which rows to update. Without WHERE, all rows will be updated.

```sql
UPDATE table_name SET column1 = value1, column2 = value2 [WHERE condition]
```

#### DELETE

Removes rows from a table. Like UPDATE, the WHERE clause determines which rows to delete.

```sql
DELETE FROM table_name [WHERE condition]
```

### WHERE Clause Operators

The WHERE clause supports comprehensive comparison and logical operations:

- `=` (equals): Exact match
- `!=` (not equals): Exclude exact matches
- `>` (greater than): Numeric and lexicographic comparison
- `<` (less than): Numeric and lexicographic comparison
- `>=` (greater than or equal): Inclusive upper bound
- `<=` (less than or equal): Inclusive lower bound
- `AND` (logical and): Both conditions must be true
- `OR` (logical or): Either condition must be true

### Advanced Features

#### Storage Format Management

The engine supports both row-based and columnar storage formats. Row-based storage is optimal for transactional workloads, while columnar storage excels at analytical queries.

```sql
-- Set storage format for analytical queries
SET STORAGE FORMAT table_name COLUMN

-- Show storage information
SHOW STORAGE INFO table_name

-- Compress columns
COMPRESS COLUMNS table_name
```

#### Query Analysis

Understanding how queries execute helps optimize performance. The EXPLAIN command shows the execution plan chosen by the query planner.

```sql
-- Explain query execution plan
EXPLAIN SELECT * FROM users WHERE age > 25

-- Show table statistics
SHOW STATS table_name
```

#### Transaction Management

The engine implements MVCC (Multi-Version Concurrency Control) for transaction isolation. This allows multiple transactions to run concurrently without interfering with each other.

```sql
-- Begin transaction with isolation level
BEGIN TRANSACTION [ISOLATION READ_COMMITTED|REPEATABLE_READ|SERIALIZABLE]

-- Commit transaction
COMMIT

-- Rollback transaction
ROLLBACK

-- Show active transactions
SHOW TRANSACTIONS
```

## 🔧 Configuration

### CLI Options

The command-line interface supports multiple modes of operation:

```bash
# Interactive CLI
cargo run cli

# Web server (default port 3000)
cargo run web [port]

# Export database
cargo run export backup.dbdump.json

# Import database
cargo run import backup.dbdump.json [--clear]

# Run performance demo
cargo run test
```

### Web Server Configuration

The web server is built with Axum and provides both the frontend interface and REST API endpoints. Configure through environment variables:

```bash
export DB_PORT=8080
export DB_DATA_DIR=./data
```

## 🎯 Performance Tuning

### Understanding Parallel Processing

The engine automatically detects when parallel processing will improve performance. For datasets larger than 1000 rows, queries are automatically distributed across multiple CPU cores using a work-stealing approach.

You can customize the parallel processing behavior:

```rust
let mut executor = QueryExecutor::with_parallel_settings(
    500,    // min_rows_threshold: minimum rows to trigger parallel processing
    250,    // chunk_size: rows per parallel work unit
    Some(4) // max_threads: maximum number of worker threads
);
```

### Indexing Strategy

Choosing the right index type is crucial for performance:

- **Hash indexes** are perfect for equality queries (`WHERE id = 1`). They provide constant-time lookup but cannot support range queries.
- **B-tree indexes** support both equality and range queries (`WHERE age > 25`). They have logarithmic lookup time but are more versatile.

Create indexes on columns that appear frequently in WHERE clauses.

### Storage Optimization

The engine supports multiple storage formats:

- **Row-based storage**: Optimal for transactional workloads with frequent updates
- **Columnar storage**: Excellent for analytical queries that scan many rows but few columns
- **Hybrid mode**: Automatically chooses the best format based on usage patterns

Enable compression for large tables to reduce memory usage and improve cache efficiency.

## 📊 Monitoring & Observability

### Performance Monitoring

The web interface includes a dedicated performance monitoring page accessible at `http://localhost:3000/stats`. This page provides real-time metrics including:

- CPU and memory usage of the database process
- Query execution times and frequency
- Index usage statistics
- Table growth and access patterns
- Transaction throughput and isolation level distribution

### Query Statistics

The database engine automatically collects detailed statistics about query execution:

```sql
SHOW STATS table_name
```

This command reveals:
- Row count and column cardinality
- Index usage frequency and performance
- Query execution patterns and optimization opportunities
- Selectivity estimates for query planning

These statistics help the query planner make informed decisions about execution strategies.

## 🛠️ Development

### Project Structure

The codebase is organized into logical modules, each with a specific responsibility:

```
src/
├── lib.rs              # Library exports and public API
├── main.rs             # CLI entry point and demo functions
├── types.rs            # Core data type definitions
├── row.rs              # Row data structure implementation
├── table.rs            # Table management and indexing
├── database.rs         # Database coordination and persistence
├── parser.rs           # SQL parser using nom combinators
├── executor.rs         # Query execution engine
├── query_planner.rs    # Query optimization and planning
├── parallel_executor.rs # Parallel processing implementation
├── columnar_storage.rs # Columnar storage engine
├── transaction.rs      # MVCC transaction management
├── errors.rs           # Comprehensive error handling
├── cli.rs              # Command-line interface
└── web.rs              # Web server and REST API
```

### Understanding the Code Architecture

Each module serves a specific purpose in the database engine:

**types.rs** defines the fundamental data types and operations. The `TypedValue` enum represents all possible values in the database, with careful ordering implementation for comparisons.

**parser.rs** contains the SQL parser built with nom combinators. This approach allows for precise error messages and makes it easy to extend the grammar.

**executor.rs** implements the query execution engine. It takes parsed SQL statements and executes them against the database, handling all the complexity of data retrieval and modification.

**query_planner.rs** contains the optimization logic. It analyzes queries and chooses the most efficient execution strategy based on available indexes and table statistics.

### Running Tests

The test suite covers both unit and integration testing:

```bash
# Run all tests
cargo test

# Run specific test module
cargo test --test integration_tests

# Run with output
cargo test -- --nocapture
```

### Adding New Features

The modular architecture makes it straightforward to add new functionality:

1. **New SQL Operations**: Add parsing logic to `parser.rs` and execution logic to `executor.rs`
2. **New Data Types**: Extend the `TypedValue` enum in `types.rs` and update comparison logic
3. **New Storage Formats**: Implement new storage engines in `columnar_storage.rs`
4. **New Optimizations**: Add optimization rules to `query_planner.rs`

### Code Style Guidelines

This project follows Rust standard conventions with additional guidelines:

- Use `cargo fmt` for consistent formatting
- Use `cargo clippy` for linting and best practices
- Follow Rust naming conventions (snake_case for functions, PascalCase for types)
- Add comprehensive documentation comments for public APIs
- Include examples in documentation when helpful

## 🤝 Contributing

We welcome contributions from developers of all skill levels. Whether you're fixing bugs, adding features, or improving documentation, your contributions help make this project better.

### Getting Started with Contributing

1. **Fork the repository** on GitHub
2. **Create a feature branch**: `git checkout -b feature/new-feature`
3. **Write tests** for new functionality to ensure quality
4. **Follow code style** guidelines for consistency
5. **Submit a pull request** with a clear description

### Development Setup

Set up your development environment with these helpful tools:

```bash
# Install development dependencies
cargo install cargo-watch cargo-tarpaulin

# Run tests in watch mode during development
cargo watch -x test

# Generate test coverage reports
cargo tarpaulin --out html
```

## 📄 API Reference

### REST API Endpoints

The REST API provides programmatic access to all database functionality. All endpoints return JSON responses and follow REST conventions.

#### Execute Query

Execute any SQL statement and receive structured results:

```http
POST /query
Content-Type: application/json

{
  "sql": "SELECT * FROM users"
}
```

Response:
```json
{
  "success": true,
  "result": {
    "Select": {
      "columns": ["id", "name", "email"],
      "rows": [["1", "John Doe", "john@example.com"]],
      "execution_time_ms": 1250
    }
  }
}
```

#### Create Backup

Generate a complete database backup in JSON format:

```http
POST /backup
```

Response:
```json
{
  "success": true,
  "backup_data": "...",
  "filename": "database_backup_20231201_143022.dbdump.json"
}
```

#### Performance Stats

Retrieve real-time performance metrics:

```http
GET /api/stats
```

Response:
```json
{
  "success": true,
  "cpu_usage": 15.2,
  "memory_usage": 45.8,
  "database_stats": {
    "table_count": 3,
    "total_rows": 1500
  }
}
```

## 🔒 Security Considerations

**Important:** This database engine is designed for educational and development purposes. The current implementation prioritizes learning and experimentation over production security.

For production use, you would need to implement additional security measures:

- **Authentication and authorization**: User management and access control
- **Input validation**: Comprehensive validation beyond type checking
- **SQL injection protection**: Parameterized queries and input sanitization
- **Rate limiting**: Protection against denial-of-service attacks
- **Audit logging**: Comprehensive logging of all database operations
- **Data encryption**: Encryption at rest and in transit
- **Network security**: TLS/SSL implementation and secure configuration

## 🐛 Known Limitations

Understanding these limitations helps set appropriate expectations:

- **Memory Usage**: The entire database is kept in memory, limiting dataset size to available RAM
- **Concurrency**: Limited support for concurrent write operations
- **Durability**: JSON persistence is not crash-safe and could lead to data loss
- **Scalability**: Single-node architecture cannot scale horizontally
- **SQL Coverage**: Limited SQL feature set compared to full SQL standard

## 📋 Roadmap

### Short Term Goals
- Implement JOIN operations for multi-table queries
- Add aggregate functions (COUNT, SUM, AVG, MIN, MAX)
- Implement ORDER BY and LIMIT clauses
- Add subquery support

### Medium Term Goals
- Implement write-ahead logging (WAL) for crash recovery
- Develop disk-based storage engine to handle larger datasets
- Add connection pooling for better concurrent access
- Implement prepared statements for better performance

### Long Term Vision
- Design distributed architecture for horizontal scaling
- Add replication support for high availability
- Implement advanced indexing techniques (GIN, GiST)
- Add query result streaming for large result sets

## 📚 Learning Resources

### Understanding Database Internals

To deepen your understanding of how this database engine works, consider these resources:

- **[Database System Concepts](https://www.db-book.com/)**: Comprehensive textbook covering all aspects of database systems
- **[Designing Data-Intensive Applications](https://dataintensive.net/)**: Modern perspective on distributed data systems
- **[The Rust Programming Language](https://doc.rust-lang.org/book/)**: Essential for understanding the implementation language

### Related Projects

Study these projects to see different approaches to database implementation:

- **[SQLite](https://sqlite.org/)**: Excellent example of embedded database design
- **[DuckDB](https://duckdb.org/)**: Modern columnar database with analytical focus
- **[TiKV](https://tikv.org/)**: Distributed key-value store implemented in Rust

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

This project builds upon the excellent work of the Rust community and various open-source projects:

- **Rust Community** for creating an excellent ecosystem of database-related crates
- **nom Parser** for providing powerful and composable parser combinators
- **Rayon** for making data parallelism accessible and efficient
- **Axum** for the excellent web framework
- **SQLite** for architectural inspiration and design patterns

The implementation draws inspiration from academic database research and production database systems, adapted for educational purposes and modern Rust idioms.

---

**Built with ❤️ and Rust**

This database engine represents a journey through the fascinating world of database internals. Whether you're learning about databases, exploring Rust, or building something new, we hope this project serves as both a useful tool and an educational resource.