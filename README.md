# A Relational Database Management System (RDBMS)

Built for the **Pesapal Junior Dev Challenge '26**, **PyMiniDB** is a lightweight, from-scratch Relational Database Management System (RDBMS). Moving beyond simple file storage, this project implements a custom storage engine, a recursive descent SQL parser, and a page-oriented data persistence layer designed for performance and reliability.



## 🚀 Project Overview
PyMiniDB was developed to demonstrate a deep understanding of database internals. While many modern applications rely on high-level abstractions, this project dives into the low-level complexities of data persistence, binary serialization, and query optimization. It features a custom binary storage format and a SQL-like interface that powers a real-world Task Management web application.

## 🛠 Core Features
* **Custom Storage Engine**: Data is persisted in 4KB binary pages to optimize disk I/O and minimize memory overhead, moving away from standard, inefficient text-based formats.
* **Recursive Descent SQL Parser**: A hand-coded parser that translates SQL strings into an Abstract Syntax Tree (AST), supporting DDL and DML commands.
* **Comprehensive CRUD**: Full support for `CREATE`, `INSERT`, `SELECT`, `UPDATE`, and `DELETE` operations with schema validation.
* **Relational Logic**: Advanced support for table schemas, primary keys, and relational `JOIN` operations to link datasets.
* **Interactive REPL**: A dedicated Command Line Interface (CLI) for direct, low-latency database interaction.
* **Web Integration**: A Flask-based Task Manager demonstrating the engine's capability as a primary backend data store.



## 🏗 System Architecture
The system follows a strictly modular, layered architecture to ensure maintainability and scalability:

1.  **Interface Layer**: The REPL and Flask Web App providing user access points.
2.  **Language Layer**: The Lexer and Parser which tokenize and validate SQL syntax.
3.  **Execution Layer**: The Query Executor, which maps AST nodes to physical storage operations.
4.  **Storage Layer**: The Page Manager and Catalog, which handle binary file I/O and maintain schema metadata.

## 💻 Technical Implementation
* **Language**: Python 3.10+
* **Indexing Logic**: Initial implementation of B+ Trees for $O(\log n)$ search efficiency. While currently undergoing optimization for complex range queries, the architecture is built to support indexed lookups.
* **Persistence Strategy**: Implements a custom `Catalog` for metadata management, ensuring that table definitions persist across sessions.
* **Recovery**: Foundational logic for Write-Ahead Logging (WAL) to ensure data integrity during unexpected shutdowns.



## 📂 Project Structure
```text
PyMiniDB/
├── src/
│   ├── parser/          # Lexer and Recursive Descent Parser logic
│   ├── storage/         # Page Manager and Binary File I/O
│   ├── executor/        # Query Execution and Scan logic
│   ├── catalog/         # Schema and metadata management
│   └── index/           # B+ Tree indexing implementation
├── web_app/             # Flask-based CRUD Demo Application
├── tests/               # Unit and Integration test suites
└── main.py              # Interactive REPL Entry point


## 🌐 Link to test the functionality
* **Link**: https://tinyurl.com/PesapalChallenge
