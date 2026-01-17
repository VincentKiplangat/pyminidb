#!/usr/bin/env python
"""
Demonstrate PyMiniDB SQL operations.
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.catalog.catalog import Catalog
from src.storage.storage_manager import StorageManager
from src.executor.query_executor import QueryExecutor, QueryType
from src.parser.sql_interface import SimpleSQLParser
import tempfile

def demo_full_sql():
    """Demonstrate full SQL operations."""
    print("="*60)
    print("PyMiniDB SQL Demonstration")
    print("="*60)
    
    # Create temporary database
    db_path = tempfile.mktemp(suffix='.db')
    
    try:
        # Initialize components
        catalog = Catalog()
        storage = StorageManager(db_path)
        storage.create_database()
        storage.open()
        
        executor = QueryExecutor(catalog, storage)
        parser = SimpleSQLParser(executor)
        
        print("\n1. Creating tables...")
        
        # Create users table
        result = parser.parse_execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                username VARCHAR(50) UNIQUE NOT NULL,
                email VARCHAR(100) UNIQUE,
                age INTEGER,
                city VARCHAR(50),
                created_at TIMESTAMP
            )
        """)
        print(f"   {result.message}")
        
        # Create orders table
        result = parser.parse_execute("""
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                product VARCHAR(100),
                amount DECIMAL(10, 2),
                status VARCHAR(20),
                order_date DATE
            )
        """)
        print(f"   {result.message}")
        
        print("\n2. Inserting data...")
        
        # Insert users
        users = [
            (1, 'alice123', 'alice@example.com', 30, 'New York'),
            (2, 'bobsmith', 'bob@example.com', 25, 'London'),
            (3, 'charlie99', 'charlie@example.com', 35, 'Paris'),
            (4, 'diana_k', 'diana@example.com', 28, 'Berlin'),
            (5, 'evan_lee', 'evan@example.com', 32, 'Tokyo'),
        ]
        
        for user in users:
            sql = f"""
                INSERT INTO users VALUES (
                    {user[0]}, 
                    '{user[1]}', 
                    '{user[2]}', 
                    {user[3]}, 
                    '{user[4]}',
                    '2024-01-01'
                )
            """
            result = parser.parse_execute(sql)
            print(f"   Inserted user: {user[1]}")
        
        # Insert orders
        orders = [
            (101, 1, 'Laptop', 999.99, 'delivered'),
            (102, 1, 'Mouse', 29.99, 'shipped'),
            (103, 2, 'Keyboard', 79.99, 'pending'),
            (104, 3, 'Monitor', 299.99, 'delivered'),
            (105, 4, 'Tablet', 499.99, 'processing'),
            (106, 5, 'Phone', 799.99, 'shipped'),
            (107, 5, 'Headphones', 129.99, 'delivered'),
        ]
        
        for order in orders:
            sql = f"""
                INSERT INTO orders VALUES (
                    {order[0]},
                    {order[1]},
                    '{order[2]}',
                    {order[3]},
                    '{order[4]}',
                    '2024-01-15'
                )
            """
            result = parser.parse_execute(sql)
            print(f"   Inserted order: {order[2]} for user {order[1]}")
        
        print("\n3. Querying data...")
        
        # Select all users
        print("\n   All users:")
        result = parser.parse_execute("SELECT * FROM users")
        if result.success and result.data:
            for row in result.data:
                print(f"     {row}")
        
        # Select with WHERE clause
        print("\n   Users from London:")
        result = parser.parse_execute("SELECT username, email, age FROM users WHERE city = 'London'")
        if result.success and result.data:
            for row in result.data:
                print(f"     {row}")
        
        # Select with LIMIT
        print("\n   First 3 users:")
        result = parser.parse_execute("SELECT * FROM users LIMIT 3")
        if result.success and result.data:
            for row in result.data:
                print(f"     {row}")
        
        # Join query (simulated)
        print("\n   User orders (simulated join):")
        print("     Note: Full JOINs coming in next version!")
        
        print("\n4. Updating data...")
        
        # Update user age
        result = parser.parse_execute("UPDATE users SET age = 31 WHERE username = 'alice123'")
        print(f"   {result.message}")
        
        # Verify update
        result = parser.parse_execute("SELECT username, age FROM users WHERE username = 'alice123'")
        if result.success and result.data:
            print(f"   Updated: {result.data[0]}")
        
        print("\n5. Deleting data...")
        
        # Delete a user
        result = parser.parse_execute("DELETE FROM users WHERE username = 'bobsmith'")
        print(f"   {result.message}")
        
        # Verify deletion
        result = parser.parse_execute("SELECT username FROM users WHERE username = 'bobsmith'")
        if result.success and result.data:
            print(f"   User still exists: {result.data}")
        else:
            print("   User successfully deleted")
        
        print("\n6. Creating indexes...")
        
        # Create index on city
        result = parser.parse_execute("CREATE INDEX idx_users_city ON users (city)")
        print(f"   {result.message}")
        
        # Create index on order status
        result = parser.parse_execute("CREATE INDEX idx_orders_status ON orders (status)")
        print(f"   {result.message}")
        
        print("\n7. Table information...")
        
        # Show tables
        tables = list(catalog.tables.keys())
        print(f"   Tables in database: {', '.join(tables)}")
        
        # Show indexes for users table
        indexes = catalog.get_table_indexes('users')
        print(f"   Indexes on 'users': {len(indexes)}")
        for idx in indexes:
            print(f"     - {idx.index_name} on ({', '.join(idx.column_names)})")
        
        print("\n8. Performance test...")
        
        import time
        
        # Insert many rows for performance test
        print("   Inserting 1000 test rows...")
        start = time.time()
        
        for i in range(1000):
            sql = f"""
                INSERT INTO users VALUES (
                    {i + 100},
                    'testuser{i}',
                    'test{i}@example.com',
                    {20 + (i % 40)},
                    'City{i % 10}',
                    '2024-01-01'
                )
            """
            parser.parse_execute(sql)
        
        insert_time = time.time() - start
        print(f"   Insert time: {insert_time:.2f}s ({insert_time/1000*1000:.1f} ms per row)")
        
        # Query test
        print("\n   Querying with WHERE clause...")
        start = time.time()
        
        result = parser.parse_execute("SELECT * FROM users WHERE age > 30")
        
        query_time = time.time() - start
        if result.success:
            print(f"   Found {len(result.data)} users older than 30")
            print(f"   Query time: {query_time:.3f}s")
        
        print("\n" + "="*60)
        print("Demonstration complete!")
        print(f"Database saved to: {db_path}")
        print("="*60)
        
    finally:
        # Clean up
        storage.close()
        if os.path.exists(db_path):
            os.unlink(db_path)

def demo_interactive():
    """Run interactive demo."""
    print("\n" + "="*60)
    print("Interactive SQL Demo")
    print("="*60)
    print("Try these SQL commands:\n")
    
    examples = [
        "CREATE TABLE test (id INTEGER PRIMARY KEY, name VARCHAR(50), value INTEGER)",
        "INSERT INTO test VALUES (1, 'First', 100)",
        "INSERT INTO test VALUES (2, 'Second', 200)",
        "SELECT * FROM test",
        "SELECT name, value FROM test WHERE value > 150",
        "UPDATE test SET value = 300 WHERE name = 'First'",
        "DELETE FROM test WHERE id = 2",
        "DROP TABLE test",
    ]
    
    for i, example in enumerate(examples, 1):
        print(f"{i:2}. {example}")
    
    print("\nType 'run' to execute these commands, or 'exit' to quit.")
    
    while True:
        cmd = input("\n> ").strip().lower()
        
        if cmd == 'run':
            run_example_queries()
            break
        elif cmd in ['exit', 'quit']:
            print("Goodbye!")
            break
        else:
            print("Type 'run' or 'exit'")

def run_example_queries():
    """Run the example queries."""
    db_path = tempfile.mktemp(suffix='.db')
    
    try:
        catalog = Catalog()
        storage = StorageManager(db_path)
        storage.create_database()
        storage.open()
        
        executor = QueryExecutor(catalog, storage)
        parser = SimpleSQLParser(executor)
        
        queries = [
            ("Creating table...", 
             "CREATE TABLE test (id INTEGER PRIMARY KEY, name VARCHAR(50), value INTEGER)"),
            
            ("Inserting first row...", 
             "INSERT INTO test VALUES (1, 'First', 100)"),
            
            ("Inserting second row...", 
             "INSERT INTO test VALUES (2, 'Second', 200)"),
            
            ("Selecting all rows...", 
             "SELECT * FROM test"),
            
            ("Selecting with condition...", 
             "SELECT name, value FROM test WHERE value > 150"),
            
            ("Updating a row...", 
             "UPDATE test SET value = 300 WHERE name = 'First'"),
            
            ("Verifying update...", 
             "SELECT * FROM test"),
            
            ("Deleting a row...", 
             "DELETE FROM test WHERE id = 2"),
            
            ("Final table state...", 
             "SELECT * FROM test"),
            
            ("Cleaning up...", 
             "DROP TABLE test"),
        ]
        
        for description, sql in queries:
            print(f"\n{description}")
            print(f"SQL: {sql}")
            
            result = parser.parse_execute(sql)
            
            if result.success:
                print(f"Result: {result.message}")
                if result.data:
                    print("Data:")
                    for row in result.data:
                        print(f"  {row}")
            else:
                print(f"Error: {result.message}")
            
            input("\nPress Enter to continue...")
        
        print("\nDemo complete!")
        
    finally:
        storage.close()
        if os.path.exists(db_path):
            os.unlink(db_path)

if __name__ == "__main__":
    print("PyMiniDB SQL Demo")
    print("1. Full demonstration")
    print("2. Interactive step-by-step")
    
    choice = input("\nSelect (1 or 2): ").strip()
    
    if choice == '1':
        demo_full_sql()
    elif choice == '2':
        demo_interactive()
    else:
        print("Invalid choice")