"""
REPL (Read-Eval-Print Loop) interface for PyMiniDB.
"""
import sys
import os
from typing import List
from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.styles import Style

from ..catalog.catalog import Catalog
from ..storage.storage_manager import StorageManager
from ..executor.query_executor import QueryExecutor, QueryResult
from ..parser.sql_interface import SimpleSQLParser

class PyMiniDBREPL:
    """Interactive REPL for PyMiniDB."""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or "pyminidb.db"
        self.catalog = Catalog()
        self.storage = StorageManager(self.db_path)
        self.query_executor = QueryExecutor(self.catalog, self.storage)
        self.parser = SimpleSQLParser(self.query_executor)
        
        # Initialize database
        self._initialize_database()
        
        # Setup prompt
        self.session = PromptSession(
            history=FileHistory('.pyminidb_history'),
            auto_suggest=AutoSuggestFromHistory(),
            enable_history_search=True
        )
        
        # Style for prompt
        self.style = Style.from_dict({
            'prompt': 'ansigreen bold',
            'sql': 'ansicyan',
            'success': 'ansigreen',
            'error': 'ansired',
            'warning': 'ansiyellow',
            'info': 'ansiblue',
        })
    
    def _initialize_database(self):
        """Initialize or load database."""
        if os.path.exists(self.db_path):
            try:
                self.storage.open()
                print(f"Loaded database: {self.db_path}")
            except Exception as e:
                print(f"Error loading database: {e}")
                print("Creating new database...")
                self.storage.create_database()
                self.storage.open()
        else:
            print(f"Creating new database: {self.db_path}")
            self.storage.create_database()
            self.storage.open()
    
    def run(self):
        """Run the REPL."""
        print("\n" + "="*60)
        print("PyMiniDB - A Minimal Relational Database")
        print("="*60)
        print("Type SQL commands or 'help' for help")
        print("Type 'exit' or 'quit' to exit")
        print("="*60 + "\n")
        
        while True:
            try:
                # Get user input
                text = self.session.prompt(
                    'pyminidb> ',
                    style=self.style
                ).strip()
                
                if not text:
                    continue
                
                # Handle special commands
                if text.lower() in ['exit', 'quit', '\\q']:
                    print("Goodbye!")
                    self.storage.close()
                    break
                elif text.lower() in ['help', '\\?']:
                    self._show_help()
                    continue
                elif text.lower() in ['tables', '\\dt']:
                    self._show_tables()
                    continue
                elif text.lower().startswith('describe '):
                    table_name = text[9:].strip()
                    self._describe_table(table_name)
                    continue
                elif text.lower() == 'clear':
                    os.system('cls' if os.name == 'nt' else 'clear')
                    continue
                
                # Execute SQL
                result = self.parser.parse_execute(text)
                self._display_result(result)
                
            except KeyboardInterrupt:
                print("\nInterrupted. Use 'exit' to quit.")
                continue
            except EOFError:
                print("\nGoodbye!")
                self.storage.close()
                break
            except Exception as e:
                print(f"\nError: {e}")
    
    def _display_result(self, result: QueryResult):
        """Display query result."""
        if result.success:
            if result.message:
                print(f"[SUCCESS] {result.message}")
            
            if result.data:
                self._display_table(result.data)
            
            if result.rows_affected > 0:
                print(f"Rows affected: {result.rows_affected}")
            
            if result.execution_time > 0:
                print(f"Execution time: {result.execution_time:.3f}s")
        else:
            print(f"[ERROR] {result.message}")
    
    def _display_table(self, data: List[dict]):
        """Display data in table format."""
        if not data:
            print("(No rows)")
            return
        
        # Get column names
        columns = list(data[0].keys())
        
        # Calculate column widths
        col_widths = {}
        for col in columns:
            col_widths[col] = len(col)
            for row in data:
                value = str(row.get(col, ''))
                col_widths[col] = max(col_widths[col], len(value))
        
        # Create format string
        format_str = " | ".join([f"{{:<{col_widths[col]}}}" for col in columns])
        
        # Print header
        print("\n" + format_str.format(*columns))
        print("-" * (sum(col_widths.values()) + 3 * (len(columns) - 1)))
        
        # Print rows
        for row in data:
            values = [str(row.get(col, '')) for col in columns]
            print(format_str.format(*values))
        
        print()
    
    def _show_help(self):
        """Show help information."""
        help_text = """
PyMiniDB Commands:
  SQL Commands:
    CREATE TABLE table_name (col1 type, col2 type, ...)
    INSERT INTO table_name VALUES (val1, val2, ...)
    SELECT * FROM table_name [WHERE condition] [LIMIT n]
    UPDATE table_name SET col1=val1 [WHERE condition]
    DELETE FROM table_name [WHERE condition]
    DROP TABLE table_name
    CREATE INDEX idx_name ON table_name (column)
    DROP INDEX idx_name

  Special Commands:
    help, ?              Show this help
    exit, quit, \\q       Exit PyMiniDB
    tables, \\dt          List all tables
    describe table_name  Show table structure
    clear                Clear screen

  Examples:
    CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50), age INTEGER)
    INSERT INTO users VALUES (1, 'Alice', 30)
    SELECT * FROM users WHERE age > 25
    UPDATE users SET age = 31 WHERE name = 'Alice'
    DELETE FROM users WHERE id = 1
"""
        print(help_text)
    
    def _show_tables(self):
        """Show all tables in the database."""
        tables = list(self.catalog.tables.keys())
        
        if not tables:
            print("No tables in database.")
            return
        
        print("\nTables in database:")
        for table in tables:
            print(f"  - {table}")
        print()
    
    def _describe_table(self, table_name: str):
        """Show structure of a table."""
        try:
            table = self.catalog.get_table(table_name)
            print(f"\nTable: {table.table_name}")
            print("Columns:")
            for col_name, col_schema in table.columns.items():
                constraints = []
                if col_schema.is_primary_key:
                    constraints.append("PRIMARY KEY")
                if col_schema.is_unique:
                    constraints.append("UNIQUE")
                if col_schema.is_not_null:
                    constraints.append("NOT NULL")
                
                constr_str = " ".join(constraints)
                type_str = f"{col_schema.data_type.value}"
                if col_schema.length:
                    type_str += f"({col_schema.length})"
                
                print(f"  {col_name:20} {type_str:15} {constr_str}")
            
            # Show indexes
            indexes = self.catalog.get_table_indexes(table_name)
            if indexes:
                print("\nIndexes:")
                for idx in indexes:
                    unique = "UNIQUE " if idx.is_unique else ""
                    print(f"  {unique}INDEX {idx.index_name} ON ({', '.join(idx.column_names)})")
            
            print()
        except Exception as e:
            print(f"Error: {e}")

def main():
    """Main entry point for REPL."""
    import argparse
    
    parser = argparse.ArgumentParser(description='PyMiniDB - A Minimal Relational Database')
    parser.add_argument('--db', '-d', default='pyminidb.db',
                       help='Database file path (default: pyminidb.db)')
    
    args = parser.parse_args()
    
    # Check if prompt_toolkit is available
    try:
        import prompt_toolkit
        repl = PyMiniDBREPL(args.db)
        repl.run()
    except ImportError:
        print("Warning: prompt_toolkit not installed. Using basic input.")
        print("Install with: pip install prompt_toolkit")
        
        # Fallback to simple input
        simple_repl = SimpleREPL(args.db)
        simple_repl.run()

class SimpleREPL:
    """Simple REPL without prompt_toolkit."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.catalog = Catalog()
        self.storage = StorageManager(self.db_path)
        self.query_executor = QueryExecutor(self.catalog, self.storage)
        self.parser = SimpleSQLParser(self.query_executor)
        self._initialize_database()
    
    def _initialize_database(self):
        if os.path.exists(self.db_path):
            try:
                self.storage.open()
                print(f"Loaded database: {self.db_path}")
            except Exception as e:
                print(f"Error loading database: {e}")
                print("Creating new database...")
                self.storage.create_database()
                self.storage.open()
        else:
            print(f"Creating new database: {self.db_path}")
            self.storage.create_database()
            self.storage.open()
    
    def run(self):
        print("\n" + "="*60)
        print("PyMiniDB - A Minimal Relational Database (Simple Mode)")
        print("="*60)
        print("Type SQL commands or 'help' for help")
        print("Type 'exit' or 'quit' to exit")
        print("="*60 + "\n")
        
        while True:
            try:
                text = input("pyminidb> ").strip()
                
                if not text:
                    continue
                
                if text.lower() in ['exit', 'quit', '\\q']:
                    print("Goodbye!")
                    self.storage.close()
                    break
                elif text.lower() in ['help', '\\?']:
                    self._show_help()
                    continue
                elif text.lower() in ['tables', '\\dt']:
                    self._show_tables()
                    continue
                elif text.lower().startswith('describe '):
                    table_name = text[9:].strip()
                    self._describe_table(table_name)
                    continue
                elif text.lower() == 'clear':
                    os.system('cls' if os.name == 'nt' else 'clear')
                    continue
                
                result = self.parser.parse_execute(text)
                self._display_result(result)
                
            except KeyboardInterrupt:
                print("\nUse 'exit' to quit.")
                continue
            except EOFError:
                print("\nGoodbye!")
                self.storage.close()
                break
            except Exception as e:
                print(f"\nError: {e}")
    
    def _display_result(self, result: QueryResult):
        if result.success:
            if result.message:
                print(f"[SUCCESS] {result.message}")
            
            if result.data:
                for row in result.data:
                    print(row)
            
            if result.rows_affected > 0:
                print(f"Rows affected: {result.rows_affected}")
            
            if result.execution_time > 0:
                print(f"Execution time: {result.execution_time:.3f}s")
        else:
            print(f"[ERROR] {result.message}")
    
    def _show_help(self):
        print("""
Basic commands: CREATE TABLE, INSERT, SELECT, UPDATE, DELETE
Type 'exit' to quit, 'tables' to list tables.
""")
    
    def _show_tables(self):
        tables = list(self.catalog.tables.keys())
        if tables:
            print("Tables:", ", ".join(tables))
        else:
            print("No tables.")
    
    def _describe_table(self, table_name: str):
        try:
            table = self.catalog.get_table(table_name)
            print(f"Table: {table.table_name}")
            for col_name, col_schema in table.columns.items():
                print(f"  {col_name}: {col_schema.data_type.value}")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()