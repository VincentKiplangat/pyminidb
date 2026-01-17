"""
Query Executor - executes SQL operations using our storage and catalog.
"""
from typing import List, Dict, Any, Optional, Tuple, Iterator
from dataclasses import dataclass
from enum import Enum
import struct
import time

from ..catalog.catalog import Catalog
from ..catalog.schema import TableSchema, ColumnSchema, DataType
from ..storage.storage_manager import StorageManager
from ..index.simple_bplus_tree import SimpleBPlusTree
from ..index.index_manager import IndexManager

class QueryType(Enum):
    """Types of SQL queries."""
    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    CREATE_TABLE = "CREATE_TABLE"
    DROP_TABLE = "DROP_TABLE"
    CREATE_INDEX = "CREATE_INDEX"
    DROP_INDEX = "DROP_INDEX"

@dataclass
class QueryResult:
    """Result of a query execution."""
    success: bool
    message: str
    data: Optional[List[Dict[str, Any]]] = None
    rows_affected: int = 0
    execution_time: float = 0.0

@dataclass
class Row:
    """A single database row."""
    values: List[Any]
    row_id: int = 0  # Physical row identifier
    
    def to_dict(self, columns: List[str]) -> Dict[str, Any]:
        """Convert row to dictionary with column names."""
        return {columns[i]: self.values[i] for i in range(len(columns))}

class QueryExecutor:
    """Executes SQL queries using our storage system."""
    
    def __init__(self, catalog: Catalog, storage_manager: StorageManager):
        self.catalog = catalog
        self.storage = storage_manager
        self.index_manager = IndexManager()
        self.current_transaction = None
    
    def execute(self, query_type: QueryType, **kwargs) -> QueryResult:
        """Execute a query."""
        start_time = time.time()
        
        try:
            if query_type == QueryType.CREATE_TABLE:
                result = self._execute_create_table(**kwargs)
            elif query_type == QueryType.INSERT:
                result = self._execute_insert(**kwargs)
            elif query_type == QueryType.SELECT:
                result = self._execute_select(**kwargs)
            elif query_type == QueryType.UPDATE:
                result = self._execute_update(**kwargs)
            elif query_type == QueryType.DELETE:
                result = self._execute_delete(**kwargs)
            elif query_type == QueryType.DROP_TABLE:
                result = self._execute_drop_table(**kwargs)
            elif query_type == QueryType.CREATE_INDEX:
                result = self._execute_create_index(**kwargs)
            elif query_type == QueryType.DROP_INDEX:
                result = self._execute_drop_index(**kwargs)
            else:
                result = QueryResult(
                    success=False,
                    message=f"Unsupported query type: {query_type}"
                )
            
            result.execution_time = time.time() - start_time
            return result
            
        except Exception as e:
            return QueryResult(
                success=False,
                message=f"Error executing query: {str(e)}",
                execution_time=time.time() - start_time
            )
    
    def _execute_create_table(self, table_name: str, columns: List[ColumnSchema]) -> QueryResult:
        """Execute CREATE TABLE query."""
        try:
            # Create table in catalog
            table_schema = self.catalog.create_table(table_name, columns)
            
            # Create primary key index if needed
            if table_schema.primary_key:
                index_name = f"pk_{table_name}"
                self.index_manager.create_index(index_name)
            
            return QueryResult(
                success=True,
                message=f"Table '{table_name}' created successfully",
                rows_affected=0
            )
        except Exception as e:
            return QueryResult(
                success=False,
                message=f"Failed to create table '{table_name}': {str(e)}"
            )
    
    def _execute_insert(self, table_name: str, values: List[Any]) -> QueryResult:
        """Execute INSERT query."""
        try:
            table_schema = self.catalog.get_table(table_name)
            
            # Validate values count
            if len(values) != len(table_schema.columns):
                return QueryResult(
                    success=False,
                    message=f"Expected {len(table_schema.columns)} values, got {len(values)}"
                )
            
            # Generate row ID (simple auto-increment for now)
            row_id = self._get_next_row_id(table_name)
            
            # Store row
            row = Row(values=values, row_id=row_id)
            self._store_row(table_name, row)
            
            # Update indexes
            self._update_indexes_for_row(table_name, row)
            
            return QueryResult(
                success=True,
                message=f"Row inserted successfully into '{table_name}'",
                rows_affected=1
            )
        except Exception as e:
            return QueryResult(
                success=False,
                message=f"Failed to insert into '{table_name}': {str(e)}"
            )
    
    def _execute_select(self, table_name: str, columns: List[str] = None, 
                       where_clause: Optional[Dict] = None,
                       limit: Optional[int] = None) -> QueryResult:
        """Execute SELECT query."""
        try:
            table_schema = self.catalog.get_table(table_name)
            
            # If columns is None, select all columns
            if columns is None:
                columns = list(table_schema.columns.keys())
            
            # Get all rows (simple full scan for now)
            rows = self._get_all_rows(table_name)
            
            # Apply WHERE clause if provided
            if where_clause:
                rows = self._apply_where_clause(rows, table_schema, where_clause)
            
            # Apply LIMIT if provided
            if limit is not None:
                rows = rows[:limit]
            
            # Convert rows to dictionaries
            data = []
            for row in rows:
                row_dict = {}
                for i, col_name in enumerate(columns):
                    col_index = table_schema.column_index[col_name]
                    row_dict[col_name] = row.values[col_index]
                data.append(row_dict)
            
            return QueryResult(
                success=True,
                message=f"Selected {len(data)} rows from '{table_name}'",
                data=data,
                rows_affected=len(data)
            )
        except Exception as e:
            return QueryResult(
                success=False,
                message=f"Failed to select from '{table_name}': {str(e)}"
            )
    
    def _execute_update(self, table_name: str, set_values: Dict[str, Any],
                       where_clause: Optional[Dict] = None) -> QueryResult:
        """Execute UPDATE query."""
        try:
            table_schema = self.catalog.get_table(table_name)
            
            # Get all rows
            rows = self._get_all_rows(table_name)
            
            # Apply WHERE clause if provided
            if where_clause:
                rows = self._apply_where_clause(rows, table_schema, where_clause)
            
            rows_updated = 0
            
            # Update each matching row
            for row in rows:
                # Update values
                for col_name, new_value in set_values.items():
                    if col_name in table_schema.column_index:
                        col_index = table_schema.column_index[col_name]
                        row.values[col_index] = new_value
                
                # Save updated row
                self._store_row(table_name, row)
                rows_updated += 1
            
            return QueryResult(
                success=True,
                message=f"Updated {rows_updated} rows in '{table_name}'",
                rows_affected=rows_updated
            )
        except Exception as e:
            return QueryResult(
                success=False,
                message=f"Failed to update '{table_name}': {str(e)}"
            )
    
    def _execute_delete(self, table_name: str, 
                       where_clause: Optional[Dict] = None) -> QueryResult:
        """Execute DELETE query."""
        try:
            table_schema = self.catalog.get_table(table_name)
            
            # Get all rows
            rows = self._get_all_rows(table_name)
            
            # Apply WHERE clause if provided
            if where_clause:
                rows = self._apply_where_clause(rows, table_schema, where_clause)
            
            rows_deleted = len(rows)
            
            # Remove rows (simple implementation - mark as deleted)
            for row in rows:
                self._delete_row(table_name, row)
            
            return QueryResult(
                success=True,
                message=f"Deleted {rows_deleted} rows from '{table_name}'",
                rows_affected=rows_deleted
            )
        except Exception as e:
            return QueryResult(
                success=False,
                message=f"Failed to delete from '{table_name}': {str(e)}"
            )
    
    def _execute_drop_table(self, table_name: str) -> QueryResult:
        """Execute DROP TABLE query."""
        try:
            self.catalog.drop_table(table_name)
            return QueryResult(
                success=True,
                message=f"Table '{table_name}' dropped successfully",
                rows_affected=0
            )
        except Exception as e:
            return QueryResult(
                success=False,
                message=f"Failed to drop table '{table_name}': {str(e)}"
            )
    
    def _execute_create_index(self, index_name: str, table_name: str, 
                            column_names: List[str]) -> QueryResult:
        """Execute CREATE INDEX query."""
        try:
            self.index_manager.create_index(index_name)
            
            # Build index from existing data
            table_schema = self.catalog.get_table(table_name)
            rows = self._get_all_rows(table_name)
            
            for row in rows:
                # Extract key from indexed columns
                key_parts = []
                for col_name in column_names:
                    col_index = table_schema.column_index[col_name]
                    key_parts.append(str(row.values[col_index]))
                key = "_".join(key_parts)
                
                # Insert into index
                self.index_manager.insert(index_name, key, row.row_id)
            
            return QueryResult(
                success=True,
                message=f"Index '{index_name}' created on '{table_name}'",
                rows_affected=0
            )
        except Exception as e:
            return QueryResult(
                success=False,
                message=f"Failed to create index '{index_name}': {str(e)}"
            )
    
    def _execute_drop_index(self, index_name: str) -> QueryResult:
        """Execute DROP INDEX query."""
        try:
            self.index_manager.drop_index(index_name)
            return QueryResult(
                success=True,
                message=f"Index '{index_name}' dropped successfully",
                rows_affected=0
            )
        except Exception as e:
            return QueryResult(
                success=False,
                message=f"Failed to drop index '{index_name}': {str(e)}"
            )
    
    # Helper methods
    
    def _get_next_row_id(self, table_name: str) -> int:
        """Get next row ID for a table (simple implementation)."""
        # In a real implementation, this would use a sequence or auto-increment
        rows = self._get_all_rows(table_name)
        if not rows:
            return 1
        return max(row.row_id for row in rows) + 1
    
    def _store_row(self, table_name: str, row: Row) -> None:
        """Store a row (simple in-memory implementation)."""
        # For now, store in memory. Later we'll use the storage manager.
        if not hasattr(self, '_table_data'):
            self._table_data = {}
        
        if table_name not in self._table_data:
            self._table_data[table_name] = []
        
        # Check if row exists
        for i, existing_row in enumerate(self._table_data[table_name]):
            if existing_row.row_id == row.row_id:
                self._table_data[table_name][i] = row
                return
        
        # Add new row
        self._table_data[table_name].append(row)
    
    def _get_all_rows(self, table_name: str) -> List[Row]:
        """Get all rows from a table."""
        if not hasattr(self, '_table_data'):
            self._table_data = {}
        
        return self._table_data.get(table_name, [])
    
    def _delete_row(self, table_name: str, row: Row) -> None:
        """Delete a row."""
        if not hasattr(self, '_table_data'):
            self._table_data = {}
        
        if table_name in self._table_data:
            self._table_data[table_name] = [
                r for r in self._table_data[table_name] 
                if r.row_id != row.row_id
            ]
    
    def _apply_where_clause(self, rows: List[Row], table_schema: TableSchema,
                           where_clause: Dict) -> List[Row]:
        """Apply WHERE clause to filter rows."""
        filtered_rows = []
        
        for row in rows:
            match = True
            
            for col_name, condition in where_clause.items():
                if col_name not in table_schema.column_index:
                    match = False
                    break
                
                col_index = table_schema.column_index[col_name]
                value = row.values[col_index]
                
                # Simple equality condition for now
                if isinstance(condition, tuple):
                    # Handle operators: ('=', value), ('>', value), etc.
                    op, expected = condition
                    if op == '=' and value != expected:
                        match = False
                        break
                    elif op == '!=' and value == expected:
                        match = False
                        break
                    elif op == '>' and not (value > expected):
                        match = False
                        break
                    elif op == '<' and not (value < expected):
                        match = False
                        break
                    elif op == '>=' and not (value >= expected):
                        match = False
                        break
                    elif op == '<=' and not (value <= expected):
                        match = False
                        break
                else:
                    # Simple equality
                    if value != condition:
                        match = False
                        break
            
            if match:
                filtered_rows.append(row)
        
        return filtered_rows
    
    def _update_indexes_for_row(self, table_name: str, row: Row) -> None:
        """Update all indexes for a row."""
        table_schema = self.catalog.get_table(table_name)
        
        for index in self.catalog.get_table_indexes(table_name):
            # Extract key from indexed columns
            key_parts = []
            for col_name in index.column_names:
                col_index = table_schema.column_index[col_name]
                key_parts.append(str(row.values[col_index]))
            key = "_".join(key_parts)
            
            # Insert into index
            self.index_manager.insert(index.index_name, key, row.row_id)