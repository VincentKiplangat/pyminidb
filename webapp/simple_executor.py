"""
Simple query executor for web application.
"""
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum
import time

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

class SimpleQueryExecutor:
    """Simple in-memory query executor for web app."""
    
    def __init__(self):
        self.tables = {}
        self.table_schemas = {}
    
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
    
    def _execute_create_table(self, table_name: str, columns: List) -> QueryResult:
        """Create a table."""
        if table_name in self.tables:
            return QueryResult(
                success=False,
                message=f"Table '{table_name}' already exists"
            )
        
        self.tables[table_name] = []
        self.table_schemas[table_name] = columns
        
        return QueryResult(
            success=True,
            message=f"Table '{table_name}' created successfully",
            rows_affected=0
        )
    
    def _execute_insert(self, table_name: str, values: List[Any]) -> QueryResult:
        """Insert data into a table."""
        if table_name not in self.tables:
            return QueryResult(
                success=False,
                message=f"Table '{table_name}' does not exist"
            )
        
        # Generate row ID
        row_id = len(self.tables[table_name]) + 1
        
        # Create row
        row = {'id': row_id}
        if len(values) <= len(self.table_schemas[table_name]):
            for i, value in enumerate(values):
                if i < len(self.table_schemas[table_name]):
                    col_name = self.table_schemas[table_name][i].name
                    row[col_name] = value
        
        self.tables[table_name].append(row)
        
        return QueryResult(
            success=True,
            message=f"Row inserted into '{table_name}'",
            rows_affected=1
        )
    
    def _execute_select(self, table_name: str, columns: List[str] = None,
                       where_clause: Optional[Dict] = None, limit: Optional[int] = None) -> QueryResult:
        """Select data from a table."""
        if table_name not in self.tables:
            return QueryResult(
                success=False,
                message=f"Table '{table_name}' does not exist"
            )
        
        rows = self.tables[table_name]
        
        # Apply WHERE clause
        if where_clause:
            filtered_rows = []
            for row in rows:
                match = True
                for col, condition in where_clause.items():
                    if col in row and row[col] != condition:
                        match = False
                        break
                if match:
                    filtered_rows.append(row)
            rows = filtered_rows
        
        # Apply column selection
        if columns:
            selected_rows = []
            for row in rows:
                selected_row = {}
                for col in columns:
                    if col in row:
                        selected_row[col] = row[col]
                selected_rows.append(selected_row)
            rows = selected_rows
        
        # Apply LIMIT
        if limit is not None:
            rows = rows[:limit]
        
        return QueryResult(
            success=True,
            message=f"Selected {len(rows)} rows from '{table_name}'",
            data=rows,
            rows_affected=len(rows)
        )
    
    def _execute_update(self, table_name: str, set_values: Dict[str, Any],
                       where_clause: Optional[Dict] = None) -> QueryResult:
        """Update data in a table."""
        if table_name not in self.tables:
            return QueryResult(
                success=False,
                message=f"Table '{table_name}' does not exist"
            )
        
        rows_updated = 0
        
        for row in self.tables[table_name]:
            match = True
            if where_clause:
                for col, condition in where_clause.items():
                    if col in row and row[col] != condition:
                        match = False
                        break
            
            if match:
                for col, value in set_values.items():
                    row[col] = value
                rows_updated += 1
        
        return QueryResult(
            success=True,
            message=f"Updated {rows_updated} rows in '{table_name}'",
            rows_affected=rows_updated
        )
    
    def _execute_delete(self, table_name: str, where_clause: Optional[Dict] = None) -> QueryResult:
        """Delete data from a table."""
        if table_name not in self.tables:
            return QueryResult(
                success=False,
                message=f"Table '{table_name}' does not exist"
            )
        
        rows_to_keep = []
        rows_deleted = 0
        
        for row in self.tables[table_name]:
            match = True
            if where_clause:
                for col, condition in where_clause.items():
                    if col in row and row[col] != condition:
                        match = False
                        break
            
            if match:
                rows_deleted += 1
            else:
                rows_to_keep.append(row)
        
        self.tables[table_name] = rows_to_keep
        
        return QueryResult(
            success=True,
            message=f"Deleted {rows_deleted} rows from '{table_name}'",
            rows_affected=rows_deleted
        )
    
    def _execute_drop_table(self, table_name: str) -> QueryResult:
        """Drop a table."""
        if table_name not in self.tables:
            return QueryResult(
                success=False,
                message=f"Table '{table_name}' does not exist"
            )
        
        del self.tables[table_name]
        if table_name in self.table_schemas:
            del self.table_schemas[table_name]
        
        return QueryResult(
            success=True,
            message=f"Table '{table_name}' dropped successfully",
            rows_affected=0
        )