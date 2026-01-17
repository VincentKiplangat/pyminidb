"""
Simple SQL interface to convert SQL strings to QueryExecutor calls.
"""
from typing import List, Dict, Any, Optional, Tuple
from enum import Enum
import re

from ..executor.query_executor import QueryExecutor, QueryType, QueryResult
from ..catalog.schema import ColumnSchema, DataType, ColumnConstraint

class SimpleSQLParser:
    """Simple SQL parser for basic SQL operations."""
    
    def __init__(self, query_executor: QueryExecutor):
        self.executor = query_executor
    
    def parse_execute(self, sql: str) -> QueryResult:
        """Parse and execute SQL statement."""
        sql = sql.strip()
        
        if not sql.endswith(';'):
            sql = sql + ';'
        
        # Remove trailing semicolon for parsing
        sql = sql.rstrip(';').strip()
        
        # Convert to uppercase for keyword matching
        sql_upper = sql.upper()
        
        if sql_upper.startswith('CREATE TABLE'):
            return self._parse_create_table(sql)
        elif sql_upper.startswith('INSERT INTO'):
            return self._parse_insert(sql)
        elif sql_upper.startswith('SELECT'):
            return self._parse_select(sql)
        elif sql_upper.startswith('UPDATE'):
            return self._parse_update(sql)
        elif sql_upper.startswith('DELETE FROM'):
            return self._parse_delete(sql)
        elif sql_upper.startswith('DROP TABLE'):
            return self._parse_drop_table(sql)
        elif sql_upper.startswith('CREATE INDEX'):
            return self._parse_create_index(sql)
        elif sql_upper.startswith('DROP INDEX'):
            return self._parse_drop_index(sql)
        else:
            return QueryResult(
                success=False,
                message=f"Unsupported SQL statement: {sql}"
            )
    
    def _parse_create_table(self, sql: str) -> QueryResult:
        """Parse CREATE TABLE statement."""
        # Simple regex for CREATE TABLE
        pattern = r'CREATE TABLE (\w+)\s*\((.*)\)'
        match = re.match(pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if not match:
            return QueryResult(
                success=False,
                message="Invalid CREATE TABLE syntax"
            )
        
        table_name = match.group(1)
        columns_sql = match.group(2).strip()
        
        # Parse column definitions
        columns = self._parse_column_definitions(columns_sql)
        
        if not columns:
            return QueryResult(
                success=False,
                message="No valid columns found in CREATE TABLE"
            )
        
        return self.executor.execute(
            QueryType.CREATE_TABLE,
            table_name=table_name,
            columns=columns
        )
    
    def _parse_column_definitions(self, columns_sql: str) -> List[ColumnSchema]:
        """Parse column definitions from SQL."""
        columns = []
        
        # Split by commas, but handle parentheses for complex types
        parts = []
        current = ""
        paren_depth = 0
        
        for char in columns_sql:
            if char == '(':
                paren_depth += 1
                current += char
            elif char == ')':
                paren_depth -= 1
                current += char
            elif char == ',' and paren_depth == 0:
                parts.append(current.strip())
                current = ""
            else:
                current += char
        
        if current.strip():
            parts.append(current.strip())
        
        for part in parts:
            column = self._parse_column_definition(part)
            if column:
                columns.append(column)
        
        return columns
    
    def _parse_column_definition(self, col_sql: str) -> Optional[ColumnSchema]:
        """Parse a single column definition."""
        col_sql = col_sql.strip()
        
        # Split into tokens
        tokens = col_sql.split()
        if len(tokens) < 2:
            return None
        
        column_name = tokens[0]
        
        # Parse data type
        data_type_str = tokens[1].upper()
        
        # Handle VARCHAR(size)
        length = None
        if '(' in data_type_str and ')' in data_type_str:
            type_name = data_type_str.split('(')[0]
            size_str = data_type_str.split('(')[1].rstrip(')')
            try:
                length = int(size_str)
            except ValueError:
                length = None
            data_type_str = type_name
        
        # Map to DataType enum
        type_map = {
            'INTEGER': DataType.INTEGER,
            'INT': DataType.INTEGER,
            'BIGINT': DataType.BIGINT,
            'VARCHAR': DataType.VARCHAR,
            'TEXT': DataType.TEXT,
            'STRING': DataType.TEXT,
            'FLOAT': DataType.FLOAT,
            'DOUBLE': DataType.DOUBLE,
            'BOOLEAN': DataType.BOOLEAN,
            'BOOL': DataType.BOOLEAN,
            'DATE': DataType.DATE,
            'TIMESTAMP': DataType.TIMESTAMP,
        }
        
        if data_type_str not in type_map:
            return None
        
        data_type = type_map[data_type_str]
        
        # Parse constraints
        constraints = []
        for token in tokens[2:]:
            token_upper = token.upper()
            if token_upper == 'PRIMARY':
                if 'KEY' in tokens[tokens.index(token) + 1:]:
                    constraints.append(ColumnConstraint.PRIMARY_KEY)
            elif token_upper == 'UNIQUE':
                constraints.append(ColumnConstraint.UNIQUE)
            elif token_upper == 'NOT' and 'NULL' in tokens[tokens.index(token) + 1:]:
                constraints.append(ColumnConstraint.NOT_NULL)
        
        return ColumnSchema(
            name=column_name,
            data_type=data_type,
            constraints=constraints,
            length=length
        )
    
    def _parse_insert(self, sql: str) -> QueryResult:
        """Parse INSERT statement."""
        # Simple regex for INSERT
        pattern = r'INSERT INTO (\w+)\s*(?:\(([^)]+)\))?\s*VALUES\s*\(([^)]+)\)'
        match = re.match(pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if not match:
            return QueryResult(
                success=False,
                message="Invalid INSERT syntax"
            )
        
        table_name = match.group(1)
        columns_str = match.group(2)
        values_str = match.group(3)
        
        # Parse values
        values = self._parse_value_list(values_str)
        
        return self.executor.execute(
            QueryType.INSERT,
            table_name=table_name,
            values=values
        )
    
    def _parse_select(self, sql: str) -> QueryResult:
        """Parse SELECT statement."""
        # Simple regex for SELECT
        pattern = r'SELECT\s+(.+?)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+?))?(?:\s+LIMIT\s+(\d+))?'
        match = re.match(pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if not match:
            return QueryResult(
                success=False,
                message="Invalid SELECT syntax"
            )
        
        columns_str = match.group(1).strip()
        table_name = match.group(2).strip()
        where_str = match.group(3)
        limit_str = match.group(4)
        
        # Parse columns
        if columns_str == '*':
            columns = None  # Select all columns
        else:
            columns = [col.strip() for col in columns_str.split(',')]
        
        # Parse WHERE clause
        where_clause = None
        if where_str:
            where_clause = self._parse_where_clause(where_str.strip())
        
        # Parse LIMIT
        limit = None
        if limit_str:
            try:
                limit = int(limit_str.strip())
            except ValueError:
                pass
        
        return self.executor.execute(
            QueryType.SELECT,
            table_name=table_name,
            columns=columns,
            where_clause=where_clause,
            limit=limit
        )
    
    def _parse_update(self, sql: str) -> QueryResult:
        """Parse UPDATE statement."""
        # Simple regex for UPDATE
        pattern = r'UPDATE\s+(\w+)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?'
        match = re.match(pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if not match:
            return QueryResult(
                success=False,
                message="Invalid UPDATE syntax"
            )
        
        table_name = match.group(1).strip()
        set_str = match.group(2).strip()
        where_str = match.group(3)
        
        # Parse SET clause
        set_values = self._parse_set_clause(set_str)
        
        # Parse WHERE clause
        where_clause = None
        if where_str:
            where_clause = self._parse_where_clause(where_str.strip())
        
        return self.executor.execute(
            QueryType.UPDATE,
            table_name=table_name,
            set_values=set_values,
            where_clause=where_clause
        )
    
    def _parse_delete(self, sql: str) -> QueryResult:
        """Parse DELETE statement."""
        # Simple regex for DELETE
        pattern = r'DELETE FROM\s+(\w+)(?:\s+WHERE\s+(.+))?'
        match = re.match(pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if not match:
            return QueryResult(
                success=False,
                message="Invalid DELETE syntax"
            )
        
        table_name = match.group(1).strip()
        where_str = match.group(2)
        
        # Parse WHERE clause
        where_clause = None
        if where_str:
            where_clause = self._parse_where_clause(where_str.strip())
        
        return self.executor.execute(
            QueryType.DELETE,
            table_name=table_name,
            where_clause=where_clause
        )
    
    def _parse_drop_table(self, sql: str) -> QueryResult:
        """Parse DROP TABLE statement."""
        pattern = r'DROP TABLE\s+(\w+)'
        match = re.match(pattern, sql, re.IGNORECASE)
        
        if not match:
            return QueryResult(
                success=False,
                message="Invalid DROP TABLE syntax"
            )
        
        table_name = match.group(1).strip()
        
        return self.executor.execute(
            QueryType.DROP_TABLE,
            table_name=table_name
        )
    
    def _parse_create_index(self, sql: str) -> QueryResult:
        """Parse CREATE INDEX statement."""
        pattern = r'CREATE INDEX\s+(\w+)\s+ON\s+(\w+)\s*\(([^)]+)\)'
        match = re.match(pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if not match:
            return QueryResult(
                success=False,
                message="Invalid CREATE INDEX syntax"
            )
        
        index_name = match.group(1).strip()
        table_name = match.group(2).strip()
        columns_str = match.group(3).strip()
        
        columns = [col.strip() for col in columns_str.split(',')]
        
        return self.executor.execute(
            QueryType.CREATE_INDEX,
            index_name=index_name,
            table_name=table_name,
            column_names=columns
        )
    
    def _parse_drop_index(self, sql: str) -> QueryResult:
        """Parse DROP INDEX statement."""
        pattern = r'DROP INDEX\s+(\w+)'
        match = re.match(pattern, sql, re.IGNORECASE)
        
        if not match:
            return QueryResult(
                success=False,
                message="Invalid DROP INDEX syntax"
            )
        
        index_name = match.group(1).strip()
        
        return self.executor.execute(
            QueryType.DROP_INDEX,
            index_name=index_name
        )
    
    def _parse_value_list(self, values_str: str) -> List[Any]:
        """Parse a list of values from SQL."""
        values = []
        current = ""
        in_quotes = False
        quote_char = None
        
        for char in values_str:
            if char in "'\"" and (quote_char is None or char == quote_char):
                if in_quotes:
                    in_quotes = False
                    quote_char = None
                else:
                    in_quotes = True
                    quote_char = char
                current += char
            elif char == ',' and not in_quotes:
                values.append(self._parse_value(current.strip()))
                current = ""
            else:
                current += char
        
        if current.strip():
            values.append(self._parse_value(current.strip()))
        
        return values
    
    def _parse_value(self, value_str: str) -> Any:
        """Parse a single SQL value."""
        value_str = value_str.strip()
        
        # Remove quotes
        if (value_str.startswith("'") and value_str.endswith("'")) or \
           (value_str.startswith('"') and value_str.endswith('"')):
            return value_str[1:-1]
        
        # Parse numbers
        if value_str.replace('.', '', 1).replace('-', '', 1).isdigit():
            if '.' in value_str:
                return float(value_str)
            else:
                return int(value_str)
        
        # Parse booleans
        if value_str.upper() in ['TRUE', 'FALSE']:
            return value_str.upper() == 'TRUE'
        
        # Parse NULL
        if value_str.upper() == 'NULL':
            return None
        
        return value_str
    
    def _parse_set_clause(self, set_str: str) -> Dict[str, Any]:
        """Parse SET clause of UPDATE statement."""
        set_values = {}
        
        # Split by commas, but handle quoted strings
        parts = []
        current = ""
        in_quotes = False
        quote_char = None
        
        for char in set_str:
            if char in "'\"" and (quote_char is None or char == quote_char):
                in_quotes = not in_quotes
                if not in_quotes:
                    quote_char = None
                else:
                    quote_char = char
                current += char
            elif char == ',' and not in_quotes:
                parts.append(current.strip())
                current = ""
            else:
                current += char
        
        if current.strip():
            parts.append(current.strip())
        
        # Parse each assignment
        for part in parts:
            if '=' in part:
                col, val = part.split('=', 1)
                col = col.strip()
                val = self._parse_value(val.strip())
                set_values[col] = val
        
        return set_values
    
    def _parse_where_clause(self, where_str: str) -> Dict[str, Any]:
        """Parse WHERE clause."""
        where_clause = {}
        
        # Simple equality for now: column = value
        if '=' in where_str:
            left, right = where_str.split('=', 1)
            left = left.strip()
            right = self._parse_value(right.strip())
            where_clause[left] = right
        
        # Handle operators
        for op in ['!=', '>=', '<=', '>', '<']:
            if op in where_str:
                left, right = where_str.split(op, 1)
                left = left.strip()
                right = self._parse_value(right.strip())
                where_clause[left] = (op, right)
                break
        
        return where_clause