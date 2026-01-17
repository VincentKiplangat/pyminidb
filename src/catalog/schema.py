"""
Schema definitions for tables, columns, and constraints.
"""
from typing import List, Dict, Any, Optional
from enum import Enum
import json

class DataType(Enum):
    """Supported data types."""
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    VARCHAR = "VARCHAR"
    TEXT = "TEXT"
    FLOAT = "FLOAT"
    DOUBLE = "DOUBLE"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    TIMESTAMP = "TIMESTAMP"
    BLOB = "BLOB"

class ColumnConstraint(Enum):
    """Column constraints."""
    PRIMARY_KEY = "PRIMARY KEY"
    UNIQUE = "UNIQUE"
    NOT_NULL = "NOT NULL"
    FOREIGN_KEY = "FOREIGN KEY"

class ColumnSchema:
    """Schema definition for a single column."""
    
    def __init__(self, name: str, data_type: DataType, 
                 constraints: Optional[List[ColumnConstraint]] = None,
                 length: Optional[int] = None,
                 default_value: Any = None):
        self.name = name
        self.data_type = data_type
        self.constraints = constraints or []
        self.length = length  # For VARCHAR types
        self.default_value = default_value
        
        # Derived properties
        self.is_primary_key = ColumnConstraint.PRIMARY_KEY in self.constraints
        self.is_unique = (ColumnConstraint.UNIQUE in self.constraints) or self.is_primary_key
        self.is_not_null = (ColumnConstraint.NOT_NULL in self.constraints) or self.is_primary_key
    
    def __repr__(self) -> str:
        constraints_str = " ".join([c.value for c in self.constraints])
        if self.length:
            return f"{self.name} {self.data_type.value}({self.length}) {constraints_str}"
        return f"{self.name} {self.data_type.value} {constraints_str}"
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization."""
        return {
            "name": self.name,
            "data_type": self.data_type.value,
            "constraints": [c.value for c in self.constraints],
            "length": self.length,
            "default_value": self.default_value
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'ColumnSchema':
        """Create from dictionary."""
        constraints = [ColumnConstraint(c) for c in data.get("constraints", [])]
        return cls(
            name=data["name"],
            data_type=DataType(data["data_type"]),
            constraints=constraints,
            length=data.get("length"),
            default_value=data.get("default_value")
        )
    
    def get_storage_size(self) -> int:
        """Get estimated storage size in bytes."""
        type_sizes = {
            DataType.INTEGER: 4,
            DataType.BIGINT: 8,
            DataType.FLOAT: 4,
            DataType.DOUBLE: 8,
            DataType.BOOLEAN: 1,
            DataType.DATE: 8,
            DataType.TIMESTAMP: 8,
        }
        
        if self.data_type in type_sizes:
            return type_sizes[self.data_type]
        elif self.data_type in [DataType.VARCHAR, DataType.TEXT, DataType.BLOB]:
            # Variable length - return pointer size
            return 8  # Pointer to actual data
        return 8  # Default

class TableSchema:
    """Schema definition for a table."""
    
    def __init__(self, table_name: str, columns: List[ColumnSchema],
                 table_id: int = 0):
        self.table_name = table_name
        self.table_id = table_id
        self.columns = {col.name: col for col in columns}
        
        # Extract primary key columns
        self.primary_key = [col.name for col in columns if col.is_primary_key]
        
        # Build column index map
        self.column_index = {col.name: idx for idx, col in enumerate(columns)}
    
    def __repr__(self) -> str:
        cols = ", ".join([repr(col) for col in self.columns.values()])
        return f"Table: {self.table_name} (id: {self.table_id})\nColumns: {cols}"
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization."""
        return {
            "table_name": self.table_name,
            "table_id": self.table_id,
            "columns": [col.to_dict() for col in self.columns.values()],
            "primary_key": self.primary_key
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'TableSchema':
        """Create from dictionary."""
        columns = [ColumnSchema.from_dict(col) for col in data["columns"]]
        schema = cls(data["table_name"], columns, data.get("table_id", 0))
        return schema
    
    def get_column(self, name: str) -> ColumnSchema:
        """Get column by name."""
        if name not in self.columns:
            raise ValueError(f"Column '{name}' not found in table '{self.table_name}'")
        return self.columns[name]
    
    def get_column_index(self, name: str) -> int:
        """Get column index by name."""
        if name not in self.column_index:
            raise ValueError(f"Column '{name}' not found in table '{self.table_name}'")
        return self.column_index[name]
    
    def get_row_size(self) -> int:
        """Get estimated row size in bytes."""
        # Header + data
        header_size = 8  # Row header (status, next row pointer)
        data_size = sum(col.get_storage_size() for col in self.columns.values())
        return header_size + data_size

class IndexSchema:
    """Schema definition for an index."""
    
    def __init__(self, index_name: str, table_name: str, 
                 column_names: List[str], is_unique: bool = False):
        self.index_name = index_name
        self.table_name = table_name
        self.column_names = column_names
        self.is_unique = is_unique
        self.index_id = 0
    
    def __repr__(self) -> str:
        unique_str = "UNIQUE " if self.is_unique else ""
        return f"{unique_str}INDEX {self.index_name} ON {self.table_name}({', '.join(self.column_names)})"
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization."""
        return {
            "index_name": self.index_name,
            "table_name": self.table_name,
            "column_names": self.column_names,
            "is_unique": self.is_unique,
            "index_id": self.index_id
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'IndexSchema':
        """Create from dictionary."""
        index = cls(
            data["index_name"],
            data["table_name"],
            data["column_names"],
            data.get("is_unique", False)
        )
        index.index_id = data.get("index_id", 0)
        return index