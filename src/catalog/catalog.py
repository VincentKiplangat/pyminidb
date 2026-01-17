"""
Catalog manager - stores metadata about tables, columns, and indexes.
"""
import json
from typing import Dict, List, Optional, Set
from pathlib import Path
from .schema import TableSchema, IndexSchema, ColumnSchema, DataType

class Catalog:
    """System catalog storing all database metadata."""
    
    def __init__(self, storage_manager=None):
        self.storage_manager = storage_manager
        self.tables: Dict[str, TableSchema] = {}
        self.indexes: Dict[str, IndexSchema] = {}
        self.table_counter = 1  # Start from 1 (0 is invalid)
        self.index_counter = 1
        
        # In-memory mappings for fast lookup
        self.table_name_to_id: Dict[str, int] = {}
        self.index_name_to_id: Dict[str, int] = {}
        
    def create_table(self, table_name: str, columns: List[ColumnSchema]) -> TableSchema:
        """Create a new table schema."""
        if table_name in self.tables:
            raise ValueError(f"Table '{table_name}' already exists")
        
        # Validate columns
        self._validate_table_creation(table_name, columns)
        
        # Create schema
        table_id = self.table_counter
        self.table_counter += 1
        
        schema = TableSchema(table_name, columns, table_id)
        self.tables[table_name] = schema
        self.table_name_to_id[table_name] = table_id
        
        # Create primary key index if needed
        if schema.primary_key:
            index_name = f"pk_{table_name}"
            index = IndexSchema(index_name, table_name, schema.primary_key, True)
            self.create_index(index)
        
        print(f"Created table: {table_name} (id: {table_id})")
        return schema
    
    def drop_table(self, table_name: str) -> None:
        """Drop a table and all its indexes."""
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist")
        
        # Drop all indexes for this table
        indexes_to_drop = [
            idx_name for idx_name, idx in self.indexes.items()
            if idx.table_name == table_name
        ]
        
        for idx_name in indexes_to_drop:
            self.drop_index(idx_name)
        
        # Remove table
        del self.tables[table_name]
        del self.table_name_to_id[table_name]
        
        print(f"Dropped table: {table_name}")
    
    def create_index(self, index_schema: IndexSchema) -> None:
        """Create a new index."""
        if index_schema.index_name in self.indexes:
            raise ValueError(f"Index '{index_schema.index_name}' already exists")
        
        # Verify table exists
        if index_schema.table_name not in self.tables:
            raise ValueError(f"Table '{index_schema.table_name}' does not exist")
        
        # Verify columns exist
        table_schema = self.tables[index_schema.table_name]
        for col_name in index_schema.column_names:
            if col_name not in table_schema.columns:
                raise ValueError(f"Column '{col_name}' does not exist in table '{index_schema.table_name}'")
        
        # Assign ID
        index_schema.index_id = self.index_counter
        self.index_counter += 1
        
        self.indexes[index_schema.index_name] = index_schema
        self.index_name_to_id[index_schema.index_name] = index_schema.index_id
        
        print(f"Created index: {index_schema.index_name} on {index_schema.table_name}")
    
    def drop_index(self, index_name: str) -> None:
        """Drop an index."""
        if index_name not in self.indexes:
            raise ValueError(f"Index '{index_name}' does not exist")
        
        del self.indexes[index_name]
        del self.index_name_to_id[index_name]
        
        print(f"Dropped index: {index_name}")
    
    def get_table(self, table_name: str) -> TableSchema:
        """Get table schema by name."""
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' does not exist")
        return self.tables[table_name]
    
    def get_index(self, index_name: str) -> IndexSchema:
        """Get index schema by name."""
        if index_name not in self.indexes:
            raise ValueError(f"Index '{index_name}' does not exist")
        return self.indexes[index_name]
    
    def get_table_indexes(self, table_name: str) -> List[IndexSchema]:
        """Get all indexes for a table."""
        return [
            idx for idx in self.indexes.values()
            if idx.table_name == table_name
        ]
    
    def _validate_table_creation(self, table_name: str, columns: List[ColumnSchema]) -> None:
        """Validate table creation parameters."""
        if not table_name:
            raise ValueError("Table name cannot be empty")
        
        if not columns:
            raise ValueError("Table must have at least one column")
        
        # Check for duplicate column names
        col_names = [col.name for col in columns]
        if len(col_names) != len(set(col_names)):
            raise ValueError("Duplicate column names")
        
        # Validate primary key
        pk_columns = [col for col in columns if col.is_primary_key]
        if len(pk_columns) > 1:
            # Composite primary key is OK
            pass
        elif len(pk_columns) == 1:
            # Single column PK - verify data type supports indexing
            pk_col = pk_columns[0]
            if pk_col.data_type in [DataType.TEXT, DataType.BLOB]:
                raise ValueError(f"Primary key cannot be of type {pk_col.data_type.value}")
    
    def serialize(self) -> bytes:
        """Serialize catalog to bytes."""
        catalog_data = {
            "tables": [table.to_dict() for table in self.tables.values()],
            "indexes": [idx.to_dict() for idx in self.indexes.values()],
            "table_counter": self.table_counter,
            "index_counter": self.index_counter
        }
        return json.dumps(catalog_data, indent=2).encode('utf-8')
    
    def deserialize(self, data: bytes) -> None:
        """Deserialize catalog from bytes."""
        catalog_data = json.loads(data.decode('utf-8'))
        
        self.tables.clear()
        self.indexes.clear()
        self.table_name_to_id.clear()
        self.index_name_to_id.clear()
        
        # Load tables
        for table_dict in catalog_data.get("tables", []):
            schema = TableSchema.from_dict(table_dict)
            self.tables[schema.table_name] = schema
            self.table_name_to_id[schema.table_name] = schema.table_id
        
        # Load indexes
        for index_dict in catalog_data.get("indexes", []):
            index = IndexSchema.from_dict(index_dict)
            self.indexes[index.index_name] = index
            self.index_name_to_id[index.index_name] = index.index_id
        
        self.table_counter = catalog_data.get("table_counter", 1)
        self.index_counter = catalog_data.get("index_counter", 1)