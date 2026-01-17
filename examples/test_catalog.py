"""
Test the catalog system.
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.catalog.schema import ColumnSchema, DataType, ColumnConstraint, TableSchema
from src.catalog.catalog import Catalog

def test_catalog():
    """Test catalog operations."""
    catalog = Catalog()
    
    # Create a users table
    users_columns = [
        ColumnSchema("id", DataType.INTEGER, [ColumnConstraint.PRIMARY_KEY]),
        ColumnSchema("username", DataType.VARCHAR, [ColumnConstraint.UNIQUE, ColumnConstraint.NOT_NULL], length=50),
        ColumnSchema("email", DataType.VARCHAR, [ColumnConstraint.UNIQUE], length=100),
        ColumnSchema("age", DataType.INTEGER),
        ColumnSchema("created_at", DataType.TIMESTAMP)
    ]
    
    users_table = catalog.create_table("users", users_columns)
    print(f"\nCreated table:\n{users_table}")
    
    # Create a posts table
    posts_columns = [
        ColumnSchema("id", DataType.INTEGER, [ColumnConstraint.PRIMARY_KEY]),
        ColumnSchema("user_id", DataType.INTEGER, [ColumnConstraint.NOT_NULL]),
        ColumnSchema("title", DataType.VARCHAR, [ColumnConstraint.NOT_NULL], length=200),
        ColumnSchema("content", DataType.TEXT),
        ColumnSchema("published", DataType.BOOLEAN, default_value=False)
    ]
    
    posts_table = catalog.create_table("posts", posts_columns)
    print(f"\nCreated table:\n{posts_table}")
    
    # Create an index
    from src.catalog.schema import IndexSchema
    title_index = IndexSchema("idx_posts_title", "posts", ["title"])
    catalog.create_index(title_index)
    
    # Get table information
    print("\n--- Table Information ---")
    retrieved_table = catalog.get_table("users")
    print(f"Users table columns: {list(retrieved_table.columns.keys())}")
    print(f"Primary key: {retrieved_table.primary_key}")
    
    # Get indexes for posts
    print("\n--- Indexes for 'posts' ---")
    for idx in catalog.get_table_indexes("posts"):
        print(f"  - {idx}")
    
    # Serialize catalog
    print("\n--- Serialized Catalog ---")
    serialized = catalog.serialize()
    print(f"Size: {len(serialized)} bytes")
    
    # Deserialize into new catalog
    new_catalog = Catalog()
    new_catalog.deserialize(serialized)
    
    print(f"\nDeserialized - Tables: {list(new_catalog.tables.keys())}")
    print(f"Deserialized - Indexes: {list(new_catalog.indexes.keys())}")

if __name__ == "__main__":
    test_catalog()