"""
Index manager for the database.
"""
from typing import Dict, Any, Optional, List
from .simple_bplus_tree import SimpleBPlusTree

class IndexManager:
    """Manages all indexes in the database."""
    
    def __init__(self):
        self.indexes: Dict[str, SimpleBPlusTree] = {}
        self.index_counter = 0
    
    def create_index(self, index_name: str, is_unique: bool = False) -> str:
        """Create a new index."""
        if index_name in self.indexes:
            raise ValueError(f"Index '{index_name}' already exists")
        
        tree = SimpleBPlusTree(order=4)
        self.indexes[index_name] = tree
        return index_name
    
    def drop_index(self, index_name: str) -> None:
        """Drop an index."""
        if index_name not in self.indexes:
            raise ValueError(f"Index '{index_name}' does not exist")
        
        del self.indexes[index_name]
    
    def insert(self, index_name: str, key: Any, value: Any) -> None:
        """Insert a key-value pair into an index."""
        if index_name not in self.indexes:
            raise ValueError(f"Index '{index_name}' does not exist")
        
        self.indexes[index_name].insert(key, value)
    
    def search(self, index_name: str, key: Any) -> Optional[Any]:
        """Search for a key in an index."""
        if index_name not in self.indexes:
            raise ValueError(f"Index '{index_name}' does not exist")
        
        return self.indexes[index_name].search(key)
    
    def delete(self, index_name: str, key: Any) -> bool:
        """Delete a key from an index."""
        if index_name not in self.indexes:
            raise ValueError(f"Index '{index_name}' does not exist")
        
        return self.indexes[index_name].delete(key)
    
    def range_search(self, index_name: str, start_key: Any, end_key: Any) -> List[Any]:
        """Search for keys in range in an index."""
        if index_name not in self.indexes:
            raise ValueError(f"Index '{index_name}' does not exist")
        
        return self.indexes[index_name].range_search(start_key, end_key)
    
    def get_all_indexes(self) -> List[str]:
        """Get all index names."""
        return list(self.indexes.keys())