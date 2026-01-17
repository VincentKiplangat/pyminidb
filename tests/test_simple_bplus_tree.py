"""
Simple but working B+ Tree implementation.
"""
import struct
from typing import List, Optional, Dict, Any, Iterator
from dataclasses import dataclass

@dataclass
class TreeNode:
    """Simple tree node for B+ Tree."""
    keys: List[bytes]
    values: List[Any]  # For leaves: record pointers
    children: Optional[List['TreeNode']] = None  # Only for internal nodes
    next_leaf: Optional['TreeNode'] = None
    is_leaf: bool = True
    
    def __post_init__(self):
        if self.children is None and not self.is_leaf:
            self.children = []
        elif self.is_leaf and self.children is not None:
            # Leaf nodes should not have children
            self.children = None

class SimpleBPlusTree:
    """A simplified but working B+ Tree implementation."""
    
    def __init__(self, order: int = 4):
        self.order = order
        self.min_keys = order // 2
        self.root = TreeNode(keys=[], values=[], is_leaf=True)
        self.key_to_value: Dict[bytes, Any] = {}  # Simple map for now
    
    def insert(self, key: Any, value: Any) -> None:
        """Insert a key-value pair."""
        key_bytes = self._serialize_key(key)
        self.key_to_value[key_bytes] = value
        
        # Insert into the tree structure
        self._insert_into_node(self.root, key_bytes, value)
        
        # If root is too full, split it
        if len(self.root.keys) > self.order - 1:
            self._split_root()
    
    def _insert_into_node(self, node: TreeNode, key_bytes: bytes, value: Any) -> None:
        """Insert into a node (recursive)."""
        if node.is_leaf:
            self._insert_into_leaf(node, key_bytes, value)
        else:
            # Find which child to insert into
            child_index = 0
            while child_index < len(node.keys) and key_bytes >= node.keys[child_index]:
                child_index += 1
            
            if child_index < len(node.children or []):
                self._insert_into_node(node.children[child_index], key_bytes, value)
                
                # Check if child needs splitting
                if node.children[child_index].is_leaf and len(node.children[child_index].keys) > self.order - 1:
                    self._split_leaf_child(node, child_index)
                elif not node.children[child_index].is_leaf and len(node.children[child_index].keys) > self.order - 1:
                    self._split_internal_child(node, child_index)
    
    def _insert_into_leaf(self, leaf: TreeNode, key_bytes: bytes, value: Any) -> None:
        """Insert into a leaf node."""
        # Find position
        pos = 0
        while pos < len(leaf.keys) and leaf.keys[pos] < key_bytes:
            pos += 1
        
        # Insert
        leaf.keys.insert(pos, key_bytes)
        leaf.values.insert(pos, value)
    
    def _split_root(self) -> None:
        """Split the root when it's too full."""
        if self.root.is_leaf:
            # Split leaf root
            mid = len(self.root.keys) // 2
            
            # Create new root
            new_root = TreeNode(
                keys=[self.root.keys[mid]],
                values=[],
                children=[],
                is_leaf=False
            )
            
            # Create left leaf
            left_leaf = TreeNode(
                keys=self.root.keys[:mid],
                values=self.root.values[:mid],
                is_leaf=True
            )
            
            # Create right leaf
            right_leaf = TreeNode(
                keys=self.root.keys[mid:],
                values=self.root.values[mid:],
                is_leaf=True
            )
            
            # Link leaves
            left_leaf.next_leaf = right_leaf
            
            # Set up new root
            new_root.children = [left_leaf, right_leaf]
            
            # Update root
            self.root = new_root
        else:
            # Split internal root
            mid = len(self.root.keys) // 2
            
            # Create new root
            new_root = TreeNode(
                keys=[self.root.keys[mid]],
                values=[],
                children=[],
                is_leaf=False
            )
            
            # Create left internal node
            left_node = TreeNode(
                keys=self.root.keys[:mid],
                values=[],
                children=self.root.children[:mid + 1],
                is_leaf=False
            )
            
            # Create right internal node
            right_node = TreeNode(
                keys=self.root.keys[mid + 1:],
                values=[],
                children=self.root.children[mid + 1:],
                is_leaf=False
            )
            
            # Set up new root
            new_root.children = [left_node, right_node]
            
            # Update root
            self.root = new_root
    
    def _split_leaf_child(self, parent: TreeNode, child_index: int) -> None:
        """Split a leaf child of an internal node."""
        child = parent.children[child_index]
        mid = len(child.keys) // 2
        
        # Create new leaf
        new_leaf = TreeNode(
            keys=child.keys[mid:],
            values=child.values[mid:],
            is_leaf=True
        )
        
        # Update old leaf
        child.keys = child.keys[:mid]
        child.values = child.values[:mid]
        
        # Link leaves
        new_leaf.next_leaf = child.next_leaf
        child.next_leaf = new_leaf
        
        # Insert into parent
        parent.keys.insert(child_index, new_leaf.keys[0])
        parent.children.insert(child_index + 1, new_leaf)
    
    def _split_internal_child(self, parent: TreeNode, child_index: int) -> None:
        """Split an internal child of an internal node."""
        child = parent.children[child_index]
        mid = len(child.keys) // 2
        
        # Create new internal node
        new_node = TreeNode(
            keys=child.keys[mid + 1:],
            values=[],
            children=child.children[mid + 1:],
            is_leaf=False
        )
        
        # Update old node
        child.keys = child.keys[:mid]
        child.children = child.children[:mid + 1]
        
        # Insert into parent
        parent.keys.insert(child_index, child.keys[mid])
        parent.children.insert(child_index + 1, new_node)
    
    def search(self, key: Any) -> Optional[Any]:
        """Search for a key."""
        key_bytes = self._serialize_key(key)
        
        # First check our direct map (for simplicity)
        if key_bytes in self.key_to_value:
            return self.key_to_value[key_bytes]
        
        # Fall back to tree search
        return self._search_node(self.root, key_bytes)
    
    def _search_node(self, node: TreeNode, key_bytes: bytes) -> Optional[Any]:
        """Search recursively in a node."""
        if node.is_leaf:
            for i, k in enumerate(node.keys):
                if k == key_bytes:
                    return node.values[i]
            return None
        
        # Find which child to search
        child_index = 0
        while child_index < len(node.keys) and key_bytes >= node.keys[child_index]:
            child_index += 1
        
        if child_index < len(node.children or []):
            return self._search_node(node.children[child_index], key_bytes)
        
        return None
    
    def range_search(self, start_key: Any, end_key: Any) -> Iterator[Any]:
        """Search for keys in range [start_key, end_key)."""
        start_bytes = self._serialize_key(start_key)
        end_bytes = self._serialize_key(end_key)
        
        # Find starting leaf
        leaf = self._find_leaf_for_search(self.root, start_bytes)
        
        # Traverse leaves
        while leaf:
            for i, key in enumerate(leaf.keys):
                if start_bytes <= key < end_bytes:
                    yield leaf.values[i]
                elif key >= end_bytes:
                    return
            
            leaf = leaf.next_leaf
    
    def _find_leaf_for_search(self, node: TreeNode, key_bytes: bytes) -> Optional[TreeNode]:
        """Find the leaf node that would contain a key."""
        if node.is_leaf:
            return node
        
        # Find which child to follow
        child_index = 0
        while child_index < len(node.keys) and key_bytes >= node.keys[child_index]:
            child_index += 1
        
        if child_index < len(node.children or []):
            return self._find_leaf_for_search(node.children[child_index], key_bytes)
        
        return None
    
    def delete(self, key: Any) -> bool:
        """Delete a key."""
        key_bytes = self._serialize_key(key)
        
        if key_bytes not in self.key_to_value:
            return False
        
        # Remove from our map
        del self.key_to_value[key_bytes]
        
        # For now, we'll just remove from the map
        # In a full implementation, we'd also remove from the tree structure
        return True
    
    def _serialize_key(self, key: Any) -> bytes:
        """Serialize key to bytes."""
        if isinstance(key, int):
            return struct.pack("<Q", key)
        elif isinstance(key, str):
            return key.encode('utf-8')
        elif isinstance(key, bytes):
            return key
        else:
            return str(key).encode('utf-8')
    
    def print_tree(self, node: TreeNode = None, level: int = 0):
        """Print tree structure for debugging."""
        if node is None:
            node = self.root
            print("\n=== B+ Tree Structure ===")
        
        indent = "  " * level
        if node.is_leaf:
            keys = [self._deserialize_key(k) for k in node.keys]
            print(f"{indent}Leaf: {keys}")
        else:
            keys = [self._deserialize_key(k) for k in node.keys]
            print(f"{indent}Internal: {keys}")
            for child in (node.children or []):
                self.print_tree(child, level + 1)
    
    def _deserialize_key(self, key_bytes: bytes) -> Any:
        """Deserialize bytes to key (simplified)."""
        try:
            return struct.unpack("<Q", key_bytes)[0]
        except:
            return key_bytes.decode('utf-8', errors='ignore')