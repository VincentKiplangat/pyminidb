"""
B+ Tree implementation for indexing.
A disk-based B+ tree for efficient range queries and point lookups.
"""
import struct
import math
from typing import List, Tuple, Optional, Any, Iterator, Dict
from dataclasses import dataclass
from enum import IntEnum

class NodeType(IntEnum):
    """Types of B+ Tree nodes."""
    INTERNAL = 0
    LEAF = 1

@dataclass
class BPlusTreeConfig:
    """Configuration for B+ Tree."""
    order: int = 4  # Maximum number of keys in a node (min = ceil(order/2))
    key_size: int = 8  # Size of key in bytes (for fixed-size keys)
    value_size: int = 8  # Size of value (record pointer) in bytes
    
    @property
    def max_keys(self):
        return self.order - 1
    
    @property
    def min_keys(self):
        return math.ceil(self.order / 2) - 1

class BPNode:
    """A node in the B+ Tree."""
    
    def __init__(self, node_id: int, node_type: NodeType, config: BPlusTreeConfig):
        self.node_id = node_id
        self.node_type = node_type
        self.config = config
        self.keys: List[bytes] = []  # Serialized keys
        self.children: List[int] = []  # For internal: child node IDs, for leaf: record pointers
        self.next_leaf: Optional[int] = None  # Pointer to next leaf (for range scans)
        self.parent: Optional[int] = None
        self.dirty = False
    
    def is_full(self) -> bool:
        """Check if node has reached maximum capacity."""
        return len(self.keys) >= self.config.max_keys
    
    def is_underflow(self) -> bool:
        """Check if node has too few keys (below minimum)."""
        # Root has different rules
        if self.parent is None:
            return len(self.keys) < 1  # Root can have 1 key
        return len(self.keys) < self.config.min_keys
    
    def insert_key_value(self, key: bytes, value: int) -> int:
        """Insert a key-value pair into a LEAF node. Returns position where inserted."""
        if self.node_type != NodeType.LEAF:
            raise ValueError("insert_key_value can only be called on leaf nodes")
        
        # Find insertion point
        pos = 0
        while pos < len(self.keys) and self.keys[pos] < key:
            pos += 1
        
        # Insert key and value
        self.keys.insert(pos, key)
        self.children.insert(pos, value)
        
        self.dirty = True
        return pos
    
    def insert_key_child(self, key: bytes, child_id: int) -> int:
        """Insert a key-child pair into an INTERNAL node. Returns position where inserted."""
        if self.node_type != NodeType.INTERNAL:
            raise ValueError("insert_key_child can only be called on internal nodes")
        
        # Find insertion point
        pos = 0
        while pos < len(self.keys) and self.keys[pos] < key:
            pos += 1
        
        # Insert key and child (child goes after key)
        self.keys.insert(pos, key)
        self.children.insert(pos + 1, child_id)
        
        self.dirty = True
        return pos
    
    def split(self) -> Tuple['BPNode', bytes]:
        """
        Split an overfull node.
        Returns: (new_node, middle_key)
        """
        mid = len(self.keys) // 2
        
        if self.node_type == NodeType.LEAF:
            # For leaf nodes, the middle key goes to the right
            right_keys = self.keys[mid:]
            right_children = self.children[mid:]
            
            # Keep middle key in left node for leaf split
            middle_key = right_keys[0]
            
            # Create new leaf node
            new_node = BPNode(
                node_id=-1,  # Will be assigned by tree
                node_type=NodeType.LEAF,
                config=self.config
            )
            new_node.keys = right_keys
            new_node.children = right_children
            
            # Update current node
            self.keys = self.keys[:mid]
            self.children = self.children[:mid]
            
            # Link leaves
            new_node.next_leaf = self.next_leaf
            self.next_leaf = new_node.node_id
            
        else:  # Internal node
            # For internal nodes, the middle key moves up to parent
            middle_key = self.keys[mid]
            
            right_keys = self.keys[mid + 1:]
            right_children = self.children[mid + 1:]
            
            # Create new internal node
            new_node = BPNode(
                node_id=-1,
                node_type=NodeType.INTERNAL,
                config=self.config
            )
            new_node.keys = right_keys
            new_node.children = right_children
            
            # Update current node
            self.keys = self.keys[:mid]
            self.children = self.children[:mid + 1]  # Keep one extra child
        
        self.dirty = True
        new_node.dirty = True
        
        return new_node, middle_key
    
    def find_key_position(self, key: bytes) -> Tuple[int, bool]:
        """
        Find position of key in node.
        Returns: (position, exact_match)
        """
        for i, k in enumerate(self.keys):
            if k == key:
                return i, True
            elif k > key:
                return i, False
        return len(self.keys), False
    
    def get_child_for_key(self, key: bytes) -> int:
        """For internal nodes, get the child node to follow for a given key."""
        if self.node_type != NodeType.INTERNAL:
            raise ValueError("get_child_for_key can only be called on internal nodes")
        
        pos, _ = self.find_key_position(key)
        return self.children[pos]

class BPlusTree:
    """Disk-based B+ Tree index."""
    
    def __init__(self, tree_id: int, config: BPlusTreeConfig = None):
        self.tree_id = tree_id
        self.config = config or BPlusTreeConfig()
        self.root_node_id = 0  # Start with root at node 0
        self.next_node_id = 1  # Next available node ID
        self.node_cache: Dict[int, BPNode] = {}
        
        # Create initial root leaf node
        self._create_root_node()
    
    def _create_root_node(self):
        """Create the initial root leaf node."""
        root = BPNode(0, NodeType.LEAF, self.config)
        self.node_cache[0] = root
        self.root_node_id = 0
    
    def insert(self, key: Any, value: int) -> None:
        """Insert a key-value pair into the index."""
        key_bytes = self._serialize_key(key)
        
        # Find the leaf node where this key should go
        leaf = self._find_leaf_for_insert(key_bytes)
        
        # Insert into leaf
        leaf.insert_key_value(key_bytes, value)
        
        # If leaf is full, split it
        if leaf.is_full():
            self._split_leaf(leaf)
    
    def search(self, key: Any) -> Optional[int]:
        """Search for a key, return value if found."""
        key_bytes = self._serialize_key(key)
        
        # Find the leaf node that would contain this key
        leaf = self._find_leaf_for_search(key_bytes)
        
        # Search within the leaf
        pos, found = leaf.find_key_position(key_bytes)
        if found:
            return leaf.children[pos]
        return None
    
    def range_search(self, start_key: Any, end_key: Any) -> Iterator[int]:
        """Search for all keys in range [start_key, end_key)."""
        start_bytes = self._serialize_key(start_key)
        end_bytes = self._serialize_key(end_key)
        
        # Find starting leaf
        leaf = self._find_leaf_for_search(start_bytes)
        
        # Traverse leaves
        while leaf is not None:
            for i, key in enumerate(leaf.keys):
                if start_bytes <= key < end_bytes:
                    yield leaf.children[i]
                elif key >= end_bytes:
                    return
            
            # Move to next leaf
            if leaf.next_leaf is not None:
                leaf = self._get_node(leaf.next_leaf)
            else:
                break
    
    def delete(self, key: Any) -> bool:
        """Delete a key from the index. Returns True if deleted."""
        key_bytes = self._serialize_key(key)
        
        # Find the leaf node
        leaf = self._find_leaf_for_search(key_bytes)
        
        pos, found = leaf.find_key_position(key_bytes)
        if not found:
            return False
        
        # Remove key-value pair
        del leaf.keys[pos]
        del leaf.children[pos]
        leaf.dirty = True
        
        # Handle underflow
        if leaf.is_underflow():
            self._handle_underflow(leaf)
        
        return True
    
    def _find_leaf_for_insert(self, key: bytes) -> BPNode:
        """Find the leaf node where a key should be inserted."""
        node = self._get_node(self.root_node_id)
        
        # Traverse down to leaf
        while node.node_type == NodeType.INTERNAL:
            child_id = node.get_child_for_key(key)
            node = self._get_node(child_id)
        
        return node
    
    def _find_leaf_for_search(self, key: bytes) -> BPNode:
        """Find the leaf node that would contain a key (same as insert for now)."""
        return self._find_leaf_for_insert(key)
    
    def _split_leaf(self, leaf: BPNode) -> None:
        """Split a leaf node and update the tree."""
        new_leaf, middle_key = leaf.split()
        
        # Assign ID to new leaf
        new_leaf.node_id = self.next_node_id
        self.next_node_id += 1
        
        # Save both leaves
        self._save_node(leaf)
        self._save_node(new_leaf)
        
        # Insert the middle key into parent
        self._insert_into_parent(leaf, middle_key, new_leaf.node_id)
    
    def _insert_into_parent(self, left_node: BPNode, key: bytes, right_node_id: int) -> None:
        """Insert a key and right child pointer into the parent node."""
        if left_node.parent is None:
            # Create new root
            self._create_new_root(left_node, key, right_node_id)
        else:
            parent = self._get_node(left_node.parent)
            
            # Insert key and right child into parent
            parent.insert_key_child(key, right_node_id)
            
            # Update right node's parent pointer
            right_node = self._get_node(right_node_id)
            right_node.parent = left_node.parent
            
            # Save nodes
            self._save_node(parent)
            self._save_node(right_node)
            
            # Split parent if it's now full
            if parent.is_full():
                self._split_internal_node(parent)
    
    def _split_internal_node(self, node: BPNode) -> None:
        """Split an internal node."""
        new_node, middle_key = node.split()
        
        # Assign ID to new node
        new_node.node_id = self.next_node_id
        self.next_node_id += 1
        
        # Update parent pointers of children that moved to new node
        for child_id in new_node.children:
            child = self._get_node(child_id)
            child.parent = new_node.node_id
            self._save_node(child)
        
        # Save both nodes
        self._save_node(node)
        self._save_node(new_node)
        
        # Insert the middle key into parent
        self._insert_into_parent(node, middle_key, new_node.node_id)
    
    def _create_new_root(self, left_node: BPNode, key: bytes, right_node_id: int) -> None:
        """Create a new root when splitting the old root."""
        # Create new root
        root = BPNode(
            node_id=self.next_node_id,
            node_type=NodeType.INTERNAL,
            config=self.config
        )
        self.next_node_id += 1
        
        # Set up root
        root.keys = [key]
        root.children = [left_node.node_id, right_node_id]
        
        # Update children's parent pointers
        left_node.parent = root.node_id
        
        right_node = self._get_node(right_node_id)
        right_node.parent = root.node_id
        
        # Update tree root
        self.root_node_id = root.node_id
        
        # Save all nodes
        self._save_node(left_node)
        self._save_node(right_node)
        self._save_node(root)
    
    def _handle_underflow(self, node: BPNode) -> None:
        """Handle underflow in a node by borrowing or merging."""
        # Simplified implementation - in a full implementation, we would:
        # 1. Try to borrow from left sibling
        # 2. Try to borrow from right sibling
        # 3. Merge with a sibling
        # For now, we'll just leave it (since we're not implementing full deletion rebalancing)
        pass
    
    def _get_node(self, node_id: int) -> BPNode:
        """Get node from cache (in-memory implementation)."""
        if node_id not in self.node_cache:
            # For now, create a placeholder (in real implementation, load from disk)
            raise ValueError(f"Node {node_id} not found in cache")
        return self.node_cache[node_id]
    
    def _save_node(self, node: BPNode) -> None:
        """Save node to cache (in-memory implementation)."""
        self.node_cache[node.node_id] = node
    
    def _serialize_key(self, key: Any) -> bytes:
        """Serialize a key to bytes."""
        if isinstance(key, int):
            return struct.pack("<Q", key)  # 64-bit integer, little-endian
        elif isinstance(key, str):
            # For strings, use UTF-8 encoding with fixed size
            encoded = key.encode('utf-8')
            # Pad or truncate to config.key_size
            if len(encoded) > self.config.key_size:
                return encoded[:self.config.key_size]
            else:
                return encoded.ljust(self.config.key_size, b'\x00')
        elif isinstance(key, bytes):
            return key
        else:
            raise TypeError(f"Unsupported key type: {type(key)}")