#!/usr/bin/env python
"""
Finalized Simple B+ Tree implementation.
Fixes range search comparisons and basic leaf deletion.
"""
import struct
from typing import List, Optional, Dict, Any
from dataclasses import dataclass

@dataclass
class TreeNode:
    keys: List[bytes]
    values: List[Any]
    children: Optional[List['TreeNode']] = None
    next_leaf: Optional['TreeNode'] = None
    is_leaf: bool = True
    
    def __post_init__(self):
        if self.children is None and not self.is_leaf:
            self.children = []

class SimpleBPlusTree:
    def __init__(self, order: int = 4):
        self.order = order
        self.root = TreeNode(keys=[], values=[], is_leaf=True)
        self.key_to_value: Dict[bytes, Any] = {}
    
    def _serialize_key(self, key: Any) -> bytes:
        # Using Big-Endian (">Q") ensures byte-comparison matches numeric comparison
        if isinstance(key, int): return struct.pack(">Q", key)
        if isinstance(key, str): return key.encode('utf-8')
        return str(key).encode('utf-8')

    def _deserialize_key(self, key_bytes: bytes) -> Any:
        try: return struct.unpack(">Q", key_bytes)[0]
        except: return key_bytes.decode('utf-8', errors='ignore')

    def insert(self, key: Any, value: Any) -> None:
        key_bytes = self._serialize_key(key)
        self.key_to_value[key_bytes] = value
        leaf = self._find_leaf(self.root, key_bytes)
        self._insert_into_leaf(leaf, key_bytes, value)
        if len(leaf.keys) >= self.order:
            self._split_leaf(leaf)

    def _find_leaf(self, node: TreeNode, key_bytes: bytes) -> TreeNode:
        if node.is_leaf:
            return node
        idx = 0
        while idx < len(node.keys) and key_bytes >= node.keys[idx]:
            idx += 1
        return self._find_leaf(node.children[idx], key_bytes)

    def _insert_into_leaf(self, leaf: TreeNode, key: bytes, value: Any):
        idx = 0
        while idx < len(leaf.keys) and leaf.keys[idx] < key:
            idx += 1
        leaf.keys.insert(idx, key)
        leaf.values.insert(idx, value)

    def _split_leaf(self, leaf: TreeNode):
        mid = len(leaf.keys) // 2
        promote_key = leaf.keys[mid]
        new_leaf = TreeNode(keys=leaf.keys[mid:], values=leaf.values[mid:], 
                            is_leaf=True, next_leaf=leaf.next_leaf)
        leaf.keys, leaf.values, leaf.next_leaf = leaf.keys[:mid], leaf.values[:mid], new_leaf
        parent = self._find_parent(self.root, leaf)
        if parent is None:
            new_root = TreeNode(keys=[promote_key], values=[], is_leaf=False)
            new_root.children = [leaf, new_leaf]
            self.root = new_root
        else:
            self._insert_into_internal(parent, promote_key, leaf, new_leaf)

    def _insert_into_internal(self, parent: TreeNode, key: bytes, left: TreeNode, right: TreeNode):
        idx = 0
        while idx < len(parent.keys) and parent.keys[idx] < key:
            idx += 1
        parent.keys.insert(idx, key)
        parent.children.insert(idx + 1, right)
        if len(parent.keys) >= self.order:
            self._split_internal(parent)

    def _split_internal(self, node: TreeNode):
        mid = len(node.keys) // 2
        promote_key = node.keys[mid]
        new_node = TreeNode(keys=node.keys[mid+1:], values=[], is_leaf=False, 
                            children=node.children[mid+1:])
        node.keys, node.children = node.keys[:mid], node.children[:mid+1]
        parent = self._find_parent(self.root, node)
        if parent is None:
            new_root = TreeNode(keys=[promote_key], values=[], is_leaf=False)
            new_root.children = [node, new_node]
            self.root = new_root
        else:
            self._insert_into_internal(parent, promote_key, node, new_node)

    def _find_parent(self, root: TreeNode, target: TreeNode) -> Optional[TreeNode]:
        if root.is_leaf: return None
        if target in root.children: return root
        for child in root.children:
            p = self._find_parent(child, target)
            if p: return p
        return None

    def search(self, key: Any) -> Optional[Any]:
        key_bytes = self._serialize_key(key)
        leaf = self._find_leaf(self.root, key_bytes)
        for i, k in enumerate(leaf.keys):
            if k == key_bytes: return leaf.values[i]
        return None

    def range_search(self, start_key: Any, end_key: Any) -> List[Any]:
        start_bytes = self._serialize_key(start_key)
        end_bytes = self._serialize_key(end_key)
        leaf = self._find_leaf(self.root, start_bytes)
        res = []
        while leaf:
            for i, k in enumerate(leaf.keys):
                if start_bytes <= k < end_bytes: res.append(leaf.values[i])
                elif k >= end_bytes: return res
            leaf = leaf.next_leaf
        return res

    def delete(self, key: Any) -> bool:
        """Deletes key from both the map and the tree structure."""
        key_bytes = self._serialize_key(key)
        found_in_map = key_bytes in self.key_to_value
        if found_in_map:
            del self.key_to_value[key_bytes]
            # Actual tree deletion
            leaf = self._find_leaf(self.root, key_bytes)
            for i, k in enumerate(leaf.keys):
                if k == key_bytes:
                    leaf.keys.pop(i)
                    leaf.values.pop(i)
                    return True
        return False