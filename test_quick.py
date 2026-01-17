#!/usr/bin/env python
"""
Quick test to verify DatabaseHeader fix.
"""
import struct
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.storage.storage_manager import DatabaseHeader
from src.storage.page import Page

print("Testing DatabaseHeader fix...")

# Test format size
format_str = "<I I Q Q Q Q I"
expected_size = struct.calcsize(format_str)
print(f"Format '{format_str}' requires {expected_size} bytes")

# Test header
header = DatabaseHeader()
header.db_size = 15
header.catalog_root = 3

serialized = header.serialize()
print(f"Serialized header size: {len(serialized)} bytes")
print(f"First 50 bytes (hex): {serialized[:50].hex()}")

# Try to deserialize
try:
    deserialized = DatabaseHeader.deserialize(serialized)
    print(f"SUCCESS! Deserialized magic: {hex(deserialized.magic)}")
    print(f"Deserialized db_size: {deserialized.db_size}")
    print(f"Deserialized catalog_root: {deserialized.catalog_root}")
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()