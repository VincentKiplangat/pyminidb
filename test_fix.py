#!/usr/bin/env python
"""
Quick test to verify fixes.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.storage.page import Page, PageType

print("Testing Page serialization fix...")
page = Page(1, PageType.DATA, 100)
offset = page.allocate_space(100)
page.write_data(offset, b"Test data")

serialized = page.serialize()
print(f"Serialized size: {len(serialized)} bytes")
print(f"Expected size: {Page.PAGE_SIZE} bytes")
print(f"Match: {len(serialized) == Page.PAGE_SIZE}")

if len(serialized) != Page.PAGE_SIZE:
    print(f"ERROR: Size mismatch! Header={Page.HEADER_SIZE}, Data={Page.DATA_SIZE}")
    print(f"Header format: {Page.HEADER_FORMAT}")
    print("Please check the struct format and sizes.")
else:
    print("SUCCESS: Page serialization fixed!")
    
    # Test deserialization
    page2 = Page.deserialize(1, serialized)
    print(f"Deserialized page_id: {page2.page_id}")
    print(f"Deserialized page_type: {page2.page_type}")
    print(f"Deserialized data: {page2.read_data(offset, 9)}")