#!/usr/bin/env python
"""
Demonstrate Simple B+ Tree operations.
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.index.simple_bplus_tree import SimpleBPlusTree
from src.index.index_manager import IndexManager

def demo_basic_operations():
    """Demonstrate basic B+ Tree operations."""
    print("=== Simple B+ Tree Demonstration ===\n")
    
    # Create a B+ Tree
    tree = SimpleBPlusTree(order=4)
    
    print("1. Inserting key-value pairs...")
    data = [
        (50, "User50"),
        (30, "User30"),
        (70, "User70"),
        (20, "User20"),
        (40, "User40"),
        (60, "User60"),
        (80, "User80"),
        (10, "User10"),
        (90, "User90"),
    ]
    
    for key, value in data:
        tree.insert(key, value)
        print(f"   Inserted: {key} -> {value}")
    
    print("\n2. Searching for keys...")
    test_keys = [10, 30, 50, 70, 90, 99]
    for key in test_keys:
        result = tree.search(key)
        if result:
            print(f"   Found {key}: {result}")
        else:
            print(f"   Key {key}: Not found")
    
    print("\n3. Range search [25, 75)...")
    results = tree.range_search(25, 75)
    print(f"   Found {len(results)} values:")
    for value in results:
        print(f"     - {value}")
    
    print("\n4. Deleting keys 20, 50, 80...")
    for key in [20, 50, 80]:
        if tree.delete(key):
            print(f"   Deleted key {key}")
        else:
            print(f"   Key {key} not found")
    
    print("\n5. Verifying after deletion...")
    for key, value in data:
        result = tree.search(key)
        if key in [20, 50, 80]:
            status = "✗" if result is None else "?"
            print(f"   {status} Key {key}: {'Deleted' if result is None else 'Still exists'}")
        else:
            status = "✓" if result is not None else "✗"
            print(f"   {status} Key {key}: {'Exists' if result is not None else 'Missing'}")
    
    print("\n=== Demonstration Complete ===")

def demo_index_manager():
    """Demonstrate index manager operations."""
    print("\n=== Index Manager Demonstration ===\n")
    
    manager = IndexManager()
    
    print("1. Creating indexes...")
    indexes = ["idx_users_id", "idx_users_email", "idx_products_price"]
    for idx in indexes:
        manager.create_index(idx)
        print(f"   Created index: {idx}")
    
    print("\n2. Inserting data into indexes...")
    # Simulate user data
    users = [
        (1, "alice@example.com", "Alice"),
        (2, "bob@example.com", "Bob"),
        (3, "charlie@example.com", "Charlie"),
    ]
    
    for user_id, email, name in users:
        manager.insert("idx_users_id", user_id, name)
        manager.insert("idx_users_email", email, user_id)
        print(f"   Inserted user: {user_id}, {email}, {name}")
    
    # Simulate product data
    products = [
        (101, 29.99, "Laptop"),
        (102, 9.99, "Mouse"),
        (103, 499.99, "Monitor"),
    ]
    
    for product_id, price, name in products:
        manager.insert("idx_products_price", price, name)
        print(f"   Inserted product: {product_id}, ${price}, {name}")
    
    print("\n3. Searching indexes...")
    # Search by user ID
    print("   Searching by user ID 2:")
    result = manager.search("idx_users_id", 2)
    print(f"     Result: {result}")
    
    # Search by email
    print("   Searching by email 'alice@example.com':")
    result = manager.search("idx_users_email", "alice@example.com")
    print(f"     Result: User ID: {result}")
    
    # Range search on prices
    print("   Searching products between $10 and $100:")
    results = manager.range_search("idx_products_price", 10.0, 100.0)
    for product in results:
        print(f"     - {product}")
    
    print("\n4. Deleting from indexes...")
    deleted = manager.delete("idx_users_id", 2)
    print(f"   Deleted user ID 2: {deleted}")
    
    # Verify deletion
    result = manager.search("idx_users_id", 2)
    print(f"   Search for deleted user ID 2: {'Not found' if result is None else 'Found'}")
    
    print("\n5. Listing all indexes...")
    indexes = manager.get_all_indexes()
    for idx in indexes:
        print(f"   - {idx}")
    
    print("\n=== Index Manager Demo Complete ===")

def demo_performance():
    """Demonstrate B+ Tree performance."""
    print("\n=== Performance Test ===\n")
    
    tree = SimpleBPlusTree(order=10)
    
    import time
    
    # Insert many keys
    n = 10000
    print(f"Inserting {n} keys...")
    start = time.time()
    
    for i in range(n):
        tree.insert(i, f"Value{i}")
    
    insert_time = time.time() - start
    print(f"Insert time: {insert_time:.3f} seconds")
    print(f"Average: {insert_time/n*1000:.2f} ms per insert")
    
    # Search test
    print(f"\nSearching for {n} keys...")
    start = time.time()
    
    found_count = 0
    for i in range(n):
        result = tree.search(i)
        if result == f"Value{i}":
            found_count += 1
    
    search_time = time.time() - start
    print(f"Search time: {search_time:.3f} seconds")
    print(f"Found {found_count}/{n} keys correctly")
    print(f"Average: {search_time/n*1000:.2f} ms per search")
    
    # Range search test
    print(f"\nRange search [2500, 7500)...")
    start = time.time()
    
    results = tree.range_search(2500, 7500)
    
    range_time = time.time() - start
    print(f"Found {len(results)} items in {range_time:.3f} seconds")
    print(f"Average: {range_time/len(results)*1000:.4f} ms per item")
    
    # Delete test
    print(f"\nDeleting first {n//2} keys...")
    start = time.time()
    
    deleted_count = 0
    for i in range(n//2):
        if tree.delete(i):
            deleted_count += 1
    
    delete_time = time.time() - start
    print(f"Delete time: {delete_time:.3f} seconds")
    print(f"Deleted {deleted_count}/{n//2} keys")
    print(f"Average: {delete_time/(n//2)*1000:.2f} ms per delete")
    
    # Verify remaining keys
    remaining = n // 2
    print(f"\nVerifying remaining {remaining} keys...")
    start = time.time()
    
    found_count = 0
    for i in range(n//2, n):
        if tree.search(i) == f"Value{i}":
            found_count += 1
    
    verify_time = time.time() - start
    print(f"Verify time: {verify_time:.3f} seconds")
    print(f"Found {found_count}/{remaining} keys correctly")

if __name__ == "__main__":
    demo_basic_operations()
    demo_index_manager()
    demo_performance()