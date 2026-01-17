"""
Test the storage layer.
"""
import pytest
import tempfile
import os
from pathlib import Path
from src.storage.storage_manager import StorageManager
from src.storage.page import Page, PageType

def test_page_serialization():
    """Test page serialization/deserialization."""
    # Create page
    page = Page(1, PageType.DATA, 100)
    
    # Write some test data
    offset = page.allocate_space(100)
    test_data = b"Hello, PyMiniDB!"
    page.write_data(offset, test_data)
    
    # Serialize and deserialize
    serialized = page.serialize()
    print(f"[TEST] Serialized length: {len(serialized)}")
    print(f"[TEST] Expected PAGE_SIZE: {Page.PAGE_SIZE}")
    assert len(serialized) == Page.PAGE_SIZE
    
    deserialized = Page.deserialize(1, serialized)
    assert deserialized.page_id == 1
    assert deserialized.page_type == PageType.DATA
    assert deserialized.table_id == 100
    
    # Verify data
    read_data = deserialized.read_data(offset, len(test_data))
    assert read_data == test_data

def test_storage_manager_create():
    """Test database creation."""
    import tempfile
    import time
    
    # Create a unique temp file path
    db_path = tempfile.mktemp(suffix='.db')
    
    try:
        # Create database
        storage = StorageManager(db_path)
        storage.create_database()
        
        # Open and verify
        storage.open()
        assert storage.is_open
        assert storage.header.magic == storage.header.MAGIC_NUMBER
        assert storage.header.db_size == 10  # Initial pages
        
        # Read a page
        page = storage.read_page(1)
        assert page.page_type == PageType.FREE
        
        # Close explicitly
        storage.close()
        
    except Exception as e:
        # If there's an error, try to clean up
        print(f"Error in test: {e}")
        raise
    finally:
        # Wait a bit for Windows to release the file
        import time
        time.sleep(0.1)
        
        # Clean up - ensure file is closed first
        if 'storage' in locals() and hasattr(storage, 'file') and storage.file:
            try:
                storage.close()
            except:
                pass
        
        # Try multiple times to delete (Windows sometimes locks files)
        for _ in range(3):
            try:
                if os.path.exists(db_path):
                    os.unlink(db_path)
                    break
            except PermissionError:
                time.sleep(0.1)
                continue

def test_storage_manager_extend():
    """Test file extension."""
    import tempfile
    import time
    
    db_path = tempfile.mktemp(suffix='.db')
    
    try:
        storage = StorageManager(db_path)
        storage.create_database()
        storage.open()
        
        initial_size = storage.header.db_size
        
        # Allocate new page (should extend file)
        page = storage.allocate_page(PageType.DATA, 1)
        assert page.page_id == initial_size
        
        # Verify file was extended
        storage.file.seek(0, 2)  # Seek to end
        file_size = storage.file.tell()
        expected_size = (initial_size + 1) * Page.PAGE_SIZE
        assert file_size == expected_size
        
        storage.close()
        
    except Exception as e:
        print(f"Error in test: {e}")
        raise
    finally:
        # Wait for file release
        time.sleep(0.1)
        
        # Ensure file is closed
        if 'storage' in locals() and hasattr(storage, 'file') and storage.file:
            try:
                storage.close()
            except:
                pass
        
        # Delete file with retries
        for _ in range(3):
            try:
                if os.path.exists(db_path):
                    os.unlink(db_path)
                    break
            except PermissionError:
                time.sleep(0.1)
                continue

def test_page_edge_cases():
    """Test edge cases for page operations."""
    page = Page(1, PageType.DATA, 100)
    
    # Test allocating too much space
    offset = page.allocate_space(Page.PAGE_SIZE)  # Too large
    assert offset is None
    
    # Test allocating reasonable space
    offset = page.allocate_space(100)
    assert offset is not None
    
    # Test writing at invalid offset
    with pytest.raises(ValueError):
        page.write_data(0, b"test")  # Offset 0 is in header
    
    # Test reading at invalid offset
    with pytest.raises(ValueError):
        page.read_data(Page.PAGE_SIZE - 10, 20)  # Read past end

# Add a new test for DatabaseHeader
def test_database_header():
    """Test database header serialization/deserialization."""
    from src.storage.storage_manager import DatabaseHeader
    
    # Create header
    header = DatabaseHeader()
    header.db_size = 20
    header.catalog_root = 5
    header.free_list_head = 10
    header.last_transaction_id = 12345
    
    # Serialize
    serialized = header.serialize()
    assert len(serialized) == DatabaseHeader.HEADER_SIZE  # 4096
    
    # Deserialize
    deserialized = DatabaseHeader.deserialize(serialized)
    
    # Verify
    assert deserialized.magic == DatabaseHeader.MAGIC_NUMBER
    assert deserialized.page_size == Page.PAGE_SIZE
    assert deserialized.db_size == 20
    assert deserialized.catalog_root == 5
    assert deserialized.free_list_head == 10
    assert deserialized.last_transaction_id == 12345

if __name__ == "__main__":
    pytest.main([__file__, "-v"])