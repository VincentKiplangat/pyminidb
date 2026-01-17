"""
Storage manager handles all disk I/O operations.
"""
import os
import struct
from typing import Dict, Optional, List
from pathlib import Path
from .page import Page, PageType

class DatabaseHeader:
    """Database file header structure."""
    HEADER_SIZE = 4096  # First page is always header
    MAGIC_NUMBER = 0x504D4442  # "PMDB" in hex
    
    def __init__(self):
        self.magic = self.MAGIC_NUMBER
        self.page_size = Page.PAGE_SIZE
        self.db_size = 0  # in pages
        self.catalog_root = 0  # page ID of catalog root
        self.free_list_head = 0  # page ID of first free page
        self.last_transaction_id = 0
        self.checksum = 0
    
    def serialize(self) -> bytes:
        """Serialize header to bytes."""
        return struct.pack(
            "<I I Q Q Q Q I",
            self.magic,
            self.page_size,
            self.db_size,
            self.catalog_root,
            self.free_list_head,
            self.last_transaction_id,
            self.checksum
        ).ljust(self.HEADER_SIZE, b'\x00')
    
    @classmethod
    def deserialize(cls, data: bytes) -> 'DatabaseHeader':
        """Deserialize header from bytes."""
        if len(data) < 44:  # Changed from 36 to 44
            raise ValueError(f"Invalid header data: expected at least 44 bytes, got {len(data)}")
        
        (magic, page_size, db_size, catalog_root, 
         free_list_head, last_transaction_id, checksum) = struct.unpack("<I I Q Q Q Q I", data[:44])  # Changed slice
        
        if magic != cls.MAGIC_NUMBER:
            raise ValueError(f"Invalid magic number: {hex(magic)} expected {hex(cls.MAGIC_NUMBER)}")
        
        header = cls()
        header.magic = magic
        header.page_size = page_size
        header.db_size = db_size
        header.catalog_root = catalog_root
        header.free_list_head = free_list_head
        header.last_transaction_id = last_transaction_id
        header.checksum = checksum
        
        return header

class StorageManager:
    """Manages all disk I/O operations for the database."""
    
    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.file = None
        self.header = DatabaseHeader()
        self.is_open = False
        
    def create_database(self, overwrite: bool = False) -> None:
        """Create a new database file."""
        if self.db_path.exists():
            if overwrite:
                self.db_path.unlink()
            else:
                raise FileExistsError(f"Database {self.db_path} already exists")
        
        # Create file with initial size
        initial_pages = 10  # Start with 10 pages (header + 9 data pages)
        with open(self.db_path, 'wb') as f:
            # Write header page
            self.header.db_size = initial_pages
            f.write(self.header.serialize())
            
            # Initialize free pages (all except header)
            for page_id in range(1, initial_pages):
                page = Page(page_id, PageType.FREE)
                f.write(page.serialize())
        
        print(f"Created database: {self.db_path}")
    
    def open(self) -> None:
        """Open existing database file."""
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database {self.db_path} not found")
        
        self.file = open(self.db_path, 'r+b')
        
        # Read and validate header
        self.file.seek(0)
        header_data = self.file.read(DatabaseHeader.HEADER_SIZE)
        self.header = DatabaseHeader.deserialize(header_data)
        
        if self.header.page_size != Page.PAGE_SIZE:
            raise ValueError(f"Page size mismatch: expected {Page.PAGE_SIZE}, got {self.header.page_size}")
        
        self.is_open = True
        print(f"Opened database: {self.db_path} (pages: {self.header.db_size})")
    
    def close(self) -> None:
        """Close database file."""
        if self.file:
            self.file.close()
            self.file = None
            self.is_open = False
            print("Database closed")
    
    def read_page(self, page_id: int) -> Page:
        """Read a page from disk."""
        if not self.is_open:
            raise RuntimeError("Database not open")
        
        if page_id >= self.header.db_size:
            raise ValueError(f"Page {page_id} out of bounds")
        
        offset = page_id * Page.PAGE_SIZE
        self.file.seek(offset)
        page_data = self.file.read(Page.PAGE_SIZE)
        
        if len(page_data) != Page.PAGE_SIZE:
            raise ValueError(f"Could not read full page {page_id}")
        
        return Page.deserialize(page_id, page_data)
    
    def write_page(self, page: Page) -> None:
        """Write a page to disk."""
        if not self.is_open:
            raise RuntimeError("Database not open")
        
        if page.page_id >= self.header.db_size:
            self._extend_file(page.page_id + 1)
        
        offset = page.page_id * Page.PAGE_SIZE
        self.file.seek(offset)
        self.file.write(page.serialize())
        self.file.flush()
        page.dirty = False
    
    def allocate_page(self, page_type: PageType, table_id: int = 0) -> Page:
        """Allocate a new page from free list or extend file."""
        # TODO: Implement free list management
        # For now, just extend the file
        new_page_id = self.header.db_size
        self._extend_file(new_page_id + 1)
        
        page = Page(new_page_id, page_type, table_id)
        self.write_page(page)
        
        return page
    
    def _extend_file(self, new_size: int) -> None:
        """Extend database file to accommodate more pages."""
        if new_size <= self.header.db_size:
            return
        
        old_size = self.header.db_size
        self.file.seek(old_size * Page.PAGE_SIZE)
        
        # Write new free pages
        for page_id in range(old_size, new_size):
            page = Page(page_id, PageType.FREE)
            self.file.write(page.serialize())
        
        # Update header
        self.header.db_size = new_size
        self.file.seek(0)
        self.file.write(self.header.serialize())
        self.file.flush()
        
        print(f"Extended database from {old_size} to {new_size} pages")