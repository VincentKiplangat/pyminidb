"""
Page structure implementation.
Each page is 4096 bytes (4KB) with a header and data section.
"""
import struct
from typing import Optional, List, Tuple
from enum import IntEnum

class PageType(IntEnum):
    """Types of pages in the database."""
    FREE = 0
    DATA = 1
    INDEX = 2
    CATALOG = 3
    FREE_SPACE_MAP = 4

class Page:
    """Represents a single 4KB page in the database."""
    
    # Page header format (all in bytes):
    # - page_id: 8 (uint64)
    # - page_type: 1 (uint8)
    # - table_id: 4 (uint32)
    # - free_space_start: 2 (uint16)
    # - free_space_end: 2 (uint16)
    # - lsn: 8 (uint64, Log Sequence Number)
    # - checksum: 4 (uint32)
    PAGE_SIZE = 4096
    HEADER_SIZE = 8 + 1 + 4 + 2 + 2 + 8 + 4  # 29 bytes
    DATA_SIZE = PAGE_SIZE - HEADER_SIZE  # 4067 bytes
    
    # Struct format for header packing/unpacking
    HEADER_FORMAT = "<Q B I H H Q I"  # < for little-endian, Q=uint64, B=uint8, I=uint32, H=uint16
    
    def __init__(self, page_id: int, page_type: PageType = PageType.FREE, 
                 table_id: int = 0):
        """Initialize a new page."""
        self.page_id = page_id
        self.page_type = page_type
        self.table_id = table_id
        self.free_space_start = self.HEADER_SIZE
        self.free_space_end = self.PAGE_SIZE
        self.lsn = 0  # Log Sequence Number
        self.checksum = 0
        self.data = bytearray(self.DATA_SIZE)
        self.dirty = False
        
    def serialize(self) -> bytes:
        """Convert page to bytes for disk storage."""
        # Pack header with initial checksum (0)
        header = struct.pack(
            self.HEADER_FORMAT,
            self.page_id,
            self.page_type,
            self.table_id,
            self.free_space_start,
            self.free_space_end,
            self.lsn,
            self.checksum  # This will be 0 initially
        )
        
        # Calculate checksum
        self.checksum = self._calculate_checksum(header + self.data)
        
        # Repack with correct checksum
        header = struct.pack(
            self.HEADER_FORMAT,
            self.page_id,
            self.page_type,
            self.table_id,
            self.free_space_start,
            self.free_space_end,
            self.lsn,
            self.checksum
        )
        
        # Combine header and data
        result = header + self.data
        
        # DEBUG: Print sizes
        # print(f"[DEBUG] Header size: {len(header)}")
        # print(f"[DEBUG] Data size: {len(self.data)}")
        # print(f"[DEBUG] Total: {len(result)}")
        
        return result
    
    @classmethod
    def deserialize(cls, page_id: int, raw_data: bytes) -> 'Page':
        """Create page from raw bytes."""
        if len(raw_data) != cls.PAGE_SIZE:
            raise ValueError(f"Expected {cls.PAGE_SIZE} bytes, got {len(raw_data)}")
        
        # Unpack header
        header = raw_data[:cls.HEADER_SIZE]
        (page_id_val, page_type_val, table_id_val, 
         free_space_start, free_space_end, lsn, checksum) = struct.unpack(cls.HEADER_FORMAT, header)
        
        # Create page first
        page = cls(page_id_val, PageType(page_type_val), table_id_val)
        page.free_space_start = free_space_start
        page.free_space_end = free_space_end
        page.lsn = lsn
        page.checksum = checksum
        
        # Copy data (starting from HEADER_SIZE)
        page.data = bytearray(raw_data[cls.HEADER_SIZE:cls.PAGE_SIZE])
        
        return page
    
    def _calculate_checksum(self, data: bytes) -> int:
        """Calculate simple checksum for data integrity."""
        if len(data) < 4:
            return 0
            
        checksum = 0
        # Process in 4-byte chunks
        for i in range(0, len(data), 4):
            chunk = data[i:i+4]
            if len(chunk) < 4:
                # Pad with zeros if needed
                chunk = chunk + b'\x00' * (4 - len(chunk))
            checksum ^= struct.unpack('<I', chunk)[0]
        return checksum & 0xFFFFFFFF
    
    def allocate_space(self, size: int) -> Optional[int]:
        """Allocate space in the page for a record.
        Returns offset where space was allocated, or None if no space.
        """
        if self.free_space_start + size <= self.free_space_end:
            offset = self.free_space_start
            self.free_space_start += size
            self.dirty = True
            return offset
        return None
    
    def write_data(self, offset: int, data: bytes) -> None:
        """Write data at specific offset in page."""
        if offset < self.HEADER_SIZE or offset + len(data) > self.PAGE_SIZE:
            raise ValueError(f"Data offset {offset} or size {len(data)} invalid")
        
        # Convert to data section offset
        data_offset = offset - self.HEADER_SIZE
        self.data[data_offset:data_offset + len(data)] = data
        self.dirty = True
    
    def read_data(self, offset: int, size: int) -> bytes:
        """Read data from specific offset in page."""
        if offset < self.HEADER_SIZE or offset + size > self.PAGE_SIZE:
            raise ValueError(f"Read offset {offset} or size {size} invalid")
        
        # Convert to data section offset
        data_offset = offset - self.HEADER_SIZE
        return bytes(self.data[data_offset:data_offset + size])