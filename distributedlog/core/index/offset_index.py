"""
Offset index for fast log lookups using sparse indexing.

The index maps logical offsets to physical byte positions in the log file,
enabling O(log n) lookups instead of O(n) sequential scans.
"""

import mmap
import os
import struct
from pathlib import Path
from typing import Optional, Tuple

from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class IndexEntry:
    """
    A single entry in the offset index.
    
    Maps a relative offset to its physical position in the log file.
    """
    
    SIZE = 8
    FORMAT = ">II"
    
    def __init__(self, relative_offset: int, position: int):
        """
        Create an index entry.
        
        Args:
            relative_offset: Offset relative to segment base offset
            position: Byte position in the log file
        
        Raises:
            ValueError: If values are negative
        """
        if relative_offset < 0:
            raise ValueError(f"Relative offset must be non-negative: {relative_offset}")
        if position < 0:
            raise ValueError(f"Position must be non-negative: {position}")
        
        self.relative_offset = relative_offset
        self.position = position
    
    def serialize(self) -> bytes:
        """
        Serialize entry to bytes.
        
        Returns:
            8 bytes representing the entry
        """
        return struct.pack(self.FORMAT, self.relative_offset, self.position)
    
    @classmethod
    def deserialize(cls, data: bytes) -> "IndexEntry":
        """
        Deserialize entry from bytes.
        
        Args:
            data: 8 bytes of serialized entry
        
        Returns:
            IndexEntry instance
        
        Raises:
            ValueError: If data is not 8 bytes
        """
        if len(data) != cls.SIZE:
            raise ValueError(f"Expected {cls.SIZE} bytes, got {len(data)}")
        
        relative_offset, position = struct.unpack(cls.FORMAT, data)
        return cls(relative_offset, position)
    
    def __repr__(self) -> str:
        return f"IndexEntry(offset={self.relative_offset}, pos={self.position})"


class OffsetIndex:
    """
    Sparse index for fast offset-to-position lookups.
    
    The index stores entries at intervals (e.g., every 4KB) rather than for
    every message, trading some precision for space efficiency.
    
    Uses memory-mapped files for efficient access and OS-managed caching.
    
    Attributes:
        base_offset: The base offset of the segment
        path: Path to the index file
        max_entries: Maximum number of entries
        interval_bytes: Bytes between index entries
    """
    
    INDEX_FILE_SUFFIX = ".index"
    DEFAULT_INTERVAL_BYTES = 4096
    DEFAULT_MAX_ENTRIES = 262144
    
    def __init__(
        self,
        base_offset: int,
        directory: Path,
        max_entries: int = DEFAULT_MAX_ENTRIES,
        interval_bytes: int = DEFAULT_INTERVAL_BYTES,
    ):
        """
        Initialize offset index.
        
        Args:
            base_offset: Base offset of the segment
            directory: Directory for index file
            max_entries: Maximum number of entries (default: 256K)
            interval_bytes: Bytes between entries (default: 4KB)
        """
        self.base_offset = base_offset
        self.directory = Path(directory)
        self.max_entries = max_entries
        self.interval_bytes = interval_bytes
        
        self.path = self._build_index_path(base_offset)
        
        self._fd: Optional[int] = None
        self._mmap: Optional[mmap.mmap] = None
        self._entries_count: int = 0
        self._last_indexed_position: int = 0
        
        self._open()
        
        logger.info(
            "Initialized offset index",
            base_offset=self.base_offset,
            path=str(self.path),
            interval_bytes=self.interval_bytes,
        )
    
    def _build_index_path(self, offset: int) -> Path:
        """
        Build the file path for an index.
        
        Args:
            offset: Base offset for the index
        
        Returns:
            Path to index file
        """
        filename = f"{str(offset).zfill(20)}{self.INDEX_FILE_SUFFIX}"
        return self.directory / filename
    
    def _open(self) -> None:
        """Open or create the index file with memory mapping."""
        if self._fd is not None:
            return
        
        flags = os.O_RDWR | os.O_CREAT
        mode = 0o644
        
        self._fd = os.open(self.path, flags, mode)
        
        file_size = os.fstat(self._fd).st_size
        target_size = self.max_entries * IndexEntry.SIZE
        
        if file_size == 0:
            os.ftruncate(self._fd, target_size)
            file_size = target_size
        
        self._mmap = mmap.mmap(
            self._fd,
            length=file_size,
            access=mmap.ACCESS_WRITE,
        )
        
        if file_size > 0:
            self._entries_count = self._count_entries()
        
        logger.debug(
            "Opened index file",
            path=str(self.path),
            size=file_size,
            entries=self._entries_count,
        )
    
    def _count_entries(self) -> int:
        """
        Count existing entries in the index.
        
        Returns:
            Number of valid entries
        """
        count = 0
        max_possible = len(self._mmap) // IndexEntry.SIZE
        
        for i in range(max_possible):
            offset = i * IndexEntry.SIZE
            data = self._mmap[offset : offset + IndexEntry.SIZE]
            
            if data == b"\x00" * IndexEntry.SIZE:
                break
            
            try:
                IndexEntry.deserialize(data)
                count += 1
            except (ValueError, struct.error):
                break
        
        return count
    
    def append(self, offset: int, position: int) -> bool:
        """
        Append an entry to the index.
        
        Args:
            offset: Absolute offset
            position: Byte position in log file
        
        Returns:
            True if entry was added, False if skipped or full
        """
        if self._mmap is None:
            raise RuntimeError("Index is not open")
        
        if self._entries_count >= self.max_entries:
            logger.warning("Index is full", entries=self._entries_count)
            return False
        
        if position < self._last_indexed_position + self.interval_bytes:
            return False
        
        relative_offset = offset - self.base_offset
        entry = IndexEntry(relative_offset, position)
        
        write_offset = self._entries_count * IndexEntry.SIZE
        self._mmap[write_offset : write_offset + IndexEntry.SIZE] = entry.serialize()
        
        self._entries_count += 1
        self._last_indexed_position = position
        
        logger.debug(
            "Added index entry",
            offset=offset,
            relative_offset=relative_offset,
            position=position,
            total_entries=self._entries_count,
        )
        
        return True
    
    def lookup(self, offset: int) -> Optional[Tuple[int, int]]:
        """
        Find the largest indexed offset <= target offset.
        
        Args:
            offset: Target absolute offset
        
        Returns:
            Tuple of (closest_offset, position) or None if not found
        """
        if self._mmap is None:
            raise RuntimeError("Index is not open")
        
        if self._entries_count == 0:
            return None
        
        relative_offset = offset - self.base_offset
        
        if relative_offset < 0:
            return None
        
        left, right = 0, self._entries_count - 1
        result: Optional[Tuple[int, int]] = None
        
        while left <= right:
            mid = (left + right) // 2
            entry = self._read_entry(mid)
            
            if entry.relative_offset <= relative_offset:
                result = (entry.relative_offset + self.base_offset, entry.position)
                left = mid + 1
            else:
                right = mid - 1
        
        logger.debug(
            "Index lookup",
            target_offset=offset,
            result_offset=result[0] if result else None,
            result_position=result[1] if result else None,
        )
        
        return result
    
    def _read_entry(self, index: int) -> IndexEntry:
        """
        Read an entry at a specific index.
        
        Args:
            index: Entry index
        
        Returns:
            IndexEntry
        
        Raises:
            IndexError: If index out of bounds
        """
        if index < 0 or index >= self._entries_count:
            raise IndexError(f"Index {index} out of bounds [0, {self._entries_count})")
        
        offset = index * IndexEntry.SIZE
        data = self._mmap[offset : offset + IndexEntry.SIZE]
        return IndexEntry.deserialize(data)
    
    def flush(self) -> None:
        """Flush index to disk."""
        if self._mmap is not None:
            self._mmap.flush()
            logger.debug("Flushed index", path=str(self.path))
    
    def close(self) -> None:
        """Close the index and release resources."""
        if self._mmap is not None:
            self._mmap.flush()
            self._mmap.close()
            self._mmap = None
        
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
        
        logger.info(
            "Closed index",
            base_offset=self.base_offset,
            entries=self._entries_count,
        )
    
    def truncate(self, entries: int) -> None:
        """
        Truncate index to specific number of entries.
        
        Args:
            entries: Number of entries to keep
        """
        if entries < 0 or entries > self._entries_count:
            raise ValueError(f"Invalid truncate size: {entries}")
        
        if self._mmap is not None:
            start = entries * IndexEntry.SIZE
            end = self._entries_count * IndexEntry.SIZE
            self._mmap[start:end] = b"\x00" * (end - start)
            self._mmap.flush()
        
        self._entries_count = entries
        
        logger.info("Truncated index", entries=entries)
    
    def entries_count(self) -> int:
        """
        Get number of entries in index.
        
        Returns:
            Entry count
        """
        return self._entries_count
    
    def __enter__(self) -> "OffsetIndex":
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()
    
    def __repr__(self) -> str:
        return (
            f"OffsetIndex(base_offset={self.base_offset}, "
            f"entries={self._entries_count})"
        )
