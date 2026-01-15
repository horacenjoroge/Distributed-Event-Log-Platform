"""
Log segment implementation for append-only storage.

A segment is a single log file that stores messages sequentially. Each segment
has a base offset and grows until it reaches a size or time threshold.
"""

import os
import struct
from pathlib import Path
from typing import Optional

from distributedlog.core.index.offset_index import OffsetIndex
from distributedlog.core.log.format import Message, MessageSet
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class LogSegment:
    """
    Manages a single log segment file.
    
    A segment stores messages sequentially on disk with the following properties:
    - Append-only writes (no random updates)
    - Named by base offset (e.g., 00000000000000000000.log)
    - Atomic append operations
    - Configurable fsync behavior
    
    Attributes:
        base_offset: The starting offset for this segment
        path: Path to the log file
        max_size_bytes: Maximum size before rotation
    """
    
    SEGMENT_FILE_SUFFIX = ".log"
    OFFSET_PADDING = 20
    
    def __init__(
        self,
        base_offset: int,
        directory: Path,
        max_size_bytes: int = 1073741824,
        fsync_on_append: bool = False,
        index_interval_bytes: int = 4096,
    ):
        """
        Initialize a log segment.
        
        Args:
            base_offset: Starting offset for this segment
            directory: Directory to store the segment file
            max_size_bytes: Maximum segment size in bytes
            fsync_on_append: Whether to fsync after each append
            index_interval_bytes: Bytes between index entries
        
        Raises:
            ValueError: If base_offset is negative
        """
        if base_offset < 0:
            raise ValueError(f"Base offset must be non-negative, got {base_offset}")
        
        self.base_offset = base_offset
        self.directory = Path(directory)
        self.max_size_bytes = max_size_bytes
        self.fsync_on_append = fsync_on_append
        
        self.directory.mkdir(parents=True, exist_ok=True)
        
        self.path = self._build_segment_path(base_offset)
        
        self._fd: Optional[int] = None
        self._current_size: int = 0
        self._next_offset: int = base_offset
        
        self._index = OffsetIndex(
            base_offset=base_offset,
            directory=directory,
            interval_bytes=index_interval_bytes,
        )
        
        self._open()
        
        logger.info(
            "Initialized log segment",
            base_offset=self.base_offset,
            path=str(self.path),
            max_size_bytes=self.max_size_bytes,
            index_interval=index_interval_bytes,
        )
    
    def _build_segment_path(self, offset: int) -> Path:
        """
        Build the file path for a segment.
        
        Args:
            offset: Base offset for the segment
        
        Returns:
            Path to segment file
        """
        filename = f"{str(offset).zfill(self.OFFSET_PADDING)}{self.SEGMENT_FILE_SUFFIX}"
        return self.directory / filename
    
    def _open(self) -> None:
        """Open the segment file for appending."""
        if self._fd is not None:
            return
        
        flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
        mode = 0o644
        
        self._fd = os.open(self.path, flags, mode)
        
        self._current_size = os.fstat(self._fd).st_size
        
        if self._current_size > 0:
            logger.info(
                "Opened existing segment",
                base_offset=self.base_offset,
                size=self._current_size,
            )
    
    def _close(self) -> None:
        """Close the segment file."""
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
            logger.debug("Closed segment", base_offset=self.base_offset)
    
    def append(self, message: Message) -> int:
        """
        Append a message to the segment.
        
        This operation is atomic - either the entire message is written or none of it.
        
        Args:
            message: Message to append
        
        Returns:
            The offset assigned to the message
        
        Raises:
            IOError: If write fails
            ValueError: If segment is full or closed
        """
        if self._fd is None:
            raise ValueError("Cannot append to closed segment")
        
        if self.is_full():
            raise ValueError("Segment is full")
        
        offset = self._next_offset
        message.offset = offset
        
        message_set = MessageSet(message=message)
        data = message_set.serialize()
        
        position_before_write = self._current_size
        
        bytes_written = os.write(self._fd, data)
        
        if bytes_written != len(data):
            raise IOError(
                f"Partial write: expected {len(data)} bytes, wrote {bytes_written} bytes"
            )
        
        if self.fsync_on_append:
            os.fsync(self._fd)
        
        self._current_size += bytes_written
        self._next_offset += 1
        
        self._index.append(offset, position_before_write)
        
        logger.debug(
            "Appended message",
            offset=offset,
            size=len(data),
            total_size=self._current_size,
            position=position_before_write,
        )
        
        return offset
    
    def flush(self) -> None:
        """
        Flush buffered data to disk.
        
        This ensures durability by forcing the OS to write data to physical storage.
        """
        if self._fd is not None:
            os.fsync(self._fd)
        
        self._index.flush()
        
        logger.debug("Flushed segment and index", base_offset=self.base_offset)
    
    def is_full(self) -> bool:
        """
        Check if segment has reached its maximum size.
        
        Returns:
            True if segment is at or above max size
        """
        return self._current_size >= self.max_size_bytes
    
    def size(self) -> int:
        """
        Get current size of segment in bytes.
        
        Returns:
            Size in bytes
        """
        return self._current_size
    
    def next_offset(self) -> int:
        """
        Get the next offset that will be assigned.
        
        Returns:
            Next offset
        """
        return self._next_offset
    
    def close(self) -> None:
        """Close the segment and release resources."""
        self.flush()
        self._close()
        self._index.close()
        
        logger.info(
            "Closed segment",
            base_offset=self.base_offset,
            final_size=self._current_size,
            messages=self._next_offset - self.base_offset,
            index_entries=self._index.entries_count(),
        )
    
    def __enter__(self) -> "LogSegment":
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()
    
    def __repr__(self) -> str:
        """String representation."""
        return (
            f"LogSegment(base_offset={self.base_offset}, "
            f"size={self._current_size}, "
            f"next_offset={self._next_offset})"
        )
