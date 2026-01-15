"""
Log segment reader for sequential reads by offset.

Provides efficient sequential reads from log segments with support for
partial read detection and corruption recovery.
"""

import os
from pathlib import Path
from typing import Iterator, Optional

from distributedlog.core.log.format import Message, MessageSet
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class LogSegmentReader:
    """
    Sequential reader for log segments.
    
    Reads messages from a segment file sequentially, handling:
    - Partial write detection
    - CRC validation
    - Corruption recovery
    """
    
    def __init__(self, path: Path, base_offset: int):
        """
        Initialize a log segment reader.
        
        Args:
            path: Path to the segment file
            base_offset: Base offset of the segment
        
        Raises:
            FileNotFoundError: If segment file doesn't exist
        """
        if not path.exists():
            raise FileNotFoundError(f"Segment file not found: {path}")
        
        self.path = path
        self.base_offset = base_offset
        self._fd: Optional[int] = None
        self._current_offset = base_offset
        
        logger.info("Initialized reader", path=str(path), base_offset=base_offset)
    
    def open(self) -> None:
        """Open the segment file for reading."""
        if self._fd is None:
            self._fd = os.open(self.path, os.O_RDONLY)
            logger.debug("Opened segment for reading", path=str(self.path))
    
    def close(self) -> None:
        """Close the segment file."""
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
            logger.debug("Closed segment reader", path=str(self.path))
    
    def read_at_offset(self, offset: int) -> Optional[Message]:
        """
        Read a single message at a specific offset.
        
        Args:
            offset: Logical offset to read
        
        Returns:
            Message if found, None if offset not in segment
        
        Raises:
            ValueError: If offset is before base offset
            IOError: If read fails
        """
        if offset < self.base_offset:
            raise ValueError(
                f"Offset {offset} is before segment base {self.base_offset}"
            )
        
        self.open()
        
        for message in self.read_all():
            if message.offset == offset:
                return message
            if message.offset > offset:
                return None
        
        return None
    
    def read_all(self) -> Iterator[Message]:
        """
        Read all messages from the segment sequentially.
        
        Yields:
            Messages in order
        
        Raises:
            IOError: If read fails
        """
        self.open()
        
        if self._fd is None:
            return
        
        os.lseek(self._fd, 0, os.SEEK_SET)
        
        current_offset = self.base_offset
        file_position = 0
        
        while True:
            length_bytes = os.read(self._fd, 4)
            
            if len(length_bytes) == 0:
                break
            
            if len(length_bytes) < 4:
                logger.warning(
                    "Partial write detected at end of segment",
                    path=str(self.path),
                    position=file_position,
                    bytes_read=len(length_bytes),
                )
                break
            
            length = int.from_bytes(length_bytes, byteorder="big")
            
            if length <= 0 or length > 100 * 1024 * 1024:
                logger.error(
                    "Invalid message length",
                    path=str(self.path),
                    position=file_position,
                    length=length,
                )
                break
            
            remaining_bytes = os.read(self._fd, length)
            
            if len(remaining_bytes) < length:
                logger.warning(
                    "Incomplete message at end of segment",
                    path=str(self.path),
                    position=file_position,
                    expected=length,
                    got=len(remaining_bytes),
                )
                break
            
            full_message = length_bytes + remaining_bytes
            
            try:
                message_set = MessageSet.deserialize(full_message, current_offset)
                yield message_set.message
                
                current_offset += 1
                file_position += len(full_message)
                
            except ValueError as e:
                logger.error(
                    "Failed to deserialize message",
                    path=str(self.path),
                    position=file_position,
                    offset=current_offset,
                    error=str(e),
                )
                break
    
    def recover_valid_messages(self) -> tuple[list[Message], int]:
        """
        Recover all valid messages from potentially corrupted segment.
        
        This method reads as many valid messages as possible, stopping at the
        first corruption. Useful for crash recovery.
        
        Returns:
            Tuple of (valid messages, bytes consumed)
        """
        messages = []
        bytes_consumed = 0
        
        try:
            for message in self.read_all():
                messages.append(message)
        except Exception as e:
            logger.warning(
                "Stopped recovery at error",
                path=str(self.path),
                messages_recovered=len(messages),
                error=str(e),
            )
        
        if self._fd is not None:
            bytes_consumed = os.lseek(self._fd, 0, os.SEEK_CUR)
        
        logger.info(
            "Recovery complete",
            path=str(self.path),
            messages=len(messages),
            bytes=bytes_consumed,
        )
        
        return messages, bytes_consumed
    
    def __enter__(self) -> "LogSegmentReader":
        """Context manager entry."""
        self.open()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()
