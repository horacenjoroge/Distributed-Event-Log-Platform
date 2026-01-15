"""
Log manager for handling multiple segments with automatic rotation.

Manages a collection of log segments, automatically rotating to new segments
when thresholds are reached.
"""

import time
from pathlib import Path
from typing import Iterator, Optional

from distributedlog.core.log.format import Message
from distributedlog.core.log.reader import LogSegmentReader
from distributedlog.core.log.segment import LogSegment
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class Log:
    """
    Manages multiple log segments with automatic rotation.
    
    The log consists of multiple segments, where each segment is a file on disk.
    When a segment reaches its size or time threshold, a new segment is created.
    
    Attributes:
        directory: Directory containing log segments
        max_segment_size: Maximum bytes per segment before rotation
        max_segment_age_ms: Maximum age in milliseconds before rotation
    """
    
    def __init__(
        self,
        directory: Path,
        max_segment_size: int = 1073741824,
        max_segment_age_ms: int = 604800000,
        fsync_on_append: bool = False,
    ):
        """
        Initialize a log.
        
        Args:
            directory: Directory to store log segments
            max_segment_size: Maximum segment size in bytes (default: 1GB)
            max_segment_age_ms: Maximum segment age in ms (default: 7 days)
            fsync_on_append: Whether to fsync after each append
        """
        self.directory = Path(directory)
        self.max_segment_size = max_segment_size
        self.max_segment_age_ms = max_segment_age_ms
        self.fsync_on_append = fsync_on_append
        
        self.directory.mkdir(parents=True, exist_ok=True)
        
        self._segments: list[LogSegment] = []
        self._active_segment: Optional[LogSegment] = None
        self._segment_creation_time: int = 0
        
        self._recover_segments()
        
        logger.info(
            "Initialized log",
            directory=str(self.directory),
            segments=len(self._segments),
            max_segment_size=self.max_segment_size,
        )
    
    def _recover_segments(self) -> None:
        """Recover existing segments from disk."""
        segment_files = sorted(
            self.directory.glob(f"*{LogSegment.SEGMENT_FILE_SUFFIX}"),
            key=lambda p: int(p.stem),
        )
        
        if not segment_files:
            self._create_new_segment(base_offset=0)
            return
        
        for segment_file in segment_files:
            base_offset = int(segment_file.stem)
            
            segment = LogSegment(
                base_offset=base_offset,
                directory=self.directory,
                max_size_bytes=self.max_segment_size,
                fsync_on_append=self.fsync_on_append,
            )
            
            reader = LogSegmentReader(segment_file, base_offset)
            messages, bytes_read = reader.recover_valid_messages()
            reader.close()
            
            segment._current_size = bytes_read
            segment._next_offset = base_offset + len(messages)
            
            self._segments.append(segment)
            
            logger.info(
                "Recovered segment",
                base_offset=base_offset,
                messages=len(messages),
                bytes=bytes_read,
            )
        
        if self._segments:
            self._active_segment = self._segments[-1]
            self._segment_creation_time = int(time.time() * 1000)
    
    def _create_new_segment(self, base_offset: int) -> None:
        """
        Create a new segment and make it active.
        
        Args:
            base_offset: Starting offset for the new segment
        """
        if self._active_segment is not None:
            self._active_segment.flush()
        
        segment = LogSegment(
            base_offset=base_offset,
            directory=self.directory,
            max_size_bytes=self.max_segment_size,
            fsync_on_append=self.fsync_on_append,
        )
        
        self._segments.append(segment)
        self._active_segment = segment
        self._segment_creation_time = int(time.time() * 1000)
        
        logger.info(
            "Created new segment",
            base_offset=base_offset,
            total_segments=len(self._segments),
        )
    
    def _should_rotate(self) -> bool:
        """
        Check if active segment should be rotated.
        
        Returns:
            True if rotation is needed
        """
        if self._active_segment is None:
            return True
        
        if self._active_segment.is_full():
            logger.info(
                "Rotation triggered by size",
                size=self._active_segment.size(),
                max_size=self.max_segment_size,
            )
            return True
        
        current_time = int(time.time() * 1000)
        age_ms = current_time - self._segment_creation_time
        
        if age_ms >= self.max_segment_age_ms:
            logger.info(
                "Rotation triggered by age",
                age_ms=age_ms,
                max_age_ms=self.max_segment_age_ms,
            )
            return True
        
        return False
    
    def append(self, key: Optional[bytes], value: bytes) -> int:
        """
        Append a message to the log.
        
        Args:
            key: Optional message key
            value: Message payload
        
        Returns:
            The offset assigned to the message
        """
        if self._should_rotate():
            next_offset = (
                self._active_segment.next_offset()
                if self._active_segment is not None
                else 0
            )
            self._create_new_segment(next_offset)
        
        if self._active_segment is None:
            raise RuntimeError("No active segment available")
        
        timestamp = int(time.time() * 1000)
        
        message = Message(
            offset=0,
            timestamp=timestamp,
            key=key,
            value=value,
        )
        
        offset = self._active_segment.append(message)
        
        logger.debug(
            "Appended to log",
            offset=offset,
            key_size=len(key) if key else 0,
            value_size=len(value),
        )
        
        return offset
    
    def read(self, start_offset: int, max_messages: int = 100) -> Iterator[Message]:
        """
        Read messages starting from an offset.
        
        Args:
            start_offset: Offset to start reading from
            max_messages: Maximum number of messages to read
        
        Yields:
            Messages in order
        """
        messages_read = 0
        
        for segment in self._segments:
            if segment.next_offset() <= start_offset:
                continue
            
            if segment.base_offset > start_offset:
                reader_start = segment.base_offset
            else:
                reader_start = start_offset
            
            reader = LogSegmentReader(segment.path, segment.base_offset)
            
            try:
                for message in reader.read_all():
                    if message.offset >= reader_start:
                        yield message
                        messages_read += 1
                        
                        if messages_read >= max_messages:
                            return
            finally:
                reader.close()
    
    def flush(self) -> None:
        """Flush all segments to disk."""
        for segment in self._segments:
            segment.flush()
        
        logger.debug("Flushed all segments", count=len(self._segments))
    
    def close(self) -> None:
        """Close all segments."""
        for segment in self._segments:
            segment.close()
        
        logger.info("Closed log", segments=len(self._segments))
    
    def get_latest_offset(self) -> int:
        """
        Get the latest offset in the log.
        
        Returns:
            Latest offset, or -1 if log is empty
        """
        if self._active_segment is None:
            return -1
        
        return self._active_segment.next_offset() - 1
    
    def segment_count(self) -> int:
        """
        Get the number of segments.
        
        Returns:
            Number of segments
        """
        return len(self._segments)
    
    def __enter__(self) -> "Log":
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.close()
