"""
Log manager for handling multiple segments with automatic rotation.

Manages a collection of log segments, automatically rotating to new segments
when thresholds are reached.
"""

import os
import threading
import time
from pathlib import Path
from typing import Iterator, List, Optional

from distributedlog.core.log.compaction import LogCompactor
from distributedlog.core.log.format import Message
from distributedlog.core.log.reader import LogSegmentReader
from distributedlog.core.log.retention import RetentionManager
from distributedlog.core.log.segment import LogSegment
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class DiskFullError(Exception):
    """Raised when disk is full."""
    pass


class SegmentDeletedException(Exception):
    """Raised when trying to read a deleted segment."""
    pass


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
        retention_hours: int = -1,
        retention_bytes: int = -1,
        enable_compaction: bool = False,
    ):
        """
        Initialize a log.
        
        Args:
            directory: Directory to store log segments
            max_segment_size: Maximum segment size in bytes (default: 1GB)
            max_segment_age_ms: Maximum segment age in ms (default: 7 days)
            fsync_on_append: Whether to fsync after each append
            retention_hours: Max age for segments in hours (-1 = unlimited)
            retention_bytes: Max total log size in bytes (-1 = unlimited)
            enable_compaction: Enable log compaction
        """
        self.directory = Path(directory)
        self.max_segment_size = max_segment_size
        self.max_segment_age_ms = max_segment_age_ms
        self.fsync_on_append = fsync_on_append
        
        self.directory.mkdir(parents=True, exist_ok=True)
        
        self._segments: List[LogSegment] = []
        self._active_segment: Optional[LogSegment] = None
        self._segment_creation_time: int = 0
        
        self._write_lock = threading.RLock()
        self._segments_lock = threading.RLock()
        
        self._retention_manager = RetentionManager(
            retention_hours=retention_hours,
            retention_bytes=retention_bytes,
        )
        
        self._compactor = LogCompactor(directory) if enable_compaction else None
        
        self._recover_segments()
        
        logger.info(
            "Initialized log",
            directory=str(self.directory),
            segments=len(self._segments),
            max_segment_size=self.max_segment_size,
            retention_enabled=retention_hours > 0 or retention_bytes > 0,
            compaction_enabled=enable_compaction,
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
        
        Raises:
            DiskFullError: If disk is full
        """
        with self._segments_lock:
            if self._active_segment is not None:
                self._active_segment.flush()
            
            try:
                segment = LogSegment(
                    base_offset=base_offset,
                    directory=self.directory,
                    max_size_bytes=self.max_segment_size,
                    fsync_on_append=self.fsync_on_append,
                )
            except OSError as e:
                if e.errno == 28:
                    raise DiskFullError("Disk is full, cannot create new segment")
                raise
            
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
        
        Thread-safe operation with automatic rotation and disk full handling.
        
        Args:
            key: Optional message key
            value: Message payload
        
        Returns:
            The offset assigned to the message
        
        Raises:
            DiskFullError: If disk is full
        """
        with self._write_lock:
            if self._should_rotate():
                next_offset = (
                    self._active_segment.next_offset()
                    if self._active_segment is not None
                    else 0
                )
                try:
                    self._create_new_segment(next_offset)
                except DiskFullError:
                    logger.error("Disk full during rotation, attempting cleanup")
                    self._apply_retention()
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
            
            try:
                offset = self._active_segment.append(message)
            except OSError as e:
                if e.errno == 28:
                    logger.error("Disk full during append, attempting cleanup")
                    self._apply_retention()
                    offset = self._active_segment.append(message)
                else:
                    raise
            
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
        
        Thread-safe read operation that handles segment deletion.
        
        Args:
            start_offset: Offset to start reading from
            max_messages: Maximum number of messages to read
        
        Yields:
            Messages in order
        
        Raises:
            SegmentDeletedException: If segment is deleted during read
        """
        messages_read = 0
        
        with self._segments_lock:
            segments_snapshot = list(self._segments)
        
        for segment in segments_snapshot:
            if segment.next_offset() <= start_offset:
                continue
            
            if segment.base_offset > start_offset:
                reader_start = segment.base_offset
            else:
                reader_start = start_offset
            
            if not segment.path.exists():
                logger.warning(
                    "Segment deleted during read",
                    base_offset=segment.base_offset,
                )
                continue
            
            try:
                reader = LogSegmentReader(segment.path, segment.base_offset)
            except FileNotFoundError:
                logger.warning(
                    "Segment disappeared before read",
                    base_offset=segment.base_offset,
                )
                continue
            
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
    
    def _apply_retention(self) -> int:
        """
        Apply retention policy and delete old segments.
        
        Returns:
            Number of segments deleted
        """
        with self._segments_lock:
            if self._active_segment is None:
                return 0
            
            to_delete = self._retention_manager.segments_to_delete(
                self._segments,
                self._active_segment,
            )
            
            if not to_delete:
                return 0
            
            deleted = self._retention_manager.delete_segments(to_delete)
            
            for segment in to_delete:
                if segment in self._segments:
                    self._segments.remove(segment)
            
            logger.info(
                "Applied retention policy",
                segments_deleted=deleted,
                segments_remaining=len(self._segments),
            )
            
            return deleted
    
    def compact(self, min_cleanable_ratio: float = 0.5) -> int:
        """
        Compact eligible segments.
        
        Args:
            min_cleanable_ratio: Min ratio of duplicates to compact
        
        Returns:
            Number of segments compacted
        """
        if self._compactor is None:
            return 0
        
        with self._segments_lock:
            if self._active_segment is None:
                return 0
            
            compacted_count = 0
            
            for segment in list(self._segments):
                if segment == self._active_segment:
                    continue
                
                if not self._compactor.should_compact(segment):
                    continue
                
                try:
                    compacted = self._compactor.compact_segment(
                        segment,
                        min_cleanable_ratio,
                    )
                    
                    if compacted:
                        self._compactor.replace_segment(segment, compacted)
                        compacted_count += 1
                
                except Exception as e:
                    logger.error(
                        "Compaction failed",
                        base_offset=segment.base_offset,
                        error=str(e),
                    )
            
            return compacted_count
    
    def check_disk_space(self) -> dict:
        """
        Check available disk space.
        
        Returns:
            Dictionary with disk space info
        """
        stat = os.statvfs(self.directory)
        
        total_bytes = stat.f_blocks * stat.f_frsize
        available_bytes = stat.f_bavail * stat.f_frsize
        used_bytes = total_bytes - available_bytes
        used_percent = (used_bytes / total_bytes) * 100
        
        return {
            "total_bytes": total_bytes,
            "used_bytes": used_bytes,
            "available_bytes": available_bytes,
            "used_percent": used_percent,
        }
