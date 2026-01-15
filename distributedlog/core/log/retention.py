"""
Log retention policies for cleanup and space management.

Implements time-based and size-based retention policies to automatically
delete old log segments.
"""

import time
from enum import Enum
from pathlib import Path
from typing import List

from distributedlog.core.log.segment import LogSegment
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class RetentionPolicy(Enum):
    """Retention policy types."""
    
    TIME_BASED = "time"
    SIZE_BASED = "size"
    BOTH = "both"


class RetentionManager:
    """
    Manages log retention policies.
    
    Handles deletion of old segments based on time and size constraints.
    """
    
    def __init__(
        self,
        retention_hours: int = -1,
        retention_bytes: int = -1,
    ):
        """
        Initialize retention manager.
        
        Args:
            retention_hours: Max age in hours (-1 = unlimited)
            retention_bytes: Max total size in bytes (-1 = unlimited)
        """
        self.retention_hours = retention_hours
        self.retention_bytes = retention_bytes
        
        if retention_hours > 0 and retention_bytes > 0:
            self.policy = RetentionPolicy.BOTH
        elif retention_hours > 0:
            self.policy = RetentionPolicy.TIME_BASED
        elif retention_bytes > 0:
            self.policy = RetentionPolicy.SIZE_BASED
        else:
            self.policy = None
        
        logger.info(
            "Initialized retention manager",
            policy=self.policy.value if self.policy else "none",
            retention_hours=retention_hours,
            retention_bytes=retention_bytes,
        )
    
    def segments_to_delete(
        self,
        segments: List[LogSegment],
        active_segment: LogSegment,
    ) -> List[LogSegment]:
        """
        Determine which segments should be deleted based on retention policy.
        
        Args:
            segments: All segments
            active_segment: Currently active segment (never deleted)
        
        Returns:
            List of segments to delete
        """
        if not self.policy:
            return []
        
        to_delete = []
        
        if self.policy in [RetentionPolicy.TIME_BASED, RetentionPolicy.BOTH]:
            to_delete.extend(self._time_based_candidates(segments, active_segment))
        
        if self.policy in [RetentionPolicy.SIZE_BASED, RetentionPolicy.BOTH]:
            to_delete.extend(self._size_based_candidates(segments, active_segment))
        
        return list(set(to_delete))
    
    def _time_based_candidates(
        self,
        segments: List[LogSegment],
        active_segment: LogSegment,
    ) -> List[LogSegment]:
        """
        Find segments older than retention period.
        
        Args:
            segments: All segments
            active_segment: Active segment
        
        Returns:
            Segments to delete
        """
        if self.retention_hours <= 0:
            return []
        
        retention_ms = self.retention_hours * 3600 * 1000
        current_time_ms = int(time.time() * 1000)
        cutoff_time_ms = current_time_ms - retention_ms
        
        candidates = []
        
        for segment in segments:
            if segment == active_segment:
                continue
            
            try:
                mtime_ns = segment.path.stat().st_mtime_ns
                mtime_ms = mtime_ns // 1_000_000
                
                if mtime_ms < cutoff_time_ms:
                    candidates.append(segment)
                    logger.debug(
                        "Segment eligible for time-based deletion",
                        base_offset=segment.base_offset,
                        age_hours=(current_time_ms - mtime_ms) / 3600000,
                    )
            except FileNotFoundError:
                logger.warning(
                    "Segment file not found during retention check",
                    base_offset=segment.base_offset,
                )
        
        return candidates
    
    def _size_based_candidates(
        self,
        segments: List[LogSegment],
        active_segment: LogSegment,
    ) -> List[LogSegment]:
        """
        Find oldest segments to maintain size limit.
        
        Args:
            segments: All segments
            active_segment: Active segment
        
        Returns:
            Segments to delete
        """
        if self.retention_bytes <= 0:
            return []
        
        total_size = sum(seg.size() for seg in segments)
        
        if total_size <= self.retention_bytes:
            return []
        
        sorted_segments = sorted(
            (seg for seg in segments if seg != active_segment),
            key=lambda s: s.base_offset,
        )
        
        candidates = []
        size_to_free = total_size - self.retention_bytes
        freed_size = 0
        
        for segment in sorted_segments:
            if freed_size >= size_to_free:
                break
            
            candidates.append(segment)
            freed_size += segment.size()
            
            logger.debug(
                "Segment eligible for size-based deletion",
                base_offset=segment.base_offset,
                size=segment.size(),
                freed_so_far=freed_size,
            )
        
        return candidates
    
    def delete_segments(self, segments: List[LogSegment]) -> int:
        """
        Delete segments and their associated files.
        
        Args:
            segments: Segments to delete
        
        Returns:
            Number of segments deleted
        """
        deleted_count = 0
        
        for segment in segments:
            try:
                segment.close()
                
                segment.path.unlink(missing_ok=True)
                
                index_path = Path(str(segment.path).replace(".log", ".index"))
                index_path.unlink(missing_ok=True)
                
                deleted_count += 1
                
                logger.info(
                    "Deleted segment",
                    base_offset=segment.base_offset,
                    path=str(segment.path),
                )
            
            except Exception as e:
                logger.error(
                    "Failed to delete segment",
                    base_offset=segment.base_offset,
                    error=str(e),
                )
        
        return deleted_count
