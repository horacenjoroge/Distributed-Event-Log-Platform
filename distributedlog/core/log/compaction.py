"""
Log compaction for keeping only latest values per key.

Compaction removes old records for the same key, keeping only the latest value.
This is useful for change data capture and stateful streams.
"""

from collections import OrderedDict
from pathlib import Path
from typing import Dict, Optional

from distributedlog.core.log.format import Message, MessageSet
from distributedlog.core.log.segment import LogSegment
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class LogCompactor:
    """
    Compacts log segments by keeping only latest value per key.
    
    Compaction process:
    1. Read all messages from segment
    2. Build map of key -> latest message
    3. Write compacted segment
    4. Replace old segment with compacted one
    """
    
    def __init__(self, directory: Path):
        """
        Initialize log compactor.
        
        Args:
            directory: Directory for log segments
        """
        self.directory = Path(directory)
        
        logger.info("Initialized log compactor", directory=str(directory))
    
    def compact_segment(
        self,
        segment: LogSegment,
        min_cleanable_ratio: float = 0.5,
    ) -> Optional[LogSegment]:
        """
        Compact a single segment.
        
        Args:
            segment: Segment to compact
            min_cleanable_ratio: Min ratio of duplicate keys to compact
        
        Returns:
            New compacted segment or None if not enough duplicates
        """
        logger.info(
            "Starting segment compaction",
            base_offset=segment.base_offset,
            size=segment.size(),
        )
        
        key_to_message: Dict[Optional[bytes], Message] = OrderedDict()
        total_messages = 0
        messages_with_keys = 0
        
        from distributedlog.core.log.reader import LogSegmentReader
        
        reader = LogSegmentReader(segment.path, segment.base_offset)
        
        try:
            for message in reader.read_all():
                total_messages += 1
                
                if message.key is not None:
                    messages_with_keys += 1
                    key_to_message[message.key] = message
                else:
                    key_to_message[None] = message
        
        finally:
            reader.close()
        
        unique_messages = len(key_to_message)
        
        if messages_with_keys == 0:
            logger.info(
                "Skipping compaction: no messages with keys",
                base_offset=segment.base_offset,
            )
            return None
        
        duplicate_ratio = 1.0 - (unique_messages / total_messages)
        
        if duplicate_ratio < min_cleanable_ratio:
            logger.info(
                "Skipping compaction: insufficient duplicates",
                base_offset=segment.base_offset,
                duplicate_ratio=duplicate_ratio,
                min_required=min_cleanable_ratio,
            )
            return None
        
        compacted_path = self.directory / f"{segment.path.stem}.compacted.log"
        
        compacted_segment = LogSegment(
            base_offset=segment.base_offset,
            directory=self.directory,
            max_size_bytes=segment.max_size_bytes,
        )
        
        compacted_segment.path = compacted_path
        
        for message in key_to_message.values():
            try:
                compacted_segment.append(message)
            except ValueError:
                logger.warning(
                    "Compacted segment full during compaction",
                    base_offset=segment.base_offset,
                )
                break
        
        compacted_segment.flush()
        
        logger.info(
            "Compaction complete",
            base_offset=segment.base_offset,
            original_messages=total_messages,
            compacted_messages=unique_messages,
            duplicate_ratio=duplicate_ratio,
            original_size=segment.size(),
            compacted_size=compacted_segment.size(),
            space_saved=segment.size() - compacted_segment.size(),
        )
        
        return compacted_segment
    
    def replace_segment(
        self,
        old_segment: LogSegment,
        new_segment: LogSegment,
    ) -> None:
        """
        Replace old segment with compacted version.
        
        Args:
            old_segment: Original segment
            new_segment: Compacted segment
        """
        try:
            old_segment.close()
            new_segment.close()
            
            old_path = old_segment.path
            new_path = new_segment.path
            
            old_index_path = Path(str(old_path).replace(".log", ".index"))
            new_index_path = Path(str(new_path).replace(".log", ".index"))
            
            backup_path = old_path.with_suffix(".log.backup")
            old_path.rename(backup_path)
            
            new_path.rename(old_path)
            
            if new_index_path.exists():
                if old_index_path.exists():
                    old_index_path.unlink()
                new_index_path.rename(old_index_path)
            
            backup_path.unlink()
            
            logger.info(
                "Replaced segment with compacted version",
                base_offset=old_segment.base_offset,
            )
        
        except Exception as e:
            logger.error(
                "Failed to replace segment",
                base_offset=old_segment.base_offset,
                error=str(e),
            )
            raise
    
    def should_compact(
        self,
        segment: LogSegment,
        min_compaction_lag_ms: int = 0,
    ) -> bool:
        """
        Determine if segment should be compacted.
        
        Args:
            segment: Segment to check
            min_compaction_lag_ms: Min age before compaction
        
        Returns:
            True if segment should be compacted
        """
        if not segment.path.exists():
            return False
        
        if segment.is_full():
            return False
        
        import time
        
        if min_compaction_lag_ms > 0:
            mtime_ns = segment.path.stat().st_mtime_ns
            mtime_ms = mtime_ns // 1_000_000
            current_ms = int(time.time() * 1000)
            age_ms = current_ms - mtime_ms
            
            if age_ms < min_compaction_lag_ms:
                return False
        
        return True
