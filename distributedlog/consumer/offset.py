"""
Offset management for consumer.

Handles tracking and committing offsets for consumed messages.
"""

import threading
import time
from dataclasses import dataclass
from typing import Dict, Optional, Set

from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class TopicPartition:
    """
    Represents a topic-partition pair.
    
    Attributes:
        topic: Topic name
        partition: Partition number
    """
    topic: str
    partition: int
    
    def __hash__(self):
        return hash((self.topic, self.partition))
    
    def __eq__(self, other):
        return self.topic == other.topic and self.partition == other.partition


@dataclass
class OffsetAndMetadata:
    """
    Offset with optional metadata.
    
    Attributes:
        offset: The offset
        metadata: Optional metadata string
    """
    offset: int
    metadata: Optional[str] = None


class OffsetManager:
    """
    Manages consumer offsets.
    
    Tracks:
    - Current position per partition
    - Committed offsets per partition
    - Pending commits
    """
    
    def __init__(self, group_id: str):
        """
        Initialize offset manager.
        
        Args:
            group_id: Consumer group ID
        """
        self.group_id = group_id
        
        self._positions: Dict[TopicPartition, int] = {}
        self._committed: Dict[TopicPartition, OffsetAndMetadata] = {}
        
        self._lock = threading.RLock()
        
        logger.info("Initialized offset manager", group_id=group_id)
    
    def update_position(self, tp: TopicPartition, offset: int) -> None:
        """
        Update current position for partition.
        
        Args:
            tp: Topic-partition
            offset: New offset position
        """
        with self._lock:
            self._positions[tp] = offset
            
            logger.debug(
                "Updated position",
                topic=tp.topic,
                partition=tp.partition,
                offset=offset,
            )
    
    def position(self, tp: TopicPartition) -> Optional[int]:
        """
        Get current position for partition.
        
        Args:
            tp: Topic-partition
        
        Returns:
            Current offset or None
        """
        with self._lock:
            return self._positions.get(tp)
    
    def commit(self, offsets: Dict[TopicPartition, OffsetAndMetadata]) -> None:
        """
        Commit offsets.
        
        Args:
            offsets: Offsets to commit
        """
        with self._lock:
            for tp, offset_metadata in offsets.items():
                self._committed[tp] = offset_metadata
                
                logger.info(
                    "Committed offset",
                    topic=tp.topic,
                    partition=tp.partition,
                    offset=offset_metadata.offset,
                    group_id=self.group_id,
                )
    
    def committed(self, tp: TopicPartition) -> Optional[OffsetAndMetadata]:
        """
        Get committed offset for partition.
        
        Args:
            tp: Topic-partition
        
        Returns:
            Committed offset or None
        """
        with self._lock:
            return self._committed.get(tp)
    
    def seek(self, tp: TopicPartition, offset: int) -> None:
        """
        Seek to specific offset.
        
        Args:
            tp: Topic-partition
            offset: Target offset
        """
        with self._lock:
            self._positions[tp] = offset
            
            logger.info(
                "Seeked to offset",
                topic=tp.topic,
                partition=tp.partition,
                offset=offset,
            )
    
    def reset_positions(self, partitions: Set[TopicPartition]) -> None:
        """
        Reset positions for partitions.
        
        Args:
            partitions: Partitions to reset
        """
        with self._lock:
            for tp in partitions:
                if tp in self._positions:
                    del self._positions[tp]
            
            logger.info("Reset positions", count=len(partitions))
    
    def get_offsets_to_commit(self) -> Dict[TopicPartition, OffsetAndMetadata]:
        """
        Get offsets ready to commit.
        
        Returns:
            Dictionary of offsets to commit
        """
        with self._lock:
            offsets = {}
            
            for tp, position in self._positions.items():
                committed_offset = self._committed.get(tp)
                committed_value = committed_offset.offset if committed_offset else -1
                
                if position > committed_value:
                    offsets[tp] = OffsetAndMetadata(offset=position)
            
            return offsets


class AutoCommitManager:
    """
    Manages automatic offset commits.
    
    Periodically commits offsets in background thread.
    """
    
    def __init__(
        self,
        offset_manager: OffsetManager,
        auto_commit_interval_ms: int = 5000,
    ):
        """
        Initialize auto-commit manager.
        
        Args:
            offset_manager: Offset manager
            auto_commit_interval_ms: Commit interval in milliseconds
        """
        self.offset_manager = offset_manager
        self.auto_commit_interval_ms = auto_commit_interval_ms
        
        self._running = False
        self._thread: Optional[threading.Thread] = None
        
        logger.info(
            "Initialized auto-commit manager",
            interval_ms=auto_commit_interval_ms,
        )
    
    def start(self) -> None:
        """Start auto-commit thread."""
        if self._running:
            return
        
        self._running = True
        self._thread = threading.Thread(
            target=self._auto_commit_loop,
            name="consumer-auto-commit",
            daemon=True,
        )
        self._thread.start()
        
        logger.info("Started auto-commit thread")
    
    def stop(self) -> None:
        """Stop auto-commit thread."""
        if not self._running:
            return
        
        self._running = False
        
        if self._thread:
            self._thread.join(timeout=5.0)
        
        logger.info("Stopped auto-commit thread")
    
    def _auto_commit_loop(self) -> None:
        """Auto-commit loop."""
        while self._running:
            try:
                time.sleep(self.auto_commit_interval_ms / 1000.0)
                
                offsets = self.offset_manager.get_offsets_to_commit()
                
                if offsets:
                    self.offset_manager.commit(offsets)
                    
                    logger.debug(
                        "Auto-committed offsets",
                        count=len(offsets),
                    )
            
            except Exception as e:
                logger.error("Auto-commit error", error=str(e))


class OffsetResetStrategy:
    """Offset reset strategies."""
    
    EARLIEST = "earliest"  # Reset to beginning
    LATEST = "latest"      # Reset to end
    NONE = "none"          # Raise error if no offset


def get_beginning_offset(tp: TopicPartition) -> int:
    """
    Get beginning offset for partition.
    
    Args:
        tp: Topic-partition
    
    Returns:
        Beginning offset (0)
    """
    return 0


def get_end_offset(tp: TopicPartition) -> int:
    """
    Get end offset for partition.
    
    This is a placeholder - in reality, would query broker.
    
    Args:
        tp: Topic-partition
    
    Returns:
        End offset
    """
    return -1
