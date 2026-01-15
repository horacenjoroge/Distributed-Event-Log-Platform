"""Consumer offset management."""

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass(frozen=True)
class TopicPartition:
    """
    Represents a topic-partition pair.
    
    Attributes:
        topic: Topic name
        partition: Partition number
    """
    topic: str
    partition: int
    
    def __str__(self) -> str:
        return f"{self.topic}-{self.partition}"
    
    def __repr__(self) -> str:
        return f"TopicPartition(topic='{self.topic}', partition={self.partition})"


@dataclass
class OffsetAndMetadata:
    """
    Offset with metadata.
    
    Attributes:
        offset: Committed offset
        metadata: Optional metadata string
    """
    offset: int
    metadata: str = ""


class OffsetResetStrategy:
    """Offset reset strategies."""
    EARLIEST = "earliest"
    LATEST = "latest"
    NONE = "none"


class OffsetManager:
    """
    Basic offset manager (in-memory only).
    
    Tracks current positions and committed offsets for partitions.
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
    
    def update_position(self, tp: TopicPartition, offset: int) -> None:
        """
        Update current position for partition.
        
        Args:
            tp: Topic-partition
            offset: New offset position
        """
        self._positions[tp] = offset
    
    def position(self, tp: TopicPartition) -> Optional[int]:
        """
        Get current position for partition.
        
        Args:
            tp: Topic-partition
        
        Returns:
            Current offset or None
        """
        return self._positions.get(tp)
    
    def commit(self, offsets: Dict[TopicPartition, OffsetAndMetadata]) -> None:
        """
        Commit offsets.
        
        Args:
            offsets: Offsets to commit
        """
        self._committed.update(offsets)
    
    def committed(self, tp: TopicPartition) -> Optional[OffsetAndMetadata]:
        """
        Get committed offset for partition.
        
        Args:
            tp: Topic-partition
        
        Returns:
            Committed offset or None
        """
        return self._committed.get(tp)
    
    def seek(self, tp: TopicPartition, offset: int) -> None:
        """
        Seek to specific offset.
        
        Args:
            tp: Topic-partition
            offset: Target offset
        """
        self._positions[tp] = offset
    
    def reset_positions(self, partitions: set) -> None:
        """
        Reset positions for partitions.
        
        Args:
            partitions: Set of topic-partitions
        """
        for tp in partitions:
            if tp in self._positions:
                del self._positions[tp]
    
    def get_offsets_to_commit(self) -> Dict[TopicPartition, OffsetAndMetadata]:
        """
        Get offsets that need to be committed.
        
        Returns:
            Map of partition to offset
        """
        offsets = {}
        for tp, position in self._positions.items():
            offsets[tp] = OffsetAndMetadata(offset=position)
        return offsets


class AutoCommitManager:
    """
    Auto-commit manager (background thread).
    
    Periodically commits offsets in the background.
    """
    
    def __init__(
        self,
        offset_manager: OffsetManager,
        interval_ms: int,
    ):
        """
        Initialize auto-commit manager.
        
        Args:
            offset_manager: Offset manager
            interval_ms: Auto-commit interval
        """
        self.offset_manager = offset_manager
        self.interval_ms = interval_ms
        self._running = False
    
    def start(self) -> None:
        """Start auto-commit."""
        self._running = True
    
    def stop(self) -> None:
        """Stop auto-commit."""
        self._running = False


def get_beginning_offset(tp: TopicPartition) -> int:
    """Get beginning offset for partition."""
    return 0


def get_end_offset(tp: TopicPartition) -> int:
    """Get end offset for partition."""
    return -1  # Placeholder


__all__ = [
    "TopicPartition",
    "OffsetAndMetadata",
    "OffsetResetStrategy",
    "OffsetManager",
    "AutoCommitManager",
    "get_beginning_offset",
    "get_end_offset",
]
