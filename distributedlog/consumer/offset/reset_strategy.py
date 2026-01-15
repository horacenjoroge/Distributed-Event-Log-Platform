"""
Offset reset strategies for handling missing committed offsets.

When a consumer has no committed offset, these strategies determine
where to start consuming from.
"""

from enum import Enum
from typing import Optional

from distributedlog.consumer.offset import TopicPartition
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class OffsetResetStrategy(str, Enum):
    """
    Strategies for resetting offsets when no committed offset exists.
    """
    EARLIEST = "earliest"  # Start from beginning (offset 0)
    LATEST = "latest"      # Start from end (current log end)
    NONE = "none"          # Raise exception if no offset
    

def get_beginning_offset(topic_partition: TopicPartition) -> int:
    """
    Get the beginning offset for a partition.
    
    Args:
        topic_partition: Topic-partition
    
    Returns:
        Beginning offset (0)
    """
    return 0


def get_end_offset(topic_partition: TopicPartition) -> int:
    """
    Get the end offset for a partition.
    
    In a real implementation, this would query the broker for the
    current log end offset. For now, we return a placeholder.
    
    Args:
        topic_partition: Topic-partition
    
    Returns:
        End offset (placeholder: -1 means "current end")
    """
    return -1


class OffsetResetHandler:
    """
    Handles offset reset based on configured strategy.
    
    When a consumer has no committed offset for a partition,
    this handler determines the initial offset to use.
    """
    
    def __init__(self, strategy: str = OffsetResetStrategy.LATEST):
        """
        Initialize offset reset handler.
        
        Args:
            strategy: Reset strategy (earliest, latest, none)
        """
        self.strategy = OffsetResetStrategy(strategy)
        
        logger.info(
            "OffsetResetHandler initialized",
            strategy=self.strategy,
        )
    
    def reset_offset(
        self,
        topic_partition: TopicPartition,
        log_end_offset: Optional[int] = None,
    ) -> int:
        """
        Get reset offset based on strategy.
        
        Args:
            topic_partition: Topic-partition
            log_end_offset: Current log end offset (if known)
        
        Returns:
            Offset to start from
        
        Raises:
            ValueError: If strategy is NONE and no offset exists
        """
        if self.strategy == OffsetResetStrategy.EARLIEST:
            offset = get_beginning_offset(topic_partition)
            
            logger.info(
                "Reset to earliest",
                topic=topic_partition.topic,
                partition=topic_partition.partition,
                offset=offset,
            )
            
            return offset
        
        elif self.strategy == OffsetResetStrategy.LATEST:
            if log_end_offset is not None:
                offset = log_end_offset
            else:
                offset = get_end_offset(topic_partition)
            
            logger.info(
                "Reset to latest",
                topic=topic_partition.topic,
                partition=topic_partition.partition,
                offset=offset,
            )
            
            return offset
        
        elif self.strategy == OffsetResetStrategy.NONE:
            raise ValueError(
                f"No committed offset for {topic_partition.topic}-"
                f"{topic_partition.partition} and auto.offset.reset is 'none'"
            )
        
        else:
            raise ValueError(f"Unknown offset reset strategy: {self.strategy}")
    
    def should_reset(
        self,
        topic_partition: TopicPartition,
        committed_offset: Optional[int],
    ) -> bool:
        """
        Check if offset should be reset.
        
        Args:
            topic_partition: Topic-partition
            committed_offset: Committed offset (if any)
        
        Returns:
            True if offset should be reset
        """
        return committed_offset is None
