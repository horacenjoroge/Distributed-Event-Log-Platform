"""
Sequence number tracking for idempotent producers.

Manages sequence numbers per producer-partition for deduplication.
"""

from dataclasses import dataclass
from typing import Dict, Optional, Tuple

from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class SequenceState:
    """
    Sequence state for a producer-partition.
    
    Attributes:
        last_sequence: Last sequence number seen
        last_offset: Offset where last_sequence was written
        last_timestamp: Timestamp of last write (ms)
    """
    last_sequence: int
    last_offset: int
    last_timestamp: int


class SequenceNumberManager:
    """
    Manages sequence numbers for producer-partition pairs.
    
    Tracks the last sequence number per (PID, partition) for deduplication.
    """
    
    def __init__(self):
        """Initialize sequence number manager."""
        # (producer_id, topic, partition) -> SequenceState
        self._sequences: Dict[Tuple[int, str, int], SequenceState] = {}
        
        logger.info("SequenceNumberManager initialized")
    
    def check_sequence(
        self,
        producer_id: int,
        topic: str,
        partition: int,
        sequence: int,
    ) -> Tuple[bool, Optional[str]]:
        """
        Check if sequence number is valid.
        
        Returns (is_valid, error_message).
        
        Cases:
        - First message (no state): Accept
        - Duplicate (same seq): Reject as duplicate
        - In-order (seq + 1): Accept
        - Gap (seq > last + 1): Reject as out-of-order
        - Old (seq < last): Reject as duplicate
        
        Args:
            producer_id: Producer ID
            topic: Topic name
            partition: Partition number
            sequence: Sequence number
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        key = (producer_id, topic, partition)
        
        # First message from this producer-partition
        if key not in self._sequences:
            logger.debug(
                "First message from producer",
                producer_id=producer_id,
                topic=topic,
                partition=partition,
                sequence=sequence,
            )
            return (True, None)
        
        state = self._sequences[key]
        last_seq = state.last_sequence
        
        # Duplicate (exact same sequence)
        if sequence == last_seq:
            logger.warning(
                "Duplicate message detected",
                producer_id=producer_id,
                topic=topic,
                partition=partition,
                sequence=sequence,
            )
            return (False, "Duplicate sequence number")
        
        # In-order (next expected sequence)
        elif sequence == last_seq + 1:
            return (True, None)
        
        # Out-of-order gap (sequence too high)
        elif sequence > last_seq + 1:
            logger.error(
                "Out-of-order message (gap detected)",
                producer_id=producer_id,
                topic=topic,
                partition=partition,
                sequence=sequence,
                last_sequence=last_seq,
                gap=sequence - last_seq - 1,
            )
            return (False, f"Out-of-order: expected {last_seq + 1}, got {sequence}")
        
        # Old message (sequence too low)
        else:  # sequence < last_seq
            logger.warning(
                "Old message detected",
                producer_id=producer_id,
                topic=topic,
                partition=partition,
                sequence=sequence,
                last_sequence=last_seq,
            )
            return (False, "Old sequence number")
    
    def update_sequence(
        self,
        producer_id: int,
        topic: str,
        partition: int,
        sequence: int,
        offset: int,
        timestamp: int,
    ) -> None:
        """
        Update sequence state after successful write.
        
        Args:
            producer_id: Producer ID
            topic: Topic name
            partition: Partition number
            sequence: Sequence number
            offset: Log offset where written
            timestamp: Write timestamp (ms)
        """
        key = (producer_id, topic, partition)
        
        self._sequences[key] = SequenceState(
            last_sequence=sequence,
            last_offset=offset,
            last_timestamp=timestamp,
        )
        
        logger.debug(
            "Updated sequence state",
            producer_id=producer_id,
            topic=topic,
            partition=partition,
            sequence=sequence,
            offset=offset,
        )
    
    def get_last_sequence(
        self,
        producer_id: int,
        topic: str,
        partition: int,
    ) -> Optional[int]:
        """
        Get last sequence number for producer-partition.
        
        Args:
            producer_id: Producer ID
            topic: Topic name
            partition: Partition number
        
        Returns:
            Last sequence number or None
        """
        key = (producer_id, topic, partition)
        
        if key in self._sequences:
            return self._sequences[key].last_sequence
        
        return None
    
    def get_last_offset(
        self,
        producer_id: int,
        topic: str,
        partition: int,
    ) -> Optional[int]:
        """
        Get last offset for producer-partition.
        
        Args:
            producer_id: Producer ID
            topic: Topic name
            partition: Int
        
        Returns:
            Last offset or None
        """
        key = (producer_id, topic, partition)
        
        if key in self._sequences:
            return self._sequences[key].last_offset
        
        return None
    
    def reset_sequence(
        self,
        producer_id: int,
        topic: str,
        partition: int,
    ) -> None:
        """
        Reset sequence state for producer-partition.
        
        Used when producer restarts or times out.
        
        Args:
            producer_id: Producer ID
            topic: Topic name
            partition: Partition number
        """
        key = (producer_id, topic, partition)
        
        if key in self._sequences:
            del self._sequences[key]
            
            logger.info(
                "Reset sequence state",
                producer_id=producer_id,
                topic=topic,
                partition=partition,
            )
    
    def cleanup_producer(
        self,
        producer_id: int,
    ) -> int:
        """
        Clean up all sequences for a producer.
        
        Args:
            producer_id: Producer ID
        
        Returns:
            Number of sequences removed
        """
        keys_to_remove = [
            key for key in self._sequences.keys()
            if key[0] == producer_id
        ]
        
        for key in keys_to_remove:
            del self._sequences[key]
        
        if keys_to_remove:
            logger.info(
                "Cleaned up producer sequences",
                producer_id=producer_id,
                count=len(keys_to_remove),
            )
        
        return len(keys_to_remove)
    
    def get_stats(self) -> Dict:
        """
        Get manager statistics.
        
        Returns:
            Statistics dict
        """
        return {
            "tracked_sequences": len(self._sequences),
        }


class ProducerSequenceTracker:
    """
    Producer-side sequence number tracker.
    
    Tracks sequence numbers for each partition a producer writes to.
    """
    
    def __init__(self, producer_id: int):
        """
        Initialize producer sequence tracker.
        
        Args:
            producer_id: Producer ID
        """
        self.producer_id = producer_id
        
        # (topic, partition) -> next sequence number
        self._next_sequence: Dict[Tuple[str, int], int] = {}
        
        logger.info(
            "ProducerSequenceTracker initialized",
            producer_id=producer_id,
        )
    
    def get_next_sequence(
        self,
        topic: str,
        partition: int,
    ) -> int:
        """
        Get next sequence number for partition.
        
        Automatically increments for next call.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            Next sequence number
        """
        key = (topic, partition)
        
        if key not in self._next_sequence:
            self._next_sequence[key] = 0
        
        sequence = self._next_sequence[key]
        self._next_sequence[key] += 1
        
        return sequence
    
    def reset_sequence(
        self,
        topic: str,
        partition: int,
    ) -> None:
        """
        Reset sequence for partition.
        
        Args:
            topic: Topic name
            partition: Partition number
        """
        key = (topic, partition)
        
        if key in self._next_sequence:
            del self._next_sequence[key]
            
            logger.info(
                "Reset producer sequence",
                producer_id=self.producer_id,
                topic=topic,
                partition=partition,
            )
