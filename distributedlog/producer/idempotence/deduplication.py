"""
Broker-side deduplication for idempotent producers.

Integrates PID and sequence management with log writes.
"""

import time
from typing import Optional, Tuple

from distributedlog.producer.idempotence.pid import ProducerIdManager
from distributedlog.producer.idempotence.sequence import SequenceNumberManager
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class IdempotentProduceResult:
    """
    Result of an idempotent produce request.
    
    Attributes:
        accepted: Whether message was accepted
        offset: Log offset (if accepted)
        error_code: Error code (0 = success)
        error_message: Error message (if rejected)
        is_duplicate: Whether rejected as duplicate
    """
    
    def __init__(
        self,
        accepted: bool,
        offset: Optional[int] = None,
        error_code: int = 0,
        error_message: Optional[str] = None,
        is_duplicate: bool = False,
    ):
        self.accepted = accepted
        self.offset = offset
        self.error_code = error_code
        self.error_message = error_message
        self.is_duplicate = is_duplicate


class DeduplicationManager:
    """
    Manages broker-side deduplication for idempotent producers.
    
    Coordinates PID and sequence management with log writes.
    """
    
    def __init__(self):
        """Initialize deduplication manager."""
        self.pid_manager = ProducerIdManager()
        self.sequence_manager = SequenceNumberManager()
        
        logger.info("DeduplicationManager initialized")
    
    def validate_produce_request(
        self,
        producer_id: int,
        producer_epoch: int,
        topic: str,
        partition: int,
        sequence: int,
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate idempotent produce request.
        
        Checks:
        1. Producer ID is valid
        2. Sequence number is valid (not duplicate, not gap)
        
        Args:
            producer_id: Producer ID
            producer_epoch: Producer epoch
            topic: Topic name
            partition: Partition number
            sequence: Sequence number
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Validate producer ID
        pid_info = self.pid_manager.get_producer_id_info(producer_id)
        
        if not pid_info:
            logger.warning(
                "Unknown producer ID",
                producer_id=producer_id,
            )
            return (False, "Unknown producer ID")
        
        # Update last used time
        self.pid_manager.update_last_used(producer_id)
        
        # Validate sequence number
        is_valid, error = self.sequence_manager.check_sequence(
            producer_id=producer_id,
            topic=topic,
            partition=partition,
            sequence=sequence,
        )
        
        return (is_valid, error)
    
    def handle_produce_success(
        self,
        producer_id: int,
        topic: str,
        partition: int,
        sequence: int,
        offset: int,
    ) -> None:
        """
        Handle successful produce (update sequence state).
        
        Args:
            producer_id: Producer ID
            topic: Topic name
            partition: Partition number
            sequence: Sequence number
            offset: Log offset where written
        """
        timestamp = int(time.time() * 1000)
        
        self.sequence_manager.update_sequence(
            producer_id=producer_id,
            topic=topic,
            partition=partition,
            sequence=sequence,
            offset=offset,
            timestamp=timestamp,
        )
    
    def get_duplicate_offset(
        self,
        producer_id: int,
        topic: str,
        partition: int,
    ) -> Optional[int]:
        """
        Get offset for duplicate message.
        
        Returns the offset where this exact message was previously written.
        
        Args:
            producer_id: Producer ID
            topic: Topic name
            partition: Partition number
        
        Returns:
            Offset or None
        """
        return self.sequence_manager.get_last_offset(
            producer_id,
            topic,
            partition,
        )
    
    def cleanup_expired_pids(self) -> None:
        """Clean up expired producer IDs and their sequences."""
        # Get expired PIDs
        expired = self.pid_manager.expire_old_pids()
        
        # Clean up sequences for expired PIDs (would need PID list)
        logger.debug("Cleaned up expired PIDs", count=expired)
    
    def get_stats(self) -> dict:
        """
        Get deduplication statistics.
        
        Returns:
            Statistics dict
        """
        return {
            "pid_stats": self.pid_manager.get_stats(),
            "sequence_stats": self.sequence_manager.get_stats(),
        }
