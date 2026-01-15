"""
Idempotent producer implementation.

Producer with enable.idempotence=true for exactly-once delivery.
"""

from typing import Optional

from distributedlog.producer.idempotence import (
    ProducerIdInfo,
    ProducerIdManager,
    ProducerSequenceTracker,
)
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class IdempotentProducerConfig:
    """
    Configuration for idempotent producer.
    
    Attributes:
        enable_idempotence: Enable idempotence
        max_in_flight_requests: Max concurrent requests (must be <= 5 for idempotence)
        retries: Number of retries (infinite for idempotence)
        acks: Acks mode (must be 'all' for idempotence)
    """
    
    def __init__(
        self,
        enable_idempotence: bool = True,
        max_in_flight_requests: int = 5,
        retries: int = 2147483647,  # Max int (infinite retries)
        acks: str = "all",
    ):
        self.enable_idempotence = enable_idempotence
        self.max_in_flight_requests = max_in_flight_requests
        self.retries = retries
        self.acks = acks
        
        # Validate idempotence requirements
        if enable_idempotence:
            if max_in_flight_requests > 5:
                raise ValueError(
                    "max_in_flight_requests must be <= 5 when idempotence is enabled"
                )
            
            if acks != "all":
                raise ValueError(
                    "acks must be 'all' when idempotence is enabled"
                )


class IdempotentProducer:
    """
    Producer with idempotence enabled.
    
    Provides exactly-once semantics by:
    1. Obtaining a Producer ID (PID)
    2. Tracking sequence numbers per partition
    3. Broker deduplicates based on (PID, sequence)
    """
    
    def __init__(
        self,
        client_id: Optional[str] = None,
        config: Optional[IdempotentProducerConfig] = None,
    ):
        """
        Initialize idempotent producer.
        
        Args:
            client_id: Client ID for PID reuse
            config: Idempotent producer configuration
        """
        self.client_id = client_id or f"producer-{id(self)}"
        self.config = config or IdempotentProducerConfig()
        
        # Producer ID (assigned by broker)
        self.producer_id_info: Optional[ProducerIdInfo] = None
        
        # Sequence tracker
        self.sequence_tracker: Optional[ProducerSequenceTracker] = None
        
        # In-flight requests tracking
        self._in_flight_count = 0
        
        logger.info(
            "IdempotentProducer initialized",
            client_id=self.client_id,
            enable_idempotence=self.config.enable_idempotence,
        )
    
    def init_producer_id(
        self,
        pid_manager: ProducerIdManager,
    ) -> None:
        """
        Initialize producer ID from broker.
        
        Args:
            pid_manager: Producer ID manager (broker-side)
        """
        self.producer_id_info = pid_manager.assign_producer_id(self.client_id)
        
        self.sequence_tracker = ProducerSequenceTracker(
            producer_id=self.producer_id_info.producer_id
        )
        
        logger.info(
            "Producer ID assigned",
            client_id=self.client_id,
            producer_id=self.producer_id_info.producer_id,
            producer_epoch=self.producer_id_info.producer_epoch,
        )
    
    def prepare_send(
        self,
        topic: str,
        partition: int,
    ) -> dict:
        """
        Prepare idempotent send metadata.
        
        Returns metadata to include with produce request.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            Metadata dict with PID, epoch, sequence
        """
        if not self.producer_id_info:
            raise RuntimeError("Producer ID not initialized")
        
        if not self.sequence_tracker:
            raise RuntimeError("Sequence tracker not initialized")
        
        # Check in-flight limit
        if self._in_flight_count >= self.config.max_in_flight_requests:
            raise RuntimeError("Too many in-flight requests")
        
        # Get next sequence number
        sequence = self.sequence_tracker.get_next_sequence(topic, partition)
        
        self._in_flight_count += 1
        
        metadata = {
            "producer_id": self.producer_id_info.producer_id,
            "producer_epoch": self.producer_id_info.producer_epoch,
            "sequence": sequence,
        }
        
        logger.debug(
            "Prepared idempotent send",
            topic=topic,
            partition=partition,
            producer_id=metadata["producer_id"],
            sequence=sequence,
        )
        
        return metadata
    
    def handle_send_complete(
        self,
        topic: str,
        partition: int,
        success: bool,
    ) -> None:
        """
        Handle send completion.
        
        Args:
            topic: Topic name
            partition: Partition number
            success: Whether send succeeded
        """
        self._in_flight_count -= 1
        
        if not success:
            logger.warning(
                "Send failed, will retry with same sequence",
                topic=topic,
                partition=partition,
            )
    
    def reset_sequence(
        self,
        topic: str,
        partition: int,
    ) -> None:
        """
        Reset sequence for partition (on fatal error).
        
        Args:
            topic: Topic name
            partition: Partition number
        """
        if self.sequence_tracker:
            self.sequence_tracker.reset_sequence(topic, partition)
    
    def get_producer_id(self) -> Optional[int]:
        """
        Get producer ID.
        
        Returns:
            Producer ID or None
        """
        if self.producer_id_info:
            return self.producer_id_info.producer_id
        
        return None
    
    def is_initialized(self) -> bool:
        """
        Check if producer ID is initialized.
        
        Returns:
            True if initialized
        """
        return self.producer_id_info is not None
