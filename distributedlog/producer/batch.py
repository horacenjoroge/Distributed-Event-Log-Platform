"""
Message batching for producer.

Accumulates messages into batches before sending to reduce network overhead.
"""

import time
from dataclasses import dataclass
from typing import Callable, List, Optional

from distributedlog.core.log.format import Message
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ProducerRecord:
    """
    A record to be sent to the broker.
    
    Attributes:
        topic: Topic name
        partition: Partition number (None for auto-assignment)
        key: Message key (None for no key)
        value: Message value
        timestamp: Message timestamp (None for current time)
    """
    topic: str
    partition: Optional[int]
    key: Optional[bytes]
    value: bytes
    timestamp: Optional[int] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = int(time.time() * 1000)


@dataclass
class RecordMetadata:
    """
    Metadata about a successfully sent record.
    
    Attributes:
        topic: Topic name
        partition: Partition number
        offset: Offset assigned by broker
        timestamp: Message timestamp
    """
    topic: str
    partition: int
    offset: int
    timestamp: int


@dataclass
class ProducerBatch:
    """
    A batch of records for a single topic-partition.
    
    Attributes:
        topic: Topic name
        partition: Partition number
        records: List of records in batch
        created_ms: When batch was created
    """
    topic: str
    partition: int
    records: List[ProducerRecord]
    created_ms: int
    
    def __init__(self, topic: str, partition: int):
        """Initialize batch."""
        self.topic = topic
        self.partition = partition
        self.records = []
        self.created_ms = int(time.time() * 1000)
    
    def append(self, record: ProducerRecord) -> None:
        """Add record to batch."""
        self.records.append(record)
    
    def size_bytes(self) -> int:
        """Calculate total size of batch in bytes."""
        total = 0
        for record in self.records:
            total += len(record.value)
            if record.key:
                total += len(record.key)
        return total
    
    def is_full(self, max_size_bytes: int) -> bool:
        """Check if batch has reached max size."""
        return self.size_bytes() >= max_size_bytes
    
    def age_ms(self) -> int:
        """Get age of batch in milliseconds."""
        return int(time.time() * 1000) - self.created_ms
    
    def is_expired(self, max_age_ms: int) -> bool:
        """Check if batch has exceeded max age."""
        return self.age_ms() >= max_age_ms


class RecordAccumulator:
    """
    Accumulates records into batches for efficient sending.
    
    Batches records by topic-partition and triggers sends when:
    1. Batch reaches max size (batch.size)
    2. Batch reaches max age (linger.ms)
    3. Producer flush() is called
    """
    
    def __init__(
        self,
        batch_size: int = 16384,
        linger_ms: int = 0,
        max_in_flight: int = 5,
    ):
        """
        Initialize accumulator.
        
        Args:
            batch_size: Max batch size in bytes
            linger_ms: Max time to wait before sending batch
            max_in_flight: Max number of in-flight requests per connection
        """
        self.batch_size = batch_size
        self.linger_ms = linger_ms
        self.max_in_flight = max_in_flight
        
        self._batches: dict[tuple[str, int], ProducerBatch] = {}
        self._in_flight: dict[tuple[str, int], int] = {}
        
        logger.info(
            "Initialized record accumulator",
            batch_size=batch_size,
            linger_ms=linger_ms,
            max_in_flight=max_in_flight,
        )
    
    def append(
        self,
        record: ProducerRecord,
        partition: int,
    ) -> Optional[ProducerBatch]:
        """
        Append record to batch.
        
        Args:
            record: Record to append
            partition: Target partition
        
        Returns:
            Batch if ready to send, None otherwise
        """
        key = (record.topic, partition)
        
        if key not in self._batches:
            self._batches[key] = ProducerBatch(record.topic, partition)
        
        batch = self._batches[key]
        batch.append(record)
        
        if batch.is_full(self.batch_size):
            logger.debug(
                "Batch full, ready to send",
                topic=record.topic,
                partition=partition,
                size_bytes=batch.size_bytes(),
            )
            return self._batches.pop(key)
        
        return None
    
    def drain_expired_batches(self) -> List[ProducerBatch]:
        """
        Get all batches that have exceeded linger time.
        
        Returns:
            List of expired batches
        """
        expired = []
        
        for key, batch in list(self._batches.items()):
            if batch.is_expired(self.linger_ms):
                expired.append(batch)
                del self._batches[key]
                
                logger.debug(
                    "Batch expired, ready to send",
                    topic=batch.topic,
                    partition=batch.partition,
                    age_ms=batch.age_ms(),
                )
        
        return expired
    
    def drain_all(self) -> List[ProducerBatch]:
        """
        Drain all pending batches (for flush).
        
        Returns:
            List of all batches
        """
        batches = list(self._batches.values())
        self._batches.clear()
        
        logger.debug("Drained all batches", count=len(batches))
        
        return batches
    
    def has_unsent(self) -> bool:
        """Check if there are unsent batches."""
        return len(self._batches) > 0
    
    def increment_in_flight(self, topic: str, partition: int) -> None:
        """Increment in-flight request count."""
        key = (topic, partition)
        self._in_flight[key] = self._in_flight.get(key, 0) + 1
    
    def decrement_in_flight(self, topic: str, partition: int) -> None:
        """Decrement in-flight request count."""
        key = (topic, partition)
        if key in self._in_flight:
            self._in_flight[key] -= 1
            if self._in_flight[key] <= 0:
                del self._in_flight[key]
    
    def can_send(self, topic: str, partition: int) -> bool:
        """Check if we can send to this partition (not at max in-flight)."""
        key = (topic, partition)
        return self._in_flight.get(key, 0) < self.max_in_flight
    
    def pending_count(self) -> int:
        """Get number of pending batches."""
        return len(self._batches)
    
    def in_flight_count(self) -> int:
        """Get total number of in-flight requests."""
        return sum(self._in_flight.values())
