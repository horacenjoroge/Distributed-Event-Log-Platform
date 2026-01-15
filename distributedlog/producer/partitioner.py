"""
Partitioner for choosing partition for each message.

Strategies:
- Key-based: Hash key to partition (same key → same partition)
- Round-robin: Distribute evenly across partitions
- Custom: User-defined partitioning logic
"""

import hashlib
from abc import ABC, abstractmethod
from typing import Optional

from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class Partitioner(ABC):
    """Abstract base class for partitioners."""
    
    @abstractmethod
    def partition(
        self,
        topic: str,
        key: Optional[bytes],
        value: bytes,
        num_partitions: int,
    ) -> int:
        """
        Choose partition for message.
        
        Args:
            topic: Topic name
            key: Message key (None for no key)
            value: Message value
            num_partitions: Number of available partitions
        
        Returns:
            Partition number (0 to num_partitions-1)
        """
        pass


class DefaultPartitioner(Partitioner):
    """
    Default partitioner with hybrid strategy.
    
    - If key is present: Hash key to partition (sticky)
    - If no key: Round-robin across partitions
    """
    
    def __init__(self):
        """Initialize partitioner."""
        self._counter = 0
        logger.info("Initialized default partitioner")
    
    def partition(
        self,
        topic: str,
        key: Optional[bytes],
        value: bytes,
        num_partitions: int,
    ) -> int:
        """
        Choose partition using hybrid strategy.
        
        Args:
            topic: Topic name
            key: Message key
            value: Message value
            num_partitions: Number of partitions
        
        Returns:
            Partition number
        """
        if num_partitions <= 0:
            raise ValueError(f"Invalid num_partitions: {num_partitions}")
        
        if key is not None:
            partition = self._hash_partition(key, num_partitions)
        else:
            partition = self._round_robin_partition(num_partitions)
        
        logger.debug(
            "Assigned partition",
            topic=topic,
            partition=partition,
            has_key=key is not None,
        )
        
        return partition
    
    def _hash_partition(self, key: bytes, num_partitions: int) -> int:
        """
        Hash key to partition.
        
        Uses murmur3-like hashing for consistent distribution.
        """
        hash_value = int(hashlib.md5(key).hexdigest(), 16)
        return hash_value % num_partitions
    
    def _round_robin_partition(self, num_partitions: int) -> int:
        """
        Round-robin partition assignment.
        
        Distributes messages evenly across partitions.
        """
        partition = self._counter % num_partitions
        self._counter += 1
        return partition


class KeyHashPartitioner(Partitioner):
    """
    Key-based hash partitioner.
    
    Always uses key hash, requires key to be present.
    """
    
    def __init__(self):
        """Initialize partitioner."""
        logger.info("Initialized key-hash partitioner")
    
    def partition(
        self,
        topic: str,
        key: Optional[bytes],
        value: bytes,
        num_partitions: int,
    ) -> int:
        """
        Hash key to partition.
        
        Args:
            topic: Topic name
            key: Message key (required)
            value: Message value
            num_partitions: Number of partitions
        
        Returns:
            Partition number
        
        Raises:
            ValueError: If key is None
        """
        if key is None:
            raise ValueError("KeyHashPartitioner requires key to be set")
        
        if num_partitions <= 0:
            raise ValueError(f"Invalid num_partitions: {num_partitions}")
        
        hash_value = int(hashlib.md5(key).hexdigest(), 16)
        partition = hash_value % num_partitions
        
        logger.debug(
            "Hashed key to partition",
            topic=topic,
            partition=partition,
            key_size=len(key),
        )
        
        return partition


class RoundRobinPartitioner(Partitioner):
    """
    Round-robin partitioner.
    
    Distributes messages evenly regardless of key.
    """
    
    def __init__(self):
        """Initialize partitioner."""
        self._counter = 0
        logger.info("Initialized round-robin partitioner")
    
    def partition(
        self,
        topic: str,
        key: Optional[bytes],
        value: bytes,
        num_partitions: int,
    ) -> int:
        """
        Round-robin partition assignment.
        
        Args:
            topic: Topic name
            key: Message key (ignored)
            value: Message value
            num_partitions: Number of partitions
        
        Returns:
            Partition number
        """
        if num_partitions <= 0:
            raise ValueError(f"Invalid num_partitions: {num_partitions}")
        
        partition = self._counter % num_partitions
        self._counter += 1
        
        logger.debug(
            "Round-robin assigned partition",
            topic=topic,
            partition=partition,
        )
        
        return partition


class RandomPartitioner(Partitioner):
    """
    Random partitioner.
    
    Randomly distributes messages across partitions.
    """
    
    def __init__(self):
        """Initialize partitioner."""
        import random
        self._random = random
        logger.info("Initialized random partitioner")
    
    def partition(
        self,
        topic: str,
        key: Optional[bytes],
        value: bytes,
        num_partitions: int,
    ) -> int:
        """
        Randomly assign partition.
        
        Args:
            topic: Topic name
            key: Message key (ignored)
            value: Message value
            num_partitions: Number of partitions
        
        Returns:
            Partition number
        """
        if num_partitions <= 0:
            raise ValueError(f"Invalid num_partitions: {num_partitions}")
        
        partition = self._random.randint(0, num_partitions - 1)
        
        logger.debug(
            "Randomly assigned partition",
            topic=topic,
            partition=partition,
        )
        
        return partition


class StickyPartitioner(Partitioner):
    """
    Sticky partitioner for better batching.
    
    Sends to same partition until batch is full, then switches.
    Improves batching efficiency for messages without keys.
    """
    
    def __init__(self):
        """Initialize partitioner."""
        self._current_partition = {}
        self._message_count = {}
        self._sticky_batch_size = 100
        logger.info("Initialized sticky partitioner")
    
    def partition(
        self,
        topic: str,
        key: Optional[bytes],
        value: bytes,
        num_partitions: int,
    ) -> int:
        """
        Sticky partition assignment.
        
        Args:
            topic: Topic name
            key: Message key (if present, use hash)
            value: Message value
            num_partitions: Number of partitions
        
        Returns:
            Partition number
        """
        if num_partitions <= 0:
            raise ValueError(f"Invalid num_partitions: {num_partitions}")
        
        if key is not None:
            hash_value = int(hashlib.md5(key).hexdigest(), 16)
            return hash_value % num_partitions
        
        if topic not in self._current_partition:
            self._current_partition[topic] = 0
            self._message_count[topic] = 0
        
        self._message_count[topic] += 1
        
        if self._message_count[topic] >= self._sticky_batch_size:
            self._current_partition[topic] = (
                (self._current_partition[topic] + 1) % num_partitions
            )
            self._message_count[topic] = 0
        
        partition = self._current_partition[topic]
        
        logger.debug(
            "Sticky assigned partition",
            topic=topic,
            partition=partition,
            messages_in_batch=self._message_count[topic],
        )
        
        return partition


def create_partitioner(partitioner_type: str = "default") -> Partitioner:
    """
    Factory method to create partitioner.
    
    Args:
        partitioner_type: Type of partitioner
            - "default": Hybrid (hash key or round-robin)
            - "key_hash": Always hash key
            - "round_robin": Always round-robin
            - "random": Random assignment
            - "sticky": Sticky for better batching
    
    Returns:
        Partitioner instance
    """
    partitioners = {
        "default": DefaultPartitioner,
        "key_hash": KeyHashPartitioner,
        "round_robin": RoundRobinPartitioner,
        "random": RandomPartitioner,
        "sticky": StickyPartitioner,
    }
    
    partitioner_class = partitioners.get(partitioner_type)
    
    if partitioner_class is None:
        raise ValueError(f"Unknown partitioner type: {partitioner_type}")
    
    return partitioner_class()
