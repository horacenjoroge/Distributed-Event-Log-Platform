"""
Replication throttling for partition reassignment.

Limits bandwidth usage during reassignment to avoid impacting production traffic.
"""

import asyncio
import time
from typing import Dict

from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class ReplicationThrottle:
    """
    Throttles replication bandwidth during reassignment.
    
    Uses token bucket algorithm for rate limiting.
    """
    
    def __init__(
        self,
        bytes_per_second: int = 10485760,  # 10 MB/s default
    ):
        """
        Initialize replication throttle.
        
        Args:
            bytes_per_second: Maximum bytes per second
        """
        self.bytes_per_second = bytes_per_second
        
        # Token bucket
        self.tokens = 0.0
        self.max_tokens = float(bytes_per_second)
        self.last_update = time.time()
        
        # Statistics
        self.total_bytes_throttled = 0
        self.total_wait_time_ms = 0
        
        logger.info(
            "ReplicationThrottle initialized",
            bytes_per_second=bytes_per_second,
        )
    
    async def acquire(self, bytes_requested: int) -> None:
        """
        Acquire tokens for replication.
        
        Blocks until enough tokens available.
        
        Args:
            bytes_requested: Number of bytes to replicate
        """
        start_time = time.time()
        
        while True:
            # Refill tokens
            self._refill_tokens()
            
            # Check if we have enough tokens
            if self.tokens >= bytes_requested:
                self.tokens -= bytes_requested
                self.total_bytes_throttled += bytes_requested
                break
            
            # Wait for tokens to refill
            wait_time = (bytes_requested - self.tokens) / self.bytes_per_second
            await asyncio.sleep(wait_time)
        
        # Track wait time
        end_time = time.time()
        wait_ms = int((end_time - start_time) * 1000)
        self.total_wait_time_ms += wait_ms
        
        if wait_ms > 100:  # Log if we waited more than 100ms
            logger.debug(
                "Throttled replication",
                bytes_requested=bytes_requested,
                wait_ms=wait_ms,
            )
    
    def _refill_tokens(self) -> None:
        """Refill token bucket based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_update
        
        # Add tokens based on elapsed time
        new_tokens = elapsed * self.bytes_per_second
        self.tokens = min(self.max_tokens, self.tokens + new_tokens)
        
        self.last_update = now
    
    def set_rate(self, bytes_per_second: int) -> None:
        """
        Update throttle rate.
        
        Args:
            bytes_per_second: New rate limit
        """
        self.bytes_per_second = bytes_per_second
        self.max_tokens = float(bytes_per_second)
        
        logger.info(
            "Updated throttle rate",
            bytes_per_second=bytes_per_second,
        )
    
    def get_stats(self) -> Dict:
        """
        Get throttling statistics.
        
        Returns:
            Statistics dict
        """
        return {
            "bytes_per_second": self.bytes_per_second,
            "current_tokens": int(self.tokens),
            "total_bytes_throttled": self.total_bytes_throttled,
            "total_wait_time_ms": self.total_wait_time_ms,
        }


class PartitionThrottleManager:
    """
    Manages throttling for multiple partitions.
    
    Provides per-partition throttles and global limits.
    """
    
    def __init__(
        self,
        global_bytes_per_second: int = 10485760,  # 10 MB/s
    ):
        """
        Initialize partition throttle manager.
        
        Args:
            global_bytes_per_second: Global bandwidth limit
        """
        self.global_throttle = ReplicationThrottle(global_bytes_per_second)
        
        # Per-partition throttles: (topic, partition) -> ReplicationThrottle
        self._partition_throttles: Dict[tuple, ReplicationThrottle] = {}
        
        logger.info(
            "PartitionThrottleManager initialized",
            global_bytes_per_second=global_bytes_per_second,
        )
    
    def get_or_create_throttle(
        self,
        topic: str,
        partition: int,
        bytes_per_second: int = 1048576,  # 1 MB/s per partition
    ) -> ReplicationThrottle:
        """
        Get or create throttle for partition.
        
        Args:
            topic: Topic name
            partition: Partition number
            bytes_per_second: Per-partition limit
        
        Returns:
            Replication throttle
        """
        key = (topic, partition)
        
        if key not in self._partition_throttles:
            self._partition_throttles[key] = ReplicationThrottle(bytes_per_second)
            
            logger.info(
                "Created partition throttle",
                topic=topic,
                partition=partition,
                bytes_per_second=bytes_per_second,
            )
        
        return self._partition_throttles[key]
    
    async def acquire(
        self,
        topic: str,
        partition: int,
        bytes_requested: int,
    ) -> None:
        """
        Acquire tokens for partition replication.
        
        Enforces both per-partition and global limits.
        
        Args:
            topic: Topic name
            partition: Partition number
            bytes_requested: Bytes to replicate
        """
        # Acquire from global throttle first
        await self.global_throttle.acquire(bytes_requested)
        
        # Then acquire from partition throttle
        partition_throttle = self.get_or_create_throttle(topic, partition)
        await partition_throttle.acquire(bytes_requested)
    
    def set_global_rate(self, bytes_per_second: int) -> None:
        """
        Set global throttle rate.
        
        Args:
            bytes_per_second: New global rate
        """
        self.global_throttle.set_rate(bytes_per_second)
    
    def set_partition_rate(
        self,
        topic: str,
        partition: int,
        bytes_per_second: int,
    ) -> None:
        """
        Set per-partition throttle rate.
        
        Args:
            topic: Topic name
            partition: Partition number
            bytes_per_second: New partition rate
        """
        throttle = self.get_or_create_throttle(topic, partition, bytes_per_second)
        throttle.set_rate(bytes_per_second)
    
    def remove_partition_throttle(
        self,
        topic: str,
        partition: int,
    ) -> None:
        """
        Remove partition throttle (after reassignment complete).
        
        Args:
            topic: Topic name
            partition: Partition number
        """
        key = (topic, partition)
        
        if key in self._partition_throttles:
            del self._partition_throttles[key]
            
            logger.info(
                "Removed partition throttle",
                topic=topic,
                partition=partition,
            )
    
    def get_global_stats(self) -> Dict:
        """
        Get global throttling statistics.
        
        Returns:
            Statistics dict
        """
        return self.global_throttle.get_stats()
    
    def get_partition_stats(
        self,
        topic: str,
        partition: int,
    ) -> Dict:
        """
        Get partition throttling statistics.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            Statistics dict or None
        """
        key = (topic, partition)
        
        if key in self._partition_throttles:
            return self._partition_throttles[key].get_stats()
        
        return None
