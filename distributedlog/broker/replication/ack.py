"""
Producer acknowledgment modes for replication.

Implements different durability guarantees:
- acks=0: No acknowledgment (fire and forget)
- acks=1: Leader acknowledgment only
- acks=all: Full ISR acknowledgment
"""

import asyncio
import time
from dataclasses import dataclass
from enum import IntEnum
from typing import Optional, Set

from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class AckMode(IntEnum):
    """Producer acknowledgment modes."""
    
    NONE = 0       # No ack (fire and forget)
    LEADER = 1     # Leader ack only
    ALL = -1       # All ISR replicas must ack


@dataclass
class ProduceRequest:
    """
    Request to produce messages.
    
    Attributes:
        topic: Topic name
        partition: Partition number
        messages: Messages to write
        acks: Required acknowledgment mode
        timeout_ms: Request timeout
        request_id: Unique request ID
    """
    topic: str
    partition: int
    messages: list
    acks: AckMode = AckMode.ALL
    timeout_ms: int = 30000
    request_id: Optional[str] = None


@dataclass
class ProduceResponse:
    """
    Response for produce request.
    
    Attributes:
        topic: Topic name
        partition: Partition number
        base_offset: Starting offset of written messages
        timestamp: Append timestamp
        error_code: Error code (0 = success)
    """
    topic: str
    partition: int
    base_offset: int
    timestamp: int
    error_code: int = 0


class AckManager:
    """
    Manages producer acknowledgments based on ack mode.
    
    Coordinates with ReplicaManager to wait for ISR replication
    before acknowledging producers.
    """
    
    def __init__(self, replica_manager):
        """
        Initialize ack manager.
        
        Args:
            replica_manager: ReplicaManager instance
        """
        self._replica_manager = replica_manager
        
        # Track pending acks: (topic, partition, offset) -> Future
        self._pending_acks: dict = {}
        self._lock = asyncio.Lock()
        
        logger.info("AckManager initialized")
    
    async def wait_for_ack(
        self,
        request: ProduceRequest,
        base_offset: int,
        log_end_offset: int,
    ) -> ProduceResponse:
        """
        Wait for acknowledgment based on acks mode.
        
        Args:
            request: Produce request
            base_offset: Starting offset of written messages
            log_end_offset: New log end offset after write
        
        Returns:
            Produce response
        """
        if request.acks == AckMode.NONE:
            # No ack required
            return ProduceResponse(
                topic=request.topic,
                partition=request.partition,
                base_offset=base_offset,
                timestamp=int(time.time() * 1000),
            )
        
        elif request.acks == AckMode.LEADER:
            # Leader ack only (already written to leader log)
            return ProduceResponse(
                topic=request.topic,
                partition=request.partition,
                base_offset=base_offset,
                timestamp=int(time.time() * 1000),
            )
        
        elif request.acks == AckMode.ALL:
            # Wait for all ISR replicas
            return await self._wait_for_isr_ack(
                request,
                base_offset,
                log_end_offset,
            )
        
        else:
            # Unknown ack mode
            return ProduceResponse(
                topic=request.topic,
                partition=request.partition,
                base_offset=base_offset,
                timestamp=int(time.time() * 1000),
                error_code=1,  # Error
            )
    
    async def _wait_for_isr_ack(
        self,
        request: ProduceRequest,
        base_offset: int,
        target_offset: int,
    ) -> ProduceResponse:
        """
        Wait for all ISR replicas to replicate.
        
        Args:
            request: Produce request
            base_offset: Starting offset
            target_offset: Target log end offset
        
        Returns:
            Produce response
        """
        start_time = time.time()
        timeout_s = request.timeout_ms / 1000
        
        while True:
            # Check if high-water mark has advanced
            hwm = await self._replica_manager.get_high_watermark(
                request.topic,
                request.partition,
            )
            
            if hwm >= target_offset:
                # All ISR replicas have replicated
                logger.debug(
                    "ISR acknowledgment complete",
                    topic=request.topic,
                    partition=request.partition,
                    offset=base_offset,
                    hwm=hwm,
                )
                
                return ProduceResponse(
                    topic=request.topic,
                    partition=request.partition,
                    base_offset=base_offset,
                    timestamp=int(time.time() * 1000),
                )
            
            # Check timeout
            elapsed = time.time() - start_time
            if elapsed > timeout_s:
                logger.warning(
                    "ISR acknowledgment timeout",
                    topic=request.topic,
                    partition=request.partition,
                    timeout_ms=request.timeout_ms,
                )
                
                return ProduceResponse(
                    topic=request.topic,
                    partition=request.partition,
                    base_offset=base_offset,
                    timestamp=int(time.time() * 1000),
                    error_code=2,  # Timeout
                )
            
            # Wait a bit before checking again
            await asyncio.sleep(0.01)


class ReplicationMonitor:
    """
    Monitors replication lag and health.
    
    Provides metrics and alerts for:
    - Per-replica lag
    - Under-replicated partitions
    - ISR shrink/expand events
    """
    
    def __init__(self, replica_manager):
        """
        Initialize replication monitor.
        
        Args:
            replica_manager: ReplicaManager instance
        """
        self._replica_manager = replica_manager
        
        # Metrics
        self._under_replicated_partitions: Set[tuple] = set()
        self._replica_lag_metrics: dict = {}
        
        self._lock = asyncio.Lock()
        self._monitoring_task: Optional[asyncio.Task] = None
        
        logger.info("ReplicationMonitor initialized")
    
    async def start(self, interval_s: int = 10) -> None:
        """
        Start monitoring.
        
        Args:
            interval_s: Monitoring interval in seconds
        """
        if self._monitoring_task:
            return
        
        self._monitoring_task = asyncio.create_task(
            self._monitoring_loop(interval_s)
        )
        
        logger.info("ReplicationMonitor started", interval_s=interval_s)
    
    async def stop(self) -> None:
        """Stop monitoring."""
        if self._monitoring_task:
            self._monitoring_task.cancel()
            
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
            
            self._monitoring_task = None
        
        logger.info("ReplicationMonitor stopped")
    
    async def _monitoring_loop(self, interval_s: int) -> None:
        """
        Continuous monitoring loop.
        
        Args:
            interval_s: Interval between checks
        """
        while True:
            try:
                await self._check_replication_health()
                await asyncio.sleep(interval_s)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    "Error in monitoring loop",
                    error=str(e),
                )
                await asyncio.sleep(interval_s)
    
    async def _check_replication_health(self) -> None:
        """Check replication health for all partitions."""
        # Check replica health
        await self._replica_manager.check_replica_health()
        
        # TODO: Collect and report metrics
        # - Under-replicated partition count
        # - Per-replica lag
        # - ISR size
    
    async def get_under_replicated_partitions(self) -> Set[tuple]:
        """
        Get set of under-replicated partitions.
        
        Returns:
            Set of (topic, partition) tuples
        """
        async with self._lock:
            return self._under_replicated_partitions.copy()
    
    async def get_replica_lag(
        self,
        topic: str,
        partition: int,
        broker_id: int,
    ) -> Optional[int]:
        """
        Get current lag for a replica.
        
        Args:
            topic: Topic name
            partition: Partition number
            broker_id: Broker ID
        
        Returns:
            Lag in messages or None
        """
        async with self._lock:
            key = (topic, partition, broker_id)
            return self._replica_lag_metrics.get(key)
