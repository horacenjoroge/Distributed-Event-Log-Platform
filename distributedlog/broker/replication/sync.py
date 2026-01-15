"""
Replica synchronization coordinator.

Coordinates leader-follower replication with ISR management.
"""

import asyncio
from dataclasses import dataclass
from typing import Dict, List, Optional

from distributedlog.broker.replication.ack import AckManager, ProduceRequest, ProduceResponse
from distributedlog.broker.replication.fetcher import LeaderReplicationManager, ReplicationFetcher
from distributedlog.broker.replication.replica import ReplicaManager, ReplicaState
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ReplicationConfig:
    """
    Configuration for replication.
    
    Attributes:
        max_replica_lag: Maximum offset lag for ISR
        max_replica_lag_time_ms: Maximum time lag for ISR
        min_isr: Minimum ISR size for writes
        fetch_interval_ms: Follower fetch interval
        fetch_max_bytes: Maximum bytes per fetch
    """
    max_replica_lag: int = 10
    max_replica_lag_time_ms: int = 10000
    min_isr: int = 1
    fetch_interval_ms: int = 500
    fetch_max_bytes: int = 1048576


class ReplicationCoordinator:
    """
    Coordinates replication between leader and followers.
    
    Responsibilities:
    - Manage replica lifecycle
    - Coordinate leader-follower replication
    - Track ISR and HWM
    - Handle replica failures
    - Provide ack guarantees
    """
    
    def __init__(
        self,
        broker_id: int,
        config: Optional[ReplicationConfig] = None,
    ):
        """
        Initialize replication coordinator.
        
        Args:
            broker_id: This broker's ID
            config: Replication configuration
        """
        self._broker_id = broker_id
        self._config = config or ReplicationConfig()
        
        # Core components
        self._replica_manager = ReplicaManager(
            max_replica_lag=self._config.max_replica_lag,
            max_replica_lag_time_ms=self._config.max_replica_lag_time_ms,
        )
        
        self._leader_manager = LeaderReplicationManager()
        
        self._fetcher = ReplicationFetcher(
            broker_id=broker_id,
            fetch_interval_ms=self._config.fetch_interval_ms,
            max_fetch_bytes=self._config.fetch_max_bytes,
        )
        
        self._ack_manager = AckManager(self._replica_manager)
        
        # Track partitions we're leading
        self._led_partitions: Dict[tuple, bool] = {}
        
        # Track partitions we're following
        self._followed_partitions: Dict[tuple, int] = {}
        
        logger.info(
            "ReplicationCoordinator initialized",
            broker_id=broker_id,
            config=config,
        )
    
    async def become_leader(
        self,
        topic: str,
        partition: int,
        replica_ids: List[int],
    ) -> None:
        """
        Become leader for a partition.
        
        Args:
            topic: Topic name
            partition: Partition number
            replica_ids: All replica broker IDs
        """
        key = (topic, partition)
        
        # Stop following if we were
        if key in self._followed_partitions:
            await self.stop_following(topic, partition)
        
        # Create replica set
        await self._replica_manager.create_partition_replica_set(
            topic=topic,
            partition=partition,
            leader_id=self._broker_id,
            replica_ids=replica_ids,
        )
        
        self._led_partitions[key] = True
        
        logger.info(
            "Became leader",
            topic=topic,
            partition=partition,
            replicas=replica_ids,
        )
    
    async def become_follower(
        self,
        topic: str,
        partition: int,
        leader_id: int,
        start_offset: int = 0,
    ) -> None:
        """
        Become follower for a partition.
        
        Args:
            topic: Topic name
            partition: Partition number
            leader_id: Leader broker ID
            start_offset: Offset to start replicating from
        """
        key = (topic, partition)
        
        # Stop leading if we were
        if key in self._led_partitions:
            del self._led_partitions[key]
        
        # Start fetching from leader
        await self._fetcher.start_fetching(
            topic=topic,
            partition=partition,
            leader_broker_id=leader_id,
            start_offset=start_offset,
        )
        
        self._followed_partitions[key] = leader_id
        
        logger.info(
            "Became follower",
            topic=topic,
            partition=partition,
            leader_id=leader_id,
            start_offset=start_offset,
        )
    
    async def stop_leading(
        self,
        topic: str,
        partition: int,
    ) -> None:
        """
        Stop leading a partition.
        
        Args:
            topic: Topic name
            partition: Partition number
        """
        key = (topic, partition)
        
        if key in self._led_partitions:
            del self._led_partitions[key]
            
            logger.info(
                "Stopped leading",
                topic=topic,
                partition=partition,
            )
    
    async def stop_following(
        self,
        topic: str,
        partition: int,
    ) -> None:
        """
        Stop following a partition.
        
        Args:
            topic: Topic name
            partition: Partition number
        """
        key = (topic, partition)
        
        if key in self._followed_partitions:
            await self._fetcher.stop_fetching(topic, partition)
            del self._followed_partitions[key]
            
            logger.info(
                "Stopped following",
                topic=topic,
                partition=partition,
            )
    
    async def replicate_produce(
        self,
        request: ProduceRequest,
        base_offset: int,
        log_end_offset: int,
    ) -> ProduceResponse:
        """
        Handle produce request with replication.
        
        Args:
            request: Produce request
            base_offset: Starting offset of write
            log_end_offset: New log end offset
        
        Returns:
            Produce response
        """
        key = (request.topic, request.partition)
        
        # Verify we're the leader
        if key not in self._led_partitions:
            return ProduceResponse(
                topic=request.topic,
                partition=request.partition,
                base_offset=0,
                timestamp=0,
                error_code=3,  # Not leader
            )
        
        # Check min ISR requirement
        isr = await self._replica_manager.get_isr(
            request.topic,
            request.partition,
        )
        
        if len(isr) < self._config.min_isr:
            logger.warning(
                "Insufficient ISR for write",
                topic=request.topic,
                partition=request.partition,
                isr_size=len(isr),
                min_isr=self._config.min_isr,
            )
            
            return ProduceResponse(
                topic=request.topic,
                partition=request.partition,
                base_offset=0,
                timestamp=0,
                error_code=4,  # Insufficient ISR
            )
        
        # Wait for ack based on acks mode
        return await self._ack_manager.wait_for_ack(
            request=request,
            base_offset=base_offset,
            log_end_offset=log_end_offset,
        )
    
    async def update_follower_replica(
        self,
        topic: str,
        partition: int,
        broker_id: int,
        log_end_offset: int,
    ) -> None:
        """
        Update follower replica state after fetch.
        
        Args:
            topic: Topic name
            partition: Partition number
            broker_id: Follower broker ID
            log_end_offset: Follower's new log end offset
        """
        await self._replica_manager.update_follower_offset(
            topic=topic,
            partition=partition,
            broker_id=broker_id,
            log_end_offset=log_end_offset,
        )
    
    async def get_high_watermark(
        self,
        topic: str,
        partition: int,
    ) -> int:
        """
        Get high-water mark for partition.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            High-water mark offset
        """
        return await self._replica_manager.get_high_watermark(
            topic,
            partition,
        )
    
    async def get_isr(
        self,
        topic: str,
        partition: int,
    ) -> List[int]:
        """
        Get In-Sync Replicas for partition.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            List of ISR broker IDs
        """
        isr_set = await self._replica_manager.get_isr(topic, partition)
        return sorted(list(isr_set))
    
    def is_leader_for(
        self,
        topic: str,
        partition: int,
    ) -> bool:
        """
        Check if this broker is leader for partition.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            True if leader
        """
        return (topic, partition) in self._led_partitions
    
    def is_follower_for(
        self,
        topic: str,
        partition: int,
    ) -> bool:
        """
        Check if this broker is follower for partition.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            True if follower
        """
        return (topic, partition) in self._followed_partitions
    
    async def shutdown(self) -> None:
        """Shutdown replication coordinator."""
        # Stop all replication
        for topic, partition in list(self._followed_partitions.keys()):
            await self.stop_following(topic, partition)
        
        await self._fetcher.shutdown()
        
        logger.info("ReplicationCoordinator shut down")
