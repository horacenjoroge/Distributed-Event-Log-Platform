"""
Replica management for partition replication.

Handles leader-follower replication with ISR tracking.
"""

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set

from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class ReplicaState(str, Enum):
    """Replica states."""
    
    ONLINE = "online"              # Replica is online and replicating
    OFFLINE = "offline"            # Replica is offline
    SYNCING = "syncing"           # Replica is catching up
    IN_SYNC = "in_sync"           # Replica is in-sync with leader


@dataclass
class ReplicaInfo:
    """
    Information about a partition replica.
    
    Attributes:
        broker_id: Broker hosting this replica
        topic: Topic name
        partition: Partition number
        log_end_offset: Current end offset of replica's log
        high_watermark: High-water mark (committed offset)
        last_fetch_time: Last time follower fetched from leader
        last_caught_up_time: Last time replica was caught up
        state: Current replica state
    """
    broker_id: int
    topic: str
    partition: int
    log_end_offset: int = 0
    high_watermark: int = 0
    last_fetch_time: int = 0
    last_caught_up_time: int = 0
    state: ReplicaState = ReplicaState.SYNCING
    
    def __post_init__(self):
        if self.last_fetch_time == 0:
            self.last_fetch_time = int(time.time() * 1000)
        if self.last_caught_up_time == 0:
            self.last_caught_up_time = int(time.time() * 1000)
    
    def update_fetch_time(self) -> None:
        """Update last fetch time."""
        self.last_fetch_time = int(time.time() * 1000)
    
    def is_caught_up(self, leader_offset: int, max_lag: int = 10) -> bool:
        """
        Check if replica is caught up with leader.
        
        Args:
            leader_offset: Leader's log end offset
            max_lag: Maximum acceptable lag
        
        Returns:
            True if caught up
        """
        lag = leader_offset - self.log_end_offset
        return lag <= max_lag
    
    def is_lagging(
        self,
        current_time_ms: int,
        max_lag_time_ms: int = 10000,
    ) -> bool:
        """
        Check if replica is lagging based on time.
        
        Args:
            current_time_ms: Current time in milliseconds
            max_lag_time_ms: Maximum acceptable lag time
        
        Returns:
            True if lagging
        """
        time_since_fetch = current_time_ms - self.last_fetch_time
        return time_since_fetch > max_lag_time_ms


@dataclass
class PartitionReplicaSet:
    """
    Manages replicas for a single partition.
    
    Tracks leader, followers, and In-Sync Replicas (ISR).
    """
    topic: str
    partition: int
    leader_id: int
    replicas: Dict[int, ReplicaInfo] = field(default_factory=dict)
    isr: Set[int] = field(default_factory=set)
    high_watermark: int = 0
    leader_epoch: int = 0
    
    def __post_init__(self):
        """Initialize leader in ISR."""
        self.isr.add(self.leader_id)
    
    def add_replica(self, replica: ReplicaInfo) -> None:
        """
        Add replica to set.
        
        Args:
            replica: Replica info
        """
        self.replicas[replica.broker_id] = replica
        
        logger.info(
            "Added replica",
            topic=self.topic,
            partition=self.partition,
            broker_id=replica.broker_id,
        )
    
    def update_replica_offset(
        self,
        broker_id: int,
        log_end_offset: int,
    ) -> None:
        """
        Update replica's log end offset.
        
        Args:
            broker_id: Broker ID
            log_end_offset: New log end offset
        """
        if broker_id in self.replicas:
            replica = self.replicas[broker_id]
            replica.log_end_offset = log_end_offset
            replica.update_fetch_time()
            
            logger.debug(
                "Updated replica offset",
                topic=self.topic,
                partition=self.partition,
                broker_id=broker_id,
                offset=log_end_offset,
            )
    
    def update_isr(self, leader_offset: int, max_lag: int = 10) -> bool:
        """
        Update In-Sync Replicas based on current lag.
        
        Args:
            leader_offset: Leader's log end offset
            max_lag: Maximum acceptable lag
        
        Returns:
            True if ISR changed
        """
        old_isr = self.isr.copy()
        new_isr = {self.leader_id}  # Leader always in ISR
        
        for broker_id, replica in self.replicas.items():
            if broker_id == self.leader_id:
                continue
            
            if replica.is_caught_up(leader_offset, max_lag):
                new_isr.add(broker_id)
                
                if replica.state != ReplicaState.IN_SYNC:
                    replica.state = ReplicaState.IN_SYNC
                    replica.last_caught_up_time = int(time.time() * 1000)
            else:
                if replica.state == ReplicaState.IN_SYNC:
                    replica.state = ReplicaState.SYNCING
        
        self.isr = new_isr
        
        if old_isr != new_isr:
            logger.info(
                "ISR changed",
                topic=self.topic,
                partition=self.partition,
                old_isr=sorted(old_isr),
                new_isr=sorted(new_isr),
            )
            return True
        
        return False
    
    def remove_lagging_replicas(
        self,
        current_time_ms: int,
        max_lag_time_ms: int = 10000,
    ) -> Set[int]:
        """
        Remove lagging replicas from ISR.
        
        Args:
            current_time_ms: Current time in milliseconds
            max_lag_time_ms: Maximum lag time
        
        Returns:
            Set of removed broker IDs
        """
        removed = set()
        
        for broker_id in list(self.isr):
            if broker_id == self.leader_id:
                continue
            
            if broker_id not in self.replicas:
                continue
            
            replica = self.replicas[broker_id]
            
            if replica.is_lagging(current_time_ms, max_lag_time_ms):
                self.isr.discard(broker_id)
                replica.state = ReplicaState.SYNCING
                removed.add(broker_id)
                
                logger.warning(
                    "Removed lagging replica from ISR",
                    topic=self.topic,
                    partition=self.partition,
                    broker_id=broker_id,
                    time_since_fetch=current_time_ms - replica.last_fetch_time,
                )
        
        return removed
    
    def calculate_high_watermark(self) -> int:
        """
        Calculate high-water mark (minimum offset in ISR).
        
        Returns:
            High-water mark offset
        """
        if not self.isr:
            return 0
        
        # Get leader offset
        leader_replica = self.replicas.get(self.leader_id)
        if not leader_replica:
            return self.high_watermark
        
        min_offset = leader_replica.log_end_offset
        
        # Find minimum offset among ISR replicas
        for broker_id in self.isr:
            if broker_id == self.leader_id:
                continue
            
            if broker_id in self.replicas:
                replica = self.replicas[broker_id]
                min_offset = min(min_offset, replica.log_end_offset)
        
        old_hwm = self.high_watermark
        self.high_watermark = min_offset
        
        if old_hwm != self.high_watermark:
            logger.debug(
                "High-water mark advanced",
                topic=self.topic,
                partition=self.partition,
                old_hwm=old_hwm,
                new_hwm=self.high_watermark,
            )
        
        return self.high_watermark
    
    def is_under_replicated(self, replication_factor: int) -> bool:
        """
        Check if partition is under-replicated.
        
        Args:
            replication_factor: Expected replication factor
        
        Returns:
            True if under-replicated
        """
        return len(self.isr) < replication_factor
    
    def get_replication_lag(self, broker_id: int) -> Optional[int]:
        """
        Get replication lag for a replica.
        
        Args:
            broker_id: Broker ID
        
        Returns:
            Lag in messages or None
        """
        if broker_id not in self.replicas:
            return None
        
        leader_replica = self.replicas.get(self.leader_id)
        if not leader_replica:
            return None
        
        follower_replica = self.replicas[broker_id]
        
        return leader_replica.log_end_offset - follower_replica.log_end_offset


class ReplicaManager:
    """
    Manages partition replicas across the cluster.
    
    Responsibilities:
    - Track partition replicas
    - Maintain ISR per partition
    - Calculate high-water marks
    - Monitor replication lag
    - Handle replica failures
    """
    
    def __init__(
        self,
        max_replica_lag: int = 10,
        max_replica_lag_time_ms: int = 10000,
    ):
        """
        Initialize replica manager.
        
        Args:
            max_replica_lag: Maximum offset lag for ISR
            max_replica_lag_time_ms: Maximum time lag for ISR
        """
        self._partitions: Dict[tuple, PartitionReplicaSet] = {}
        self._max_lag = max_replica_lag
        self._max_lag_time_ms = max_replica_lag_time_ms
        self._lock = asyncio.Lock()
        
        logger.info(
            "ReplicaManager initialized",
            max_lag=max_replica_lag,
            max_lag_time_ms=max_replica_lag_time_ms,
        )
    
    async def create_partition_replica_set(
        self,
        topic: str,
        partition: int,
        leader_id: int,
        replica_ids: List[int],
    ) -> PartitionReplicaSet:
        """
        Create replica set for partition.
        
        Args:
            topic: Topic name
            partition: Partition number
            leader_id: Leader broker ID
            replica_ids: List of all replica broker IDs
        
        Returns:
            Partition replica set
        """
        async with self._lock:
            key = (topic, partition)
            
            replica_set = PartitionReplicaSet(
                topic=topic,
                partition=partition,
                leader_id=leader_id,
            )
            
            # Add all replicas
            for broker_id in replica_ids:
                replica = ReplicaInfo(
                    broker_id=broker_id,
                    topic=topic,
                    partition=partition,
                )
                
                if broker_id == leader_id:
                    replica.state = ReplicaState.IN_SYNC
                
                replica_set.add_replica(replica)
            
            self._partitions[key] = replica_set
            
            logger.info(
                "Created partition replica set",
                topic=topic,
                partition=partition,
                leader_id=leader_id,
                replicas=replica_ids,
            )
            
            return replica_set
    
    async def update_follower_offset(
        self,
        topic: str,
        partition: int,
        broker_id: int,
        log_end_offset: int,
    ) -> None:
        """
        Update follower's log end offset after fetch.
        
        Args:
            topic: Topic name
            partition: Partition number
            broker_id: Follower broker ID
            log_end_offset: New log end offset
        """
        async with self._lock:
            key = (topic, partition)
            
            if key not in self._partitions:
                return
            
            replica_set = self._partitions[key]
            replica_set.update_replica_offset(broker_id, log_end_offset)
            
            # Update ISR based on new offset
            leader_replica = replica_set.replicas.get(replica_set.leader_id)
            if leader_replica:
                replica_set.update_isr(
                    leader_replica.log_end_offset,
                    self._max_lag,
                )
                
                # Recalculate HWM
                replica_set.calculate_high_watermark()
    
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
        async with self._lock:
            key = (topic, partition)
            
            if key not in self._partitions:
                return 0
            
            return self._partitions[key].high_watermark
    
    async def get_isr(
        self,
        topic: str,
        partition: int,
    ) -> Set[int]:
        """
        Get In-Sync Replicas for partition.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            Set of ISR broker IDs
        """
        async with self._lock:
            key = (topic, partition)
            
            if key not in self._partitions:
                return set()
            
            return self._partitions[key].isr.copy()
    
    async def check_replica_health(self) -> None:
        """Check health of all replicas and update ISR."""
        async with self._lock:
            current_time = int(time.time() * 1000)
            
            for replica_set in self._partitions.values():
                # Remove lagging replicas
                removed = replica_set.remove_lagging_replicas(
                    current_time,
                    self._max_lag_time_ms,
                )
                
                # Recalculate HWM if ISR changed
                if removed:
                    replica_set.calculate_high_watermark()
