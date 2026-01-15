"""
Consumer client with consumer group support.

Extends the basic consumer with group coordination, heartbeating, and rebalancing.
"""

import asyncio
import threading
import time
from typing import List, Optional, Set, Union

from distributedlog.consumer.consumer import Consumer, ConsumerConfig
from distributedlog.consumer.group.coordinator import GroupCoordinator
from distributedlog.consumer.offset import TopicPartition
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class GroupConsumerConfig(ConsumerConfig):
    """
    Extended configuration for group consumer.
    
    Attributes:
        session_timeout_ms: Session timeout
        rebalance_timeout_ms: Rebalance timeout
        heartbeat_interval_ms: Heartbeat interval
        partition_assignment_strategy: Assignment strategy (range, roundrobin, sticky)
    """
    session_timeout_ms: int = 30000
    rebalance_timeout_ms: int = 60000
    heartbeat_interval_ms: int = 3000
    partition_assignment_strategy: str = "range"


class GroupConsumer(Consumer):
    """
    Consumer with consumer group support.
    
    Features:
    - Automatic group membership
    - Heartbeat protocol
    - Rebalancing on member changes
    - Coordinated partition assignment
    
    Example:
        consumer = GroupConsumer(
            bootstrap_servers=['localhost:9092'],
            group_id='my-group',
            auto_commit=True,
        )
        
        consumer.subscribe(['my-topic'])
        
        while True:
            messages = consumer.poll(timeout_ms=1000)
            for message in messages:
                print(f"Offset: {message.offset}, Value: {message.value}")
        
        consumer.close()
    """
    
    def __init__(
        self,
        bootstrap_servers: Union[str, List[str], None] = None,
        group_id: Optional[str] = None,
        auto_commit: bool = True,
        config: Optional[GroupConsumerConfig] = None,
        coordinator: Optional[GroupCoordinator] = None,
        **kwargs,
    ):
        """
        Initialize group consumer.
        
        Args:
            bootstrap_servers: Broker list
            group_id: Consumer group ID
            auto_commit: Enable auto-commit
            config: Consumer configuration
            coordinator: Group coordinator (for testing)
            **kwargs: Additional config overrides
        """
        self.group_config = config or GroupConsumerConfig()
        
        if bootstrap_servers is not None:
            self.group_config.bootstrap_servers = bootstrap_servers
        
        if group_id is not None:
            self.group_config.group_id = group_id
        
        self.group_config.auto_commit = auto_commit
        
        for key, value in kwargs.items():
            if hasattr(self.group_config, key):
                setattr(self.group_config, key, value)
        
        super().__init__(config=self.group_config)
        
        self._coordinator = coordinator
        
        self._member_id: Optional[str] = None
        self._generation_id: int = -1
        self._is_leader: bool = False
        
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._heartbeat_running = False
        
        self._rebalance_in_progress = False
        
        logger.info(
            "GroupConsumer initialized",
            group_id=self.config.group_id,
            strategy=self.group_config.partition_assignment_strategy,
        )
    
    def subscribe(self, topics: List[str]) -> None:
        """
        Subscribe to topics with group coordination.
        
        Args:
            topics: List of topic names
        """
        self._subscriptions = set(topics)
        
        if self._coordinator:
            asyncio.run(self._join_group())
            self._start_heartbeat()
        else:
            super().subscribe(topics)
        
        logger.info(
            "Subscribed to topics with group",
            topics=topics,
            group_id=self.config.group_id,
            member_id=self._member_id,
        )
    
    async def _join_group(self) -> None:
        """Join consumer group."""
        if not self._coordinator:
            return
        
        self._rebalance_in_progress = True
        
        try:
            generation_id, member_id, is_leader = await self._coordinator.join_group(
                group_id=self.config.group_id,
                member_id=self._member_id or "",
                client_id=f"consumer-{id(self)}",
                session_timeout_ms=self.group_config.session_timeout_ms,
                rebalance_timeout_ms=self.group_config.rebalance_timeout_ms,
                protocol_name=self.group_config.partition_assignment_strategy,
                topics=self._subscriptions,
            )
            
            self._member_id = member_id
            self._generation_id = generation_id
            self._is_leader = is_leader
            
            logger.info(
                "Joined consumer group",
                group_id=self.config.group_id,
                member_id=self._member_id,
                generation_id=self._generation_id,
                is_leader=self._is_leader,
            )
            
            await self._sync_group()
            
        finally:
            self._rebalance_in_progress = False
    
    async def _sync_group(self) -> None:
        """Sync with group to get partition assignment."""
        if not self._coordinator or not self._member_id:
            return
        
        assignment = await self._coordinator.sync_group(
            group_id=self.config.group_id,
            member_id=self._member_id,
            generation_id=self._generation_id,
        )
        
        self._update_group_assignment(assignment)
        
        logger.info(
            "Synced with group",
            group_id=self.config.group_id,
            member_id=self._member_id,
            partitions=len(assignment),
        )
    
    def _update_group_assignment(self, partitions: Set[TopicPartition]) -> None:
        """
        Update consumer assignment from coordinator.
        
        Args:
            partitions: Assigned partitions
        """
        removed = self._assignment - partitions
        added = partitions - self._assignment
        
        if removed:
            self._fetcher.unassign_partitions(removed)
            self._offset_manager.reset_positions(removed)
        
        if added:
            offsets = {}
            for tp in added:
                committed = self._offset_manager.committed(tp)
                if committed:
                    offsets[tp] = committed.offset
                else:
                    offsets[tp] = 0
                
                self._offset_manager.update_position(tp, offsets[tp])
            
            self._fetcher.assign_partitions(added, offsets)
        
        self._assignment = partitions
        
        logger.info(
            "Updated group assignment",
            added=len(added),
            removed=len(removed),
            total=len(self._assignment),
        )
    
    def _start_heartbeat(self) -> None:
        """Start heartbeat thread."""
        if self._heartbeat_running:
            return
        
        self._heartbeat_running = True
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
        )
        self._heartbeat_thread.start()
        
        logger.info("Heartbeat thread started", member_id=self._member_id)
    
    def _heartbeat_loop(self) -> None:
        """Heartbeat loop (runs in separate thread)."""
        interval_sec = self.group_config.heartbeat_interval_ms / 1000.0
        
        while self._heartbeat_running and not self._closed:
            try:
                if self._coordinator and self._member_id:
                    result = asyncio.run(
                        self._coordinator.heartbeat(
                            group_id=self.config.group_id,
                            member_id=self._member_id,
                            generation_id=self._generation_id,
                        )
                    )
                    
                    if not result:
                        logger.warning(
                            "Heartbeat rejected, rebalance needed",
                            member_id=self._member_id,
                        )
                        
                        if not self._rebalance_in_progress:
                            asyncio.run(self._join_group())
                    
                    logger.debug(
                        "Heartbeat sent",
                        member_id=self._member_id,
                        generation_id=self._generation_id,
                    )
            
            except Exception as e:
                logger.error(
                    "Heartbeat error",
                    member_id=self._member_id,
                    error=str(e),
                )
            
            time.sleep(interval_sec)
    
    def _stop_heartbeat(self) -> None:
        """Stop heartbeat thread."""
        if not self._heartbeat_running:
            return
        
        self._heartbeat_running = False
        
        if self._heartbeat_thread:
            self._heartbeat_thread.join(timeout=5.0)
        
        logger.info("Heartbeat thread stopped", member_id=self._member_id)
    
    async def _leave_group(self) -> None:
        """Leave consumer group."""
        if not self._coordinator or not self._member_id:
            return
        
        await self._coordinator.leave_group(
            group_id=self.config.group_id,
            member_id=self._member_id,
        )
        
        logger.info(
            "Left consumer group",
            group_id=self.config.group_id,
            member_id=self._member_id,
        )
    
    def close(self) -> None:
        """Close consumer and leave group."""
        if self._closed:
            return
        
        logger.info(
            "Closing group consumer",
            group_id=self.config.group_id,
            member_id=self._member_id,
        )
        
        self._stop_heartbeat()
        
        if self._coordinator:
            asyncio.run(self._leave_group())
        
        super().close()
        
        logger.info("Group consumer closed", member_id=self._member_id)
    
    def metrics(self) -> dict:
        """
        Get consumer metrics.
        
        Returns:
            Dictionary with metrics
        """
        base_metrics = super().metrics()
        
        base_metrics.update({
            "member_id": self._member_id,
            "generation_id": self._generation_id,
            "is_leader": self._is_leader,
            "heartbeat_running": self._heartbeat_running,
            "rebalance_in_progress": self._rebalance_in_progress,
        })
        
        return base_metrics
