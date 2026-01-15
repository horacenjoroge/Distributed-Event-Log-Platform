"""
Consumer group coordinator.

Manages consumer group lifecycle, membership, and rebalancing.
"""

import asyncio
import time
from typing import Dict, List, Optional, Set

from distributedlog.consumer.group.assignment import create_assignment_strategy
from distributedlog.consumer.group.metadata import (
    ConsumerGroupMetadata,
    GroupOffsets,
    GroupState,
    MemberMetadata,
)
from distributedlog.consumer.offset import TopicPartition
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class GroupCoordinator:
    """
    Coordinates consumer group operations.
    
    Responsibilities:
    - Manage group membership
    - Handle join/sync/heartbeat requests
    - Coordinate rebalancing
    - Track committed offsets
    - Monitor member health
    """
    
    def __init__(self, partitions_per_topic: Dict[str, int]):
        """
        Initialize group coordinator.
        
        Args:
            partitions_per_topic: Number of partitions per topic
        """
        self._groups: Dict[str, ConsumerGroupMetadata] = {}
        self._offsets: Dict[str, GroupOffsets] = {}
        self._partitions_per_topic = partitions_per_topic
        
        self._rebalance_delay_ms = 3000
        self._lock = asyncio.Lock()
        
        logger.info("GroupCoordinator initialized")
    
    async def join_group(
        self,
        group_id: str,
        member_id: str,
        client_id: str,
        session_timeout_ms: int,
        rebalance_timeout_ms: int,
        protocol_name: str,
        topics: Set[str],
    ) -> tuple[int, str, bool]:
        """
        Handle join group request from consumer.
        
        Args:
            group_id: Consumer group ID
            member_id: Member ID (empty for new members)
            client_id: Client ID
            session_timeout_ms: Session timeout
            rebalance_timeout_ms: Rebalance timeout
            protocol_name: Assignment protocol
            topics: Subscribed topics
        
        Returns:
            Tuple of (generation_id, assigned_member_id, is_leader)
        """
        async with self._lock:
            group = self._get_or_create_group(group_id, protocol_name)
            
            if not member_id:
                member_id = self._generate_member_id(client_id)
            
            member = MemberMetadata(
                member_id=member_id,
                client_id=client_id,
                session_timeout_ms=session_timeout_ms,
                rebalance_timeout_ms=rebalance_timeout_ms,
                subscription=topics,
            )
            
            existing_member = group.members.get(member_id)
            
            if group.state == GroupState.STABLE and existing_member:
                member.assignment = existing_member.assignment.copy()
                group.members[member_id] = member
                
                logger.info(
                    "Member rejoined stable group",
                    group_id=group_id,
                    member_id=member_id,
                )
                
                return group.generation_id, member_id, group.is_leader(member_id)
            
            group.members[member_id] = member
            
            if group.state != GroupState.PREPARING_REBALANCE:
                await self._prepare_rebalance(group)
            
            logger.info(
                "Member joined group",
                group_id=group_id,
                member_id=member_id,
                member_count=len(group.members),
            )
            
            return group.generation_id, member_id, group.is_leader(member_id)
    
    async def sync_group(
        self,
        group_id: str,
        member_id: str,
        generation_id: int,
    ) -> Set[TopicPartition]:
        """
        Handle sync group request from consumer.
        
        Args:
            group_id: Consumer group ID
            member_id: Member ID
            generation_id: Expected generation ID
        
        Returns:
            Assigned partitions for member
        """
        async with self._lock:
            group = self._groups.get(group_id)
            
            if not group:
                raise ValueError(f"Unknown group: {group_id}")
            
            if generation_id != group.generation_id:
                raise ValueError(
                    f"Generation mismatch: {generation_id} != {group.generation_id}"
                )
            
            member = group.members.get(member_id)
            if not member:
                raise ValueError(f"Unknown member: {member_id}")
            
            if group.state == GroupState.STABLE:
                logger.debug(
                    "Member synced with stable group",
                    group_id=group_id,
                    member_id=member_id,
                )
                return member.assignment
            
            if group.state == GroupState.COMPLETING_REBALANCE:
                await self._wait_for_rebalance_complete(group)
                return member.assignment
            
            raise ValueError(f"Cannot sync in state: {group.state}")
    
    async def heartbeat(
        self,
        group_id: str,
        member_id: str,
        generation_id: int,
    ) -> bool:
        """
        Handle heartbeat from consumer.
        
        Args:
            group_id: Consumer group ID
            member_id: Member ID
            generation_id: Current generation ID
        
        Returns:
            True if heartbeat accepted
        """
        async with self._lock:
            group = self._groups.get(group_id)
            
            if not group:
                raise ValueError(f"Unknown group: {group_id}")
            
            if generation_id != group.generation_id:
                logger.warning(
                    "Heartbeat generation mismatch",
                    group_id=group_id,
                    member_id=member_id,
                    expected=group.generation_id,
                    received=generation_id,
                )
                return False
            
            member = group.members.get(member_id)
            if not member:
                raise ValueError(f"Unknown member: {member_id}")
            
            member.update_heartbeat()
            
            logger.debug(
                "Heartbeat received",
                group_id=group_id,
                member_id=member_id,
            )
            
            return True
    
    async def leave_group(
        self,
        group_id: str,
        member_id: str,
    ) -> None:
        """
        Handle leave group request from consumer.
        
        Args:
            group_id: Consumer group ID
            member_id: Member ID
        """
        async with self._lock:
            group = self._groups.get(group_id)
            
            if not group:
                return
            
            if member_id in group.members:
                del group.members[member_id]
                
                logger.info(
                    "Member left group",
                    group_id=group_id,
                    member_id=member_id,
                    remaining=len(group.members),
                )
                
                if not group.members:
                    group.state = GroupState.EMPTY
                elif group.state == GroupState.STABLE:
                    await self._prepare_rebalance(group)
    
    async def commit_offset(
        self,
        group_id: str,
        topic_partition: TopicPartition,
        offset: int,
    ) -> None:
        """
        Commit offset for group and partition.
        
        Args:
            group_id: Consumer group ID
            topic_partition: Topic-partition
            offset: Offset to commit
        """
        async with self._lock:
            if group_id not in self._offsets:
                self._offsets[group_id] = GroupOffsets(group_id)
            
            self._offsets[group_id].commit(topic_partition, offset)
    
    async def fetch_offset(
        self,
        group_id: str,
        topic_partition: TopicPartition,
    ) -> Optional[int]:
        """
        Fetch committed offset for group and partition.
        
        Args:
            group_id: Consumer group ID
            topic_partition: Topic-partition
        
        Returns:
            Committed offset or None
        """
        async with self._lock:
            if group_id not in self._offsets:
                return None
            
            return self._offsets[group_id].get_offset(topic_partition)
    
    async def get_lag(
        self,
        group_id: str,
        topic_partition: TopicPartition,
        log_end_offset: int,
    ) -> Optional[int]:
        """
        Calculate consumer lag.
        
        Args:
            group_id: Consumer group ID
            topic_partition: Topic-partition
            log_end_offset: End offset of log
        
        Returns:
            Lag or None
        """
        async with self._lock:
            if group_id not in self._offsets:
                return None
            
            return self._offsets[group_id].get_lag(topic_partition, log_end_offset)
    
    async def _prepare_rebalance(self, group: ConsumerGroupMetadata) -> None:
        """
        Prepare group for rebalancing.
        
        Args:
            group: Consumer group
        """
        group.state = GroupState.PREPARING_REBALANCE
        group.clear_assignments()
        
        logger.info(
            "Preparing rebalance",
            group_id=group.group_id,
            members=len(group.members),
        )
        
        await asyncio.sleep(self._rebalance_delay_ms / 1000)
        
        await self._complete_rebalance(group)
    
    async def _complete_rebalance(self, group: ConsumerGroupMetadata) -> None:
        """
        Complete rebalancing by assigning partitions.
        
        Args:
            group: Consumer group
        """
        group.state = GroupState.COMPLETING_REBALANCE
        group.next_generation()
        
        strategy = create_assignment_strategy(group.protocol_name)
        
        subscriptions = {
            member_id: member.subscription
            for member_id, member in group.members.items()
        }
        
        assignments = strategy.assign(
            list(group.members.keys()),
            self._partitions_per_topic,
            subscriptions,
        )
        
        for member_id, partitions in assignments.items():
            if member_id in group.members:
                group.members[member_id].assignment = partitions
        
        if group.members:
            group.leader_id = sorted(group.members.keys())[0]
        
        group.state = GroupState.STABLE
        
        logger.info(
            "Rebalance complete",
            group_id=group.group_id,
            generation_id=group.generation_id,
            leader_id=group.leader_id,
            assignments={m: len(p) for m, p in assignments.items()},
        )
    
    async def _wait_for_rebalance_complete(
        self,
        group: ConsumerGroupMetadata,
    ) -> None:
        """
        Wait for rebalance to complete.
        
        Args:
            group: Consumer group
        """
        max_wait = 30
        waited = 0
        
        while group.state != GroupState.STABLE and waited < max_wait:
            await asyncio.sleep(0.1)
            waited += 0.1
        
        if group.state != GroupState.STABLE:
            raise TimeoutError("Rebalance did not complete in time")
    
    def _get_or_create_group(
        self,
        group_id: str,
        protocol_name: str,
    ) -> ConsumerGroupMetadata:
        """
        Get or create consumer group.
        
        Args:
            group_id: Group ID
            protocol_name: Assignment protocol
        
        Returns:
            Consumer group metadata
        """
        if group_id not in self._groups:
            self._groups[group_id] = ConsumerGroupMetadata(
                group_id=group_id,
                protocol_name=protocol_name,
            )
            
            logger.info(
                "Created consumer group",
                group_id=group_id,
                protocol=protocol_name,
            )
        
        return self._groups[group_id]
    
    def _generate_member_id(self, client_id: str) -> str:
        """
        Generate unique member ID.
        
        Args:
            client_id: Client ID
        
        Returns:
            Member ID
        """
        timestamp = int(time.time() * 1000)
        return f"{client_id}-{timestamp}"
    
    async def check_expired_members(self) -> None:
        """Check and remove expired members from all groups."""
        async with self._lock:
            current_time_ms = int(time.time() * 1000)
            
            for group in self._groups.values():
                if group.state == GroupState.EMPTY:
                    continue
                
                expired = group.remove_expired_members(current_time_ms)
                
                if expired and group.state == GroupState.STABLE:
                    logger.warning(
                        "Expired members triggered rebalance",
                        group_id=group.group_id,
                        expired_members=expired,
                    )
                    await self._prepare_rebalance(group)
