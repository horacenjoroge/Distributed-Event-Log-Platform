"""
Consumer group metadata and state management.

Manages the state of consumer groups including members, assignments, and offsets.
"""

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set

from distributedlog.consumer.offset import TopicPartition
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class GroupState(Enum):
    """Consumer group states."""
    
    EMPTY = "Empty"                      # No members
    PREPARING_REBALANCE = "PreparingRebalance"  # Waiting for members
    COMPLETING_REBALANCE = "CompletingRebalance"  # Waiting for sync
    STABLE = "Stable"                    # All members synced
    DEAD = "Dead"                        # Group deleted


@dataclass
class MemberMetadata:
    """
    Metadata about a consumer group member.
    
    Attributes:
        member_id: Unique member identifier
        client_id: Client identifier
        host: Member host
        session_timeout_ms: Session timeout
        rebalance_timeout_ms: Rebalance timeout
        subscription: Subscribed topics
        assignment: Assigned partitions
        last_heartbeat_ms: Last heartbeat timestamp
    """
    member_id: str
    client_id: str
    host: str = "localhost"
    session_timeout_ms: int = 30000
    rebalance_timeout_ms: int = 60000
    subscription: Set[str] = field(default_factory=set)
    assignment: Set[TopicPartition] = field(default_factory=set)
    last_heartbeat_ms: int = 0
    
    def __post_init__(self):
        if self.last_heartbeat_ms == 0:
            self.last_heartbeat_ms = int(time.time() * 1000)
    
    def is_expired(self, current_time_ms: int) -> bool:
        """
        Check if member has expired (no heartbeat).
        
        Args:
            current_time_ms: Current time in milliseconds
        
        Returns:
            True if expired
        """
        return (current_time_ms - self.last_heartbeat_ms) > self.session_timeout_ms
    
    def update_heartbeat(self) -> None:
        """Update last heartbeat timestamp."""
        self.last_heartbeat_ms = int(time.time() * 1000)


@dataclass
class ConsumerGroupMetadata:
    """
    Complete metadata for a consumer group.
    
    Attributes:
        group_id: Group identifier
        generation_id: Current generation (increments on rebalance)
        protocol_type: Protocol type (consumer)
        protocol_name: Assignment protocol (range, roundrobin, sticky)
        leader_id: Current leader member ID
        state: Current group state
        members: Group members
    """
    group_id: str
    generation_id: int = 0
    protocol_type: str = "consumer"
    protocol_name: str = "range"
    leader_id: Optional[str] = None
    state: GroupState = GroupState.EMPTY
    members: Dict[str, MemberMetadata] = field(default_factory=dict)
    
    def get_all_subscriptions(self) -> Set[str]:
        """
        Get all topics subscribed by group members.
        
        Returns:
            Set of topic names
        """
        all_topics = set()
        for member in self.members.values():
            all_topics.update(member.subscription)
        return all_topics
    
    def get_all_partitions(self) -> Set[TopicPartition]:
        """
        Get all partitions assigned to group members.
        
        Returns:
            Set of topic-partitions
        """
        all_partitions = set()
        for member in self.members.values():
            all_partitions.update(member.assignment)
        return all_partitions
    
    def clear_assignments(self) -> None:
        """Clear all member assignments."""
        for member in self.members.values():
            member.assignment.clear()
    
    def remove_expired_members(self, current_time_ms: int) -> List[str]:
        """
        Remove expired members from group.
        
        Args:
            current_time_ms: Current time in milliseconds
        
        Returns:
            List of removed member IDs
        """
        expired = []
        
        for member_id, member in list(self.members.items()):
            if member.is_expired(current_time_ms):
                expired.append(member_id)
                del self.members[member_id]
                
                logger.info(
                    "Removed expired member",
                    group_id=self.group_id,
                    member_id=member_id,
                )
        
        return expired
    
    def next_generation(self) -> None:
        """Increment generation ID for new rebalance."""
        self.generation_id += 1
        logger.info(
            "Advanced to next generation",
            group_id=self.group_id,
            generation_id=self.generation_id,
        )
    
    def is_leader(self, member_id: str) -> bool:
        """
        Check if member is group leader.
        
        Args:
            member_id: Member ID
        
        Returns:
            True if member is leader
        """
        return self.leader_id == member_id


@dataclass
class GroupOffsets:
    """
    Offset storage for consumer group.
    
    Tracks committed offsets per partition for each group.
    """
    group_id: str
    offsets: Dict[TopicPartition, int] = field(default_factory=dict)
    
    def commit(self, topic_partition: TopicPartition, offset: int) -> None:
        """
        Commit offset for partition.
        
        Args:
            topic_partition: Topic-partition
            offset: Offset to commit
        """
        self.offsets[topic_partition] = offset
        
        logger.debug(
            "Committed group offset",
            group_id=self.group_id,
            topic=topic_partition.topic,
            partition=topic_partition.partition,
            offset=offset,
        )
    
    def get_offset(self, topic_partition: TopicPartition) -> Optional[int]:
        """
        Get committed offset for partition.
        
        Args:
            topic_partition: Topic-partition
        
        Returns:
            Committed offset or None
        """
        return self.offsets.get(topic_partition)
    
    def get_lag(
        self,
        topic_partition: TopicPartition,
        log_end_offset: int,
    ) -> Optional[int]:
        """
        Calculate consumer lag.
        
        Args:
            topic_partition: Topic-partition
            log_end_offset: End offset of log
        
        Returns:
            Lag (log end - committed) or None
        """
        committed = self.get_offset(topic_partition)
        if committed is None:
            return None
        
        return log_end_offset - committed
