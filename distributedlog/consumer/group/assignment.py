"""
Partition assignment strategies for consumer groups.

Implements different strategies for assigning partitions to consumers.
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Set

from distributedlog.consumer.offset import TopicPartition
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class AssignmentStrategy(ABC):
    """Abstract base class for assignment strategies."""
    
    @abstractmethod
    def assign(
        self,
        members: List[str],
        partitions_per_topic: Dict[str, int],
        subscriptions: Dict[str, Set[str]],
    ) -> Dict[str, Set[TopicPartition]]:
        """
        Assign partitions to members.
        
        Args:
            members: List of member IDs
            partitions_per_topic: Number of partitions per topic
            subscriptions: Member subscriptions (member_id -> topics)
        
        Returns:
            Assignment map (member_id -> assigned partitions)
        """
        pass


class RangeAssignmentStrategy(AssignmentStrategy):
    """
    Range assignment strategy.
    
    Divides partitions into ranges and assigns to consumers.
    Example: 10 partitions, 3 consumers
      Consumer 1: partitions 0-3
      Consumer 2: partitions 4-6  
      Consumer 3: partitions 7-9
    """
    
    def assign(
        self,
        members: List[str],
        partitions_per_topic: Dict[str, int],
        subscriptions: Dict[str, Set[str]],
    ) -> Dict[str, Set[TopicPartition]]:
        """Assign partitions using range strategy."""
        assignment = {member: set() for member in members}
        
        all_topics = set()
        for topics in subscriptions.values():
            all_topics.update(topics)
        
        sorted_members = sorted(members)
        
        for topic in sorted(all_topics):
            num_partitions = partitions_per_topic.get(topic, 0)
            if num_partitions == 0:
                continue
            
            subscribed_members = [
                m for m in sorted_members
                if topic in subscriptions.get(m, set())
            ]
            
            if not subscribed_members:
                continue
            
            partitions_per_consumer = num_partitions // len(subscribed_members)
            extra_partitions = num_partitions % len(subscribed_members)
            
            partition_idx = 0
            for i, member in enumerate(subscribed_members):
                num_assigned = partitions_per_consumer
                if i < extra_partitions:
                    num_assigned += 1
                
                for _ in range(num_assigned):
                    assignment[member].add(TopicPartition(topic, partition_idx))
                    partition_idx += 1
        
        logger.info(
            "Range assignment complete",
            members=len(members),
            assignments={m: len(p) for m, p in assignment.items()},
        )
        
        return assignment


class RoundRobinAssignmentStrategy(AssignmentStrategy):
    """
    Round-robin assignment strategy.
    
    Alternates partitions across consumers.
    Example: 10 partitions, 3 consumers
      Consumer 1: partitions 0, 3, 6, 9
      Consumer 2: partitions 1, 4, 7
      Consumer 3: partitions 2, 5, 8
    """
    
    def assign(
        self,
        members: List[str],
        partitions_per_topic: Dict[str, int],
        subscriptions: Dict[str, Set[str]],
    ) -> Dict[str, Set[TopicPartition]]:
        """Assign partitions using round-robin strategy."""
        assignment = {member: set() for member in members}
        
        all_topics = set()
        for topics in subscriptions.values():
            all_topics.update(topics)
        
        all_partitions = []
        for topic in sorted(all_topics):
            num_partitions = partitions_per_topic.get(topic, 0)
            for partition in range(num_partitions):
                all_partitions.append(TopicPartition(topic, partition))
        
        sorted_members = sorted(members)
        
        for i, partition in enumerate(all_partitions):
            member = sorted_members[i % len(sorted_members)]
            
            if partition.topic in subscriptions.get(member, set()):
                assignment[member].add(partition)
        
        logger.info(
            "Round-robin assignment complete",
            members=len(members),
            assignments={m: len(p) for m, p in assignment.items()},
        )
        
        return assignment


class StickyAssignmentStrategy(AssignmentStrategy):
    """
    Sticky assignment strategy.
    
    Minimizes partition movement during rebalance.
    Tries to keep existing assignments when possible.
    """
    
    def __init__(self):
        """Initialize sticky strategy."""
        self._previous_assignment: Dict[str, Set[TopicPartition]] = {}
    
    def assign(
        self,
        members: List[str],
        partitions_per_topic: Dict[str, int],
        subscriptions: Dict[str, Set[str]],
    ) -> Dict[str, Set[TopicPartition]]:
        """Assign partitions using sticky strategy."""
        assignment = {member: set() for member in members}
        
        all_topics = set()
        for topics in subscriptions.values():
            all_topics.update(topics)
        
        all_partitions = []
        for topic in sorted(all_topics):
            num_partitions = partitions_per_topic.get(topic, 0)
            for partition in range(num_partitions):
                all_partitions.append(TopicPartition(topic, partition))
        
        assigned_partitions = set()
        
        for member in members:
            if member in self._previous_assignment:
                for partition in self._previous_assignment[member]:
                    if (partition in all_partitions and
                        partition.topic in subscriptions.get(member, set()) and
                        partition not in assigned_partitions):
                        assignment[member].add(partition)
                        assigned_partitions.add(partition)
        
        unassigned_partitions = [
            p for p in all_partitions
            if p not in assigned_partitions
        ]
        
        sorted_members = sorted(members)
        member_idx = 0
        
        for partition in unassigned_partitions:
            while True:
                member = sorted_members[member_idx % len(sorted_members)]
                member_idx += 1
                
                if partition.topic in subscriptions.get(member, set()):
                    assignment[member].add(partition)
                    break
        
        self._previous_assignment = {
            m: p.copy() for m, p in assignment.items()
        }
        
        logger.info(
            "Sticky assignment complete",
            members=len(members),
            assignments={m: len(p) for m, p in assignment.items()},
        )
        
        return assignment


def create_assignment_strategy(name: str) -> AssignmentStrategy:
    """
    Create assignment strategy by name.
    
    Args:
        name: Strategy name (range, roundrobin, sticky)
    
    Returns:
        Assignment strategy instance
    """
    strategies = {
        "range": RangeAssignmentStrategy,
        "roundrobin": RoundRobinAssignmentStrategy,
        "sticky": StickyAssignmentStrategy,
    }
    
    strategy_class = strategies.get(name.lower())
    if not strategy_class:
        raise ValueError(f"Unknown assignment strategy: {name}")
    
    return strategy_class()
