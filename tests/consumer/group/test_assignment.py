"""Tests for partition assignment strategies."""

import pytest

from distributedlog.consumer.group.assignment import (
    RangeAssignmentStrategy,
    RoundRobinAssignmentStrategy,
    StickyAssignmentStrategy,
    create_assignment_strategy,
)
from distributedlog.consumer.offset import TopicPartition


class TestRangeAssignment:
    """Test RangeAssignmentStrategy."""
    
    def test_single_topic_even_distribution(self):
        """Test even distribution of partitions."""
        strategy = RangeAssignmentStrategy()
        
        members = ["consumer-1", "consumer-2", "consumer-3"]
        partitions_per_topic = {"topic1": 6}
        subscriptions = {
            "consumer-1": {"topic1"},
            "consumer-2": {"topic1"},
            "consumer-3": {"topic1"},
        }
        
        assignment = strategy.assign(members, partitions_per_topic, subscriptions)
        
        assert len(assignment["consumer-1"]) == 2
        assert len(assignment["consumer-2"]) == 2
        assert len(assignment["consumer-3"]) == 2
    
    def test_single_topic_uneven_distribution(self):
        """Test uneven distribution of partitions."""
        strategy = RangeAssignmentStrategy()
        
        members = ["consumer-1", "consumer-2", "consumer-3"]
        partitions_per_topic = {"topic1": 7}
        subscriptions = {
            "consumer-1": {"topic1"},
            "consumer-2": {"topic1"},
            "consumer-3": {"topic1"},
        }
        
        assignment = strategy.assign(members, partitions_per_topic, subscriptions)
        
        assert len(assignment["consumer-1"]) == 3
        assert len(assignment["consumer-2"]) == 2
        assert len(assignment["consumer-3"]) == 2
    
    def test_multiple_topics(self):
        """Test assignment with multiple topics."""
        strategy = RangeAssignmentStrategy()
        
        members = ["consumer-1", "consumer-2"]
        partitions_per_topic = {"topic1": 4, "topic2": 4}
        subscriptions = {
            "consumer-1": {"topic1", "topic2"},
            "consumer-2": {"topic1", "topic2"},
        }
        
        assignment = strategy.assign(members, partitions_per_topic, subscriptions)
        
        assert len(assignment["consumer-1"]) == 4
        assert len(assignment["consumer-2"]) == 4
    
    def test_partial_subscription(self):
        """Test when not all consumers subscribe to all topics."""
        strategy = RangeAssignmentStrategy()
        
        members = ["consumer-1", "consumer-2"]
        partitions_per_topic = {"topic1": 4, "topic2": 4}
        subscriptions = {
            "consumer-1": {"topic1"},
            "consumer-2": {"topic2"},
        }
        
        assignment = strategy.assign(members, partitions_per_topic, subscriptions)
        
        assert len(assignment["consumer-1"]) == 4
        assert len(assignment["consumer-2"]) == 4
        
        for tp in assignment["consumer-1"]:
            assert tp.topic == "topic1"
        
        for tp in assignment["consumer-2"]:
            assert tp.topic == "topic2"


class TestRoundRobinAssignment:
    """Test RoundRobinAssignmentStrategy."""
    
    def test_even_distribution(self):
        """Test round-robin distribution."""
        strategy = RoundRobinAssignmentStrategy()
        
        members = ["consumer-1", "consumer-2", "consumer-3"]
        partitions_per_topic = {"topic1": 6}
        subscriptions = {
            "consumer-1": {"topic1"},
            "consumer-2": {"topic1"},
            "consumer-3": {"topic1"},
        }
        
        assignment = strategy.assign(members, partitions_per_topic, subscriptions)
        
        assert len(assignment["consumer-1"]) == 2
        assert len(assignment["consumer-2"]) == 2
        assert len(assignment["consumer-3"]) == 2
    
    def test_alternating_assignment(self):
        """Test alternating partition assignment."""
        strategy = RoundRobinAssignmentStrategy()
        
        members = ["consumer-1", "consumer-2"]
        partitions_per_topic = {"topic1": 4}
        subscriptions = {
            "consumer-1": {"topic1"},
            "consumer-2": {"topic1"},
        }
        
        assignment = strategy.assign(members, partitions_per_topic, subscriptions)
        
        assert len(assignment["consumer-1"]) == 2
        assert len(assignment["consumer-2"]) == 2
        
        c1_partitions = sorted([tp.partition for tp in assignment["consumer-1"]])
        c2_partitions = sorted([tp.partition for tp in assignment["consumer-2"]])
        
        assert c1_partitions == [0, 2]
        assert c2_partitions == [1, 3]
    
    def test_multiple_topics(self):
        """Test round-robin with multiple topics."""
        strategy = RoundRobinAssignmentStrategy()
        
        members = ["consumer-1", "consumer-2"]
        partitions_per_topic = {"topic1": 2, "topic2": 2}
        subscriptions = {
            "consumer-1": {"topic1", "topic2"},
            "consumer-2": {"topic1", "topic2"},
        }
        
        assignment = strategy.assign(members, partitions_per_topic, subscriptions)
        
        assert len(assignment["consumer-1"]) == 2
        assert len(assignment["consumer-2"]) == 2


class TestStickyAssignment:
    """Test StickyAssignmentStrategy."""
    
    def test_initial_assignment(self):
        """Test initial assignment (no previous state)."""
        strategy = StickyAssignmentStrategy()
        
        members = ["consumer-1", "consumer-2"]
        partitions_per_topic = {"topic1": 4}
        subscriptions = {
            "consumer-1": {"topic1"},
            "consumer-2": {"topic1"},
        }
        
        assignment = strategy.assign(members, partitions_per_topic, subscriptions)
        
        assert len(assignment["consumer-1"]) == 2
        assert len(assignment["consumer-2"]) == 2
    
    def test_sticky_on_rebalance(self):
        """Test that assignments stick on rebalance."""
        strategy = StickyAssignmentStrategy()
        
        members = ["consumer-1", "consumer-2"]
        partitions_per_topic = {"topic1": 4}
        subscriptions = {
            "consumer-1": {"topic1"},
            "consumer-2": {"topic1"},
        }
        
        assignment1 = strategy.assign(members, partitions_per_topic, subscriptions)
        
        c1_first = assignment1["consumer-1"].copy()
        c2_first = assignment1["consumer-2"].copy()
        
        assignment2 = strategy.assign(members, partitions_per_topic, subscriptions)
        
        assert assignment2["consumer-1"] == c1_first
        assert assignment2["consumer-2"] == c2_first
    
    def test_member_added(self):
        """Test adding a new member minimizes movement."""
        strategy = StickyAssignmentStrategy()
        
        members = ["consumer-1", "consumer-2"]
        partitions_per_topic = {"topic1": 6}
        subscriptions = {
            "consumer-1": {"topic1"},
            "consumer-2": {"topic1"},
        }
        
        assignment1 = strategy.assign(members, partitions_per_topic, subscriptions)
        
        c1_first = assignment1["consumer-1"].copy()
        c2_first = assignment1["consumer-2"].copy()
        
        members = ["consumer-1", "consumer-2", "consumer-3"]
        subscriptions["consumer-3"] = {"topic1"}
        
        assignment2 = strategy.assign(members, partitions_per_topic, subscriptions)
        
        kept_c1 = c1_first & assignment2["consumer-1"]
        kept_c2 = c2_first & assignment2["consumer-2"]
        
        assert len(kept_c1) >= 1
        assert len(kept_c2) >= 1
    
    def test_member_removed(self):
        """Test removing a member redistributes partitions."""
        strategy = StickyAssignmentStrategy()
        
        members = ["consumer-1", "consumer-2", "consumer-3"]
        partitions_per_topic = {"topic1": 6}
        subscriptions = {
            "consumer-1": {"topic1"},
            "consumer-2": {"topic1"},
            "consumer-3": {"topic1"},
        }
        
        assignment1 = strategy.assign(members, partitions_per_topic, subscriptions)
        
        c1_first = assignment1["consumer-1"].copy()
        
        members = ["consumer-1", "consumer-2"]
        del subscriptions["consumer-3"]
        
        assignment2 = strategy.assign(members, partitions_per_topic, subscriptions)
        
        kept_c1 = c1_first & assignment2["consumer-1"]
        
        assert len(kept_c1) == len(c1_first)


class TestStrategyFactory:
    """Test strategy factory."""
    
    def test_create_range(self):
        """Test creating range strategy."""
        strategy = create_assignment_strategy("range")
        assert isinstance(strategy, RangeAssignmentStrategy)
    
    def test_create_roundrobin(self):
        """Test creating round-robin strategy."""
        strategy = create_assignment_strategy("roundrobin")
        assert isinstance(strategy, RoundRobinAssignmentStrategy)
    
    def test_create_sticky(self):
        """Test creating sticky strategy."""
        strategy = create_assignment_strategy("sticky")
        assert isinstance(strategy, StickyAssignmentStrategy)
    
    def test_create_unknown(self):
        """Test creating unknown strategy."""
        with pytest.raises(ValueError, match="Unknown assignment strategy"):
            create_assignment_strategy("unknown")
