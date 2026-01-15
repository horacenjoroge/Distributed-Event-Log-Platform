"""Tests for consumer group metadata."""

import time

import pytest

from distributedlog.consumer.group.metadata import (
    ConsumerGroupMetadata,
    GroupOffsets,
    GroupState,
    MemberMetadata,
)
from distributedlog.consumer.offset import TopicPartition


class TestMemberMetadata:
    """Test MemberMetadata."""
    
    def test_create_member(self):
        """Test creating member metadata."""
        member = MemberMetadata(
            member_id="member-1",
            client_id="client-1",
            session_timeout_ms=10000,
        )
        
        assert member.member_id == "member-1"
        assert member.client_id == "client-1"
        assert member.session_timeout_ms == 10000
        assert member.last_heartbeat_ms > 0
    
    def test_heartbeat_update(self):
        """Test heartbeat update."""
        member = MemberMetadata(
            member_id="member-1",
            client_id="client-1",
        )
        
        old_heartbeat = member.last_heartbeat_ms
        time.sleep(0.01)
        member.update_heartbeat()
        
        assert member.last_heartbeat_ms > old_heartbeat
    
    def test_member_expiration(self):
        """Test member expiration detection."""
        member = MemberMetadata(
            member_id="member-1",
            client_id="client-1",
            session_timeout_ms=100,
        )
        
        current_time = int(time.time() * 1000)
        
        assert not member.is_expired(current_time)
        
        future_time = current_time + 200
        assert member.is_expired(future_time)


class TestConsumerGroupMetadata:
    """Test ConsumerGroupMetadata."""
    
    def test_create_group(self):
        """Test creating group metadata."""
        group = ConsumerGroupMetadata(
            group_id="test-group",
            protocol_name="range",
        )
        
        assert group.group_id == "test-group"
        assert group.protocol_name == "range"
        assert group.generation_id == 0
        assert group.state == GroupState.EMPTY
        assert len(group.members) == 0
    
    def test_add_members(self):
        """Test adding members to group."""
        group = ConsumerGroupMetadata(group_id="test-group")
        
        member1 = MemberMetadata(
            member_id="member-1",
            client_id="client-1",
            subscription={"topic1"},
        )
        
        member2 = MemberMetadata(
            member_id="member-2",
            client_id="client-2",
            subscription={"topic1", "topic2"},
        )
        
        group.members["member-1"] = member1
        group.members["member-2"] = member2
        
        assert len(group.members) == 2
        assert group.get_all_subscriptions() == {"topic1", "topic2"}
    
    def test_get_all_partitions(self):
        """Test getting all assigned partitions."""
        group = ConsumerGroupMetadata(group_id="test-group")
        
        member1 = MemberMetadata(
            member_id="member-1",
            client_id="client-1",
            assignment={
                TopicPartition("topic1", 0),
                TopicPartition("topic1", 1),
            },
        )
        
        member2 = MemberMetadata(
            member_id="member-2",
            client_id="client-2",
            assignment={
                TopicPartition("topic1", 2),
            },
        )
        
        group.members["member-1"] = member1
        group.members["member-2"] = member2
        
        all_partitions = group.get_all_partitions()
        
        assert len(all_partitions) == 3
        assert TopicPartition("topic1", 0) in all_partitions
        assert TopicPartition("topic1", 1) in all_partitions
        assert TopicPartition("topic1", 2) in all_partitions
    
    def test_clear_assignments(self):
        """Test clearing member assignments."""
        group = ConsumerGroupMetadata(group_id="test-group")
        
        member = MemberMetadata(
            member_id="member-1",
            client_id="client-1",
            assignment={TopicPartition("topic1", 0)},
        )
        
        group.members["member-1"] = member
        
        assert len(member.assignment) == 1
        
        group.clear_assignments()
        
        assert len(member.assignment) == 0
    
    def test_remove_expired_members(self):
        """Test removing expired members."""
        group = ConsumerGroupMetadata(group_id="test-group")
        
        member1 = MemberMetadata(
            member_id="member-1",
            client_id="client-1",
            session_timeout_ms=100,
        )
        
        member2 = MemberMetadata(
            member_id="member-2",
            client_id="client-2",
            session_timeout_ms=100,
        )
        
        member2.last_heartbeat_ms = int(time.time() * 1000) - 200
        
        group.members["member-1"] = member1
        group.members["member-2"] = member2
        
        assert len(group.members) == 2
        
        current_time = int(time.time() * 1000)
        expired = group.remove_expired_members(current_time)
        
        assert len(expired) == 1
        assert "member-2" in expired
        assert len(group.members) == 1
        assert "member-1" in group.members
    
    def test_next_generation(self):
        """Test generation advancement."""
        group = ConsumerGroupMetadata(group_id="test-group")
        
        assert group.generation_id == 0
        
        group.next_generation()
        assert group.generation_id == 1
        
        group.next_generation()
        assert group.generation_id == 2
    
    def test_is_leader(self):
        """Test leader checking."""
        group = ConsumerGroupMetadata(
            group_id="test-group",
            leader_id="member-1",
        )
        
        assert group.is_leader("member-1")
        assert not group.is_leader("member-2")


class TestGroupOffsets:
    """Test GroupOffsets."""
    
    def test_create_offsets(self):
        """Test creating group offsets."""
        offsets = GroupOffsets(group_id="test-group")
        
        assert offsets.group_id == "test-group"
        assert len(offsets.offsets) == 0
    
    def test_commit_offset(self):
        """Test committing offsets."""
        offsets = GroupOffsets(group_id="test-group")
        
        tp1 = TopicPartition("topic1", 0)
        tp2 = TopicPartition("topic1", 1)
        
        offsets.commit(tp1, 100)
        offsets.commit(tp2, 200)
        
        assert offsets.get_offset(tp1) == 100
        assert offsets.get_offset(tp2) == 200
    
    def test_get_nonexistent_offset(self):
        """Test getting nonexistent offset."""
        offsets = GroupOffsets(group_id="test-group")
        
        tp = TopicPartition("topic1", 0)
        
        assert offsets.get_offset(tp) is None
    
    def test_calculate_lag(self):
        """Test lag calculation."""
        offsets = GroupOffsets(group_id="test-group")
        
        tp = TopicPartition("topic1", 0)
        
        offsets.commit(tp, 100)
        
        lag = offsets.get_lag(tp, 150)
        assert lag == 50
        
        lag = offsets.get_lag(tp, 100)
        assert lag == 0
    
    def test_lag_without_commit(self):
        """Test lag when no offset committed."""
        offsets = GroupOffsets(group_id="test-group")
        
        tp = TopicPartition("topic1", 0)
        
        lag = offsets.get_lag(tp, 100)
        assert lag is None
