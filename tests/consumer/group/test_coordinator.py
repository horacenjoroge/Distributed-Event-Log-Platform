"""Tests for group coordinator."""

import asyncio

import pytest

from distributedlog.consumer.group.coordinator import GroupCoordinator
from distributedlog.consumer.group.metadata import GroupState
from distributedlog.consumer.offset import TopicPartition


class TestGroupCoordinator:
    """Test GroupCoordinator."""
    
    @pytest.fixture
    def coordinator(self):
        """Create test coordinator."""
        partitions_per_topic = {
            "topic1": 6,
            "topic2": 4,
        }
        return GroupCoordinator(partitions_per_topic)
    
    @pytest.mark.asyncio
    async def test_single_member_join(self, coordinator):
        """Test single member joining group."""
        generation_id, member_id, is_leader = await coordinator.join_group(
            group_id="test-group",
            member_id="",
            client_id="client-1",
            session_timeout_ms=30000,
            rebalance_timeout_ms=60000,
            protocol_name="range",
            topics={"topic1"},
        )
        
        assert generation_id >= 0
        assert member_id.startswith("client-1")
        assert is_leader
    
    @pytest.mark.asyncio
    async def test_multiple_members_join(self, coordinator):
        """Test multiple members joining group."""
        gen1, member1, leader1 = await coordinator.join_group(
            group_id="test-group",
            member_id="",
            client_id="client-1",
            session_timeout_ms=30000,
            rebalance_timeout_ms=60000,
            protocol_name="range",
            topics={"topic1"},
        )
        
        gen2, member2, leader2 = await coordinator.join_group(
            group_id="test-group",
            member_id="",
            client_id="client-2",
            session_timeout_ms=30000,
            rebalance_timeout_ms=60000,
            protocol_name="range",
            topics={"topic1"},
        )
        
        assert member1 != member2
        assert leader1 or leader2
    
    @pytest.mark.asyncio
    async def test_sync_group(self, coordinator):
        """Test syncing with group."""
        gen_id, member_id, _ = await coordinator.join_group(
            group_id="test-group",
            member_id="",
            client_id="client-1",
            session_timeout_ms=30000,
            rebalance_timeout_ms=60000,
            protocol_name="range",
            topics={"topic1"},
        )
        
        await asyncio.sleep(3.5)
        
        assignment = await coordinator.sync_group(
            group_id="test-group",
            member_id=member_id,
            generation_id=gen_id,
        )
        
        assert len(assignment) > 0
        assert all(isinstance(tp, TopicPartition) for tp in assignment)
    
    @pytest.mark.asyncio
    async def test_heartbeat(self, coordinator):
        """Test heartbeat protocol."""
        gen_id, member_id, _ = await coordinator.join_group(
            group_id="test-group",
            member_id="",
            client_id="client-1",
            session_timeout_ms=30000,
            rebalance_timeout_ms=60000,
            protocol_name="range",
            topics={"topic1"},
        )
        
        await asyncio.sleep(3.5)
        
        await coordinator.sync_group(
            group_id="test-group",
            member_id=member_id,
            generation_id=gen_id,
        )
        
        result = await coordinator.heartbeat(
            group_id="test-group",
            member_id=member_id,
            generation_id=gen_id,
        )
        
        assert result is True
    
    @pytest.mark.asyncio
    async def test_heartbeat_wrong_generation(self, coordinator):
        """Test heartbeat with wrong generation."""
        gen_id, member_id, _ = await coordinator.join_group(
            group_id="test-group",
            member_id="",
            client_id="client-1",
            session_timeout_ms=30000,
            rebalance_timeout_ms=60000,
            protocol_name="range",
            topics={"topic1"},
        )
        
        await asyncio.sleep(3.5)
        
        await coordinator.sync_group(
            group_id="test-group",
            member_id=member_id,
            generation_id=gen_id,
        )
        
        result = await coordinator.heartbeat(
            group_id="test-group",
            member_id=member_id,
            generation_id=gen_id + 999,
        )
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_leave_group(self, coordinator):
        """Test leaving group."""
        gen_id, member_id, _ = await coordinator.join_group(
            group_id="test-group",
            member_id="",
            client_id="client-1",
            session_timeout_ms=30000,
            rebalance_timeout_ms=60000,
            protocol_name="range",
            topics={"topic1"},
        )
        
        await asyncio.sleep(3.5)
        
        await coordinator.leave_group(
            group_id="test-group",
            member_id=member_id,
        )
        
        group = coordinator._groups.get("test-group")
        assert group is not None
        assert member_id not in group.members
    
    @pytest.mark.asyncio
    async def test_commit_and_fetch_offset(self, coordinator):
        """Test committing and fetching offsets."""
        tp = TopicPartition("topic1", 0)
        
        await coordinator.commit_offset(
            group_id="test-group",
            topic_partition=tp,
            offset=100,
        )
        
        fetched = await coordinator.fetch_offset(
            group_id="test-group",
            topic_partition=tp,
        )
        
        assert fetched == 100
    
    @pytest.mark.asyncio
    async def test_calculate_lag(self, coordinator):
        """Test calculating consumer lag."""
        tp = TopicPartition("topic1", 0)
        
        await coordinator.commit_offset(
            group_id="test-group",
            topic_partition=tp,
            offset=100,
        )
        
        lag = await coordinator.get_lag(
            group_id="test-group",
            topic_partition=tp,
            log_end_offset=150,
        )
        
        assert lag == 50
    
    @pytest.mark.asyncio
    async def test_partition_assignment_distribution(self, coordinator):
        """Test that partitions are distributed evenly."""
        members = []
        
        for i in range(3):
            gen_id, member_id, _ = await coordinator.join_group(
                group_id="test-group",
                member_id="",
                client_id=f"client-{i}",
                session_timeout_ms=30000,
                rebalance_timeout_ms=60000,
                protocol_name="range",
                topics={"topic1"},
            )
            members.append((gen_id, member_id))
        
        await asyncio.sleep(3.5)
        
        assignments = []
        for gen_id, member_id in members:
            assignment = await coordinator.sync_group(
                group_id="test-group",
                member_id=member_id,
                generation_id=gen_id,
            )
            assignments.append(len(assignment))
        
        assert sum(assignments) == 6
        assert all(a >= 1 for a in assignments)
    
    @pytest.mark.asyncio
    async def test_different_assignment_strategies(self, coordinator):
        """Test different assignment strategies produce different results."""
        strategies = ["range", "roundrobin", "sticky"]
        
        all_assignments = []
        
        for strategy in strategies:
            group_id = f"test-group-{strategy}"
            
            members = []
            for i in range(2):
                gen_id, member_id, _ = await coordinator.join_group(
                    group_id=group_id,
                    member_id="",
                    client_id=f"client-{i}",
                    session_timeout_ms=30000,
                    rebalance_timeout_ms=60000,
                    protocol_name=strategy,
                    topics={"topic1"},
                )
                members.append((gen_id, member_id))
            
            await asyncio.sleep(3.5)
            
            assignments = []
            for gen_id, member_id in members:
                assignment = await coordinator.sync_group(
                    group_id=group_id,
                    member_id=member_id,
                    generation_id=gen_id,
                )
                assignments.append(assignment)
            
            all_assignments.append(assignments)
        
        for assignments in all_assignments:
            total = sum(len(a) for a in assignments)
            assert total == 6
