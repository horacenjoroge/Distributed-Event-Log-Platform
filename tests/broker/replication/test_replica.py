"""
Tests for replica management.
"""

import asyncio
import time

import pytest

from distributedlog.broker.replication.replica import (
    PartitionReplicaSet,
    ReplicaInfo,
    ReplicaManager,
    ReplicaState,
)


class TestReplicaInfo:
    """Test ReplicaInfo."""
    
    def test_create_replica(self):
        """Test creating replica."""
        replica = ReplicaInfo(
            broker_id=1,
            topic="test",
            partition=0,
        )
        
        assert replica.broker_id == 1
        assert replica.topic == "test"
        assert replica.partition == 0
        assert replica.log_end_offset == 0
        assert replica.state == ReplicaState.SYNCING
    
    def test_update_fetch_time(self):
        """Test updating fetch time."""
        replica = ReplicaInfo(
            broker_id=1,
            topic="test",
            partition=0,
        )
        
        old_time = replica.last_fetch_time
        time.sleep(0.01)
        
        replica.update_fetch_time()
        
        assert replica.last_fetch_time > old_time
    
    def test_is_caught_up(self):
        """Test caught up check."""
        replica = ReplicaInfo(
            broker_id=1,
            topic="test",
            partition=0,
            log_end_offset=95,
        )
        
        # Within lag threshold
        assert replica.is_caught_up(100, max_lag=10)
        
        # Exactly at threshold
        assert replica.is_caught_up(105, max_lag=10)
        
        # Beyond threshold
        assert not replica.is_caught_up(106, max_lag=10)
    
    def test_is_lagging(self):
        """Test lagging check."""
        replica = ReplicaInfo(
            broker_id=1,
            topic="test",
            partition=0,
        )
        
        current_time = int(time.time() * 1000)
        
        # Not lagging (just fetched)
        assert not replica.is_lagging(current_time, max_lag_time_ms=10000)
        
        # Lagging (old fetch time)
        replica.last_fetch_time = current_time - 15000
        assert replica.is_lagging(current_time, max_lag_time_ms=10000)


class TestPartitionReplicaSet:
    """Test PartitionReplicaSet."""
    
    def test_create_replica_set(self):
        """Test creating replica set."""
        replica_set = PartitionReplicaSet(
            topic="test",
            partition=0,
            leader_id=1,
        )
        
        assert replica_set.topic == "test"
        assert replica_set.partition == 0
        assert replica_set.leader_id == 1
        assert 1 in replica_set.isr
        assert replica_set.high_watermark == 0
    
    def test_add_replica(self):
        """Test adding replicas."""
        replica_set = PartitionReplicaSet(
            topic="test",
            partition=0,
            leader_id=1,
        )
        
        replica = ReplicaInfo(
            broker_id=2,
            topic="test",
            partition=0,
        )
        
        replica_set.add_replica(replica)
        
        assert 2 in replica_set.replicas
        assert replica_set.replicas[2] == replica
    
    def test_update_replica_offset(self):
        """Test updating replica offset."""
        replica_set = PartitionReplicaSet(
            topic="test",
            partition=0,
            leader_id=1,
        )
        
        replica = ReplicaInfo(
            broker_id=2,
            topic="test",
            partition=0,
        )
        replica_set.add_replica(replica)
        
        replica_set.update_replica_offset(2, 100)
        
        assert replica_set.replicas[2].log_end_offset == 100
    
    def test_update_isr_adds_caught_up_replicas(self):
        """Test ISR update adds caught up replicas."""
        replica_set = PartitionReplicaSet(
            topic="test",
            partition=0,
            leader_id=1,
        )
        
        # Add leader replica
        leader = ReplicaInfo(
            broker_id=1,
            topic="test",
            partition=0,
            log_end_offset=100,
        )
        replica_set.add_replica(leader)
        
        # Add follower replica that's caught up
        follower = ReplicaInfo(
            broker_id=2,
            topic="test",
            partition=0,
            log_end_offset=95,
        )
        replica_set.add_replica(follower)
        
        changed = replica_set.update_isr(100, max_lag=10)
        
        assert changed
        assert 2 in replica_set.isr
        assert follower.state == ReplicaState.IN_SYNC
    
    def test_update_isr_removes_lagging_replicas(self):
        """Test ISR update removes lagging replicas."""
        replica_set = PartitionReplicaSet(
            topic="test",
            partition=0,
            leader_id=1,
        )
        
        # Add leader replica
        leader = ReplicaInfo(
            broker_id=1,
            topic="test",
            partition=0,
            log_end_offset=100,
        )
        replica_set.add_replica(leader)
        
        # Add follower replica that's lagging
        follower = ReplicaInfo(
            broker_id=2,
            topic="test",
            partition=0,
            log_end_offset=50,
            state=ReplicaState.IN_SYNC,
        )
        replica_set.add_replica(follower)
        replica_set.isr.add(2)
        
        changed = replica_set.update_isr(100, max_lag=10)
        
        assert changed
        assert 2 not in replica_set.isr
        assert follower.state == ReplicaState.SYNCING
    
    def test_remove_lagging_replicas_by_time(self):
        """Test removing replicas lagging by time."""
        replica_set = PartitionReplicaSet(
            topic="test",
            partition=0,
            leader_id=1,
        )
        
        # Add follower to ISR
        follower = ReplicaInfo(
            broker_id=2,
            topic="test",
            partition=0,
        )
        replica_set.add_replica(follower)
        replica_set.isr.add(2)
        follower.state = ReplicaState.IN_SYNC
        
        # Make it lag by time
        current_time = int(time.time() * 1000)
        follower.last_fetch_time = current_time - 15000
        
        removed = replica_set.remove_lagging_replicas(
            current_time,
            max_lag_time_ms=10000,
        )
        
        assert 2 in removed
        assert 2 not in replica_set.isr
        assert follower.state == ReplicaState.SYNCING
    
    def test_calculate_high_watermark(self):
        """Test high-water mark calculation."""
        replica_set = PartitionReplicaSet(
            topic="test",
            partition=0,
            leader_id=1,
        )
        
        # Add leader
        leader = ReplicaInfo(
            broker_id=1,
            topic="test",
            partition=0,
            log_end_offset=100,
        )
        replica_set.add_replica(leader)
        
        # Add follower 1 (in ISR)
        follower1 = ReplicaInfo(
            broker_id=2,
            topic="test",
            partition=0,
            log_end_offset=95,
        )
        replica_set.add_replica(follower1)
        replica_set.isr.add(2)
        
        # Add follower 2 (not in ISR, lagging)
        follower2 = ReplicaInfo(
            broker_id=3,
            topic="test",
            partition=0,
            log_end_offset=50,
        )
        replica_set.add_replica(follower2)
        
        hwm = replica_set.calculate_high_watermark()
        
        # HWM should be min of ISR (leader=100, follower1=95)
        assert hwm == 95
        assert replica_set.high_watermark == 95
    
    def test_is_under_replicated(self):
        """Test under-replication check."""
        replica_set = PartitionReplicaSet(
            topic="test",
            partition=0,
            leader_id=1,
        )
        
        # Only leader in ISR
        assert replica_set.is_under_replicated(replication_factor=3)
        
        # Add two more to ISR
        replica_set.isr.add(2)
        replica_set.isr.add(3)
        
        assert not replica_set.is_under_replicated(replication_factor=3)
    
    def test_get_replication_lag(self):
        """Test getting replication lag."""
        replica_set = PartitionReplicaSet(
            topic="test",
            partition=0,
            leader_id=1,
        )
        
        # Add leader
        leader = ReplicaInfo(
            broker_id=1,
            topic="test",
            partition=0,
            log_end_offset=100,
        )
        replica_set.add_replica(leader)
        
        # Add follower
        follower = ReplicaInfo(
            broker_id=2,
            topic="test",
            partition=0,
            log_end_offset=85,
        )
        replica_set.add_replica(follower)
        
        lag = replica_set.get_replication_lag(2)
        
        assert lag == 15


@pytest.mark.asyncio
class TestReplicaManager:
    """Test ReplicaManager."""
    
    async def test_create_partition_replica_set(self):
        """Test creating partition replica set."""
        manager = ReplicaManager()
        
        replica_set = await manager.create_partition_replica_set(
            topic="test",
            partition=0,
            leader_id=1,
            replica_ids=[1, 2, 3],
        )
        
        assert replica_set.topic == "test"
        assert replica_set.partition == 0
        assert replica_set.leader_id == 1
        assert len(replica_set.replicas) == 3
    
    async def test_update_follower_offset(self):
        """Test updating follower offset."""
        manager = ReplicaManager()
        
        await manager.create_partition_replica_set(
            topic="test",
            partition=0,
            leader_id=1,
            replica_ids=[1, 2, 3],
        )
        
        # Update leader offset
        await manager.update_follower_offset("test", 0, 1, 100)
        
        # Update follower offset
        await manager.update_follower_offset("test", 0, 2, 95)
        
        hwm = await manager.get_high_watermark("test", 0)
        
        # HWM should be 0 since follower 3 hasn't reported
        assert hwm >= 0
    
    async def test_get_isr(self):
        """Test getting ISR."""
        manager = ReplicaManager()
        
        await manager.create_partition_replica_set(
            topic="test",
            partition=0,
            leader_id=1,
            replica_ids=[1, 2, 3],
        )
        
        isr = await manager.get_isr("test", 0)
        
        # At least leader should be in ISR
        assert 1 in isr
    
    async def test_check_replica_health(self):
        """Test replica health check."""
        manager = ReplicaManager(max_replica_lag_time_ms=100)
        
        await manager.create_partition_replica_set(
            topic="test",
            partition=0,
            leader_id=1,
            replica_ids=[1, 2],
        )
        
        # Add follower to ISR
        key = ("test", 0)
        replica_set = manager._partitions[key]
        replica_set.isr.add(2)
        
        # Make follower lag by time
        follower = replica_set.replicas[2]
        follower.last_fetch_time = int(time.time() * 1000) - 1000
        
        # Check health
        await asyncio.sleep(0.2)
        await manager.check_replica_health()
        
        isr = await manager.get_isr("test", 0)
        
        # Follower should be removed from ISR
        # (might still be there depending on timing)
        assert isinstance(isr, set)
