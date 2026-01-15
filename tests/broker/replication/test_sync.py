"""
Tests for replication coordination.
"""

import pytest

from distributedlog.broker.replication.ack import AckMode, ProduceRequest
from distributedlog.broker.replication.sync import (
    ReplicationConfig,
    ReplicationCoordinator,
)


@pytest.mark.asyncio
class TestReplicationCoordinator:
    """Test ReplicationCoordinator."""
    
    async def test_become_leader(self):
        """Test becoming leader for a partition."""
        coordinator = ReplicationCoordinator(broker_id=1)
        
        await coordinator.become_leader(
            topic="test",
            partition=0,
            replica_ids=[1, 2, 3],
        )
        
        assert coordinator.is_leader_for("test", 0)
        assert not coordinator.is_follower_for("test", 0)
        
        isr = await coordinator.get_isr("test", 0)
        assert 1 in isr
    
    async def test_become_follower(self):
        """Test becoming follower for a partition."""
        coordinator = ReplicationCoordinator(broker_id=2)
        
        await coordinator.become_follower(
            topic="test",
            partition=0,
            leader_id=1,
            start_offset=0,
        )
        
        assert not coordinator.is_leader_for("test", 0)
        assert coordinator.is_follower_for("test", 0)
    
    async def test_switch_from_leader_to_follower(self):
        """Test switching from leader to follower."""
        coordinator = ReplicationCoordinator(broker_id=1)
        
        # Become leader
        await coordinator.become_leader(
            topic="test",
            partition=0,
            replica_ids=[1, 2, 3],
        )
        
        assert coordinator.is_leader_for("test", 0)
        
        # Become follower
        await coordinator.become_follower(
            topic="test",
            partition=0,
            leader_id=2,
        )
        
        assert not coordinator.is_leader_for("test", 0)
        assert coordinator.is_follower_for("test", 0)
    
    async def test_switch_from_follower_to_leader(self):
        """Test switching from follower to leader."""
        coordinator = ReplicationCoordinator(broker_id=1)
        
        # Become follower
        await coordinator.become_follower(
            topic="test",
            partition=0,
            leader_id=2,
        )
        
        assert coordinator.is_follower_for("test", 0)
        
        # Become leader
        await coordinator.become_leader(
            topic="test",
            partition=0,
            replica_ids=[1, 2, 3],
        )
        
        assert coordinator.is_leader_for("test", 0)
        assert not coordinator.is_follower_for("test", 0)
    
    async def test_stop_leading(self):
        """Test stopping leadership."""
        coordinator = ReplicationCoordinator(broker_id=1)
        
        await coordinator.become_leader(
            topic="test",
            partition=0,
            replica_ids=[1, 2, 3],
        )
        
        await coordinator.stop_leading("test", 0)
        
        assert not coordinator.is_leader_for("test", 0)
    
    async def test_stop_following(self):
        """Test stopping following."""
        coordinator = ReplicationCoordinator(broker_id=2)
        
        await coordinator.become_follower(
            topic="test",
            partition=0,
            leader_id=1,
        )
        
        await coordinator.stop_following("test", 0)
        
        assert not coordinator.is_follower_for("test", 0)
    
    async def test_replicate_produce_not_leader(self):
        """Test produce when not leader."""
        coordinator = ReplicationCoordinator(broker_id=1)
        
        request = ProduceRequest(
            topic="test",
            partition=0,
            messages=[b"msg1"],
        )
        
        response = await coordinator.replicate_produce(
            request=request,
            base_offset=0,
            log_end_offset=1,
        )
        
        # Should fail - not leader
        assert response.error_code == 3
    
    async def test_replicate_produce_insufficient_isr(self):
        """Test produce with insufficient ISR."""
        config = ReplicationConfig(min_isr=3)
        coordinator = ReplicationCoordinator(broker_id=1, config=config)
        
        # Become leader with only 1 replica (insufficient ISR)
        await coordinator.become_leader(
            topic="test",
            partition=0,
            replica_ids=[1],
        )
        
        request = ProduceRequest(
            topic="test",
            partition=0,
            messages=[b"msg1"],
        )
        
        response = await coordinator.replicate_produce(
            request=request,
            base_offset=0,
            log_end_offset=1,
        )
        
        # Should fail - insufficient ISR
        assert response.error_code == 4
    
    async def test_replicate_produce_acks_leader(self):
        """Test produce with acks=1."""
        coordinator = ReplicationCoordinator(broker_id=1)
        
        await coordinator.become_leader(
            topic="test",
            partition=0,
            replica_ids=[1, 2, 3],
        )
        
        request = ProduceRequest(
            topic="test",
            partition=0,
            messages=[b"msg1"],
            acks=AckMode.LEADER,
        )
        
        response = await coordinator.replicate_produce(
            request=request,
            base_offset=0,
            log_end_offset=1,
        )
        
        # Should succeed immediately with leader ack
        assert response.error_code == 0
        assert response.base_offset == 0
    
    async def test_update_follower_replica(self):
        """Test updating follower replica."""
        coordinator = ReplicationCoordinator(broker_id=1)
        
        await coordinator.become_leader(
            topic="test",
            partition=0,
            replica_ids=[1, 2, 3],
        )
        
        await coordinator.update_follower_replica(
            topic="test",
            partition=0,
            broker_id=2,
            log_end_offset=100,
        )
        
        # Should not raise
    
    async def test_get_high_watermark(self):
        """Test getting high-water mark."""
        coordinator = ReplicationCoordinator(broker_id=1)
        
        await coordinator.become_leader(
            topic="test",
            partition=0,
            replica_ids=[1, 2, 3],
        )
        
        hwm = await coordinator.get_high_watermark("test", 0)
        
        assert hwm >= 0
    
    async def test_shutdown(self):
        """Test shutdown."""
        coordinator = ReplicationCoordinator(broker_id=1)
        
        await coordinator.become_follower(
            topic="test",
            partition=0,
            leader_id=2,
        )
        
        await coordinator.shutdown()
        
        assert not coordinator.is_follower_for("test", 0)


class TestReplicationConfig:
    """Test ReplicationConfig."""
    
    def test_default_config(self):
        """Test default configuration."""
        config = ReplicationConfig()
        
        assert config.max_replica_lag == 10
        assert config.max_replica_lag_time_ms == 10000
        assert config.min_isr == 1
        assert config.fetch_interval_ms == 500
        assert config.fetch_max_bytes == 1048576
    
    def test_custom_config(self):
        """Test custom configuration."""
        config = ReplicationConfig(
            max_replica_lag=20,
            max_replica_lag_time_ms=5000,
            min_isr=2,
        )
        
        assert config.max_replica_lag == 20
        assert config.max_replica_lag_time_ms == 5000
        assert config.min_isr == 2
