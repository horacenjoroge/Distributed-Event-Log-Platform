"""
Tests for acknowledgment modes and monitoring.
"""

import asyncio
import time

import pytest

from distributedlog.broker.replication.ack import (
    AckManager,
    AckMode,
    ProduceRequest,
    ProduceResponse,
    ReplicationMonitor,
)
from distributedlog.broker.replication.replica import ReplicaManager


@pytest.mark.asyncio
class TestAckManager:
    """Test AckManager."""
    
    async def test_ack_mode_none(self):
        """Test acks=0 (no acknowledgment)."""
        replica_manager = ReplicaManager()
        ack_manager = AckManager(replica_manager)
        
        request = ProduceRequest(
            topic="test",
            partition=0,
            messages=[b"msg1"],
            acks=AckMode.NONE,
        )
        
        response = await ack_manager.wait_for_ack(
            request=request,
            base_offset=0,
            log_end_offset=1,
        )
        
        assert response.error_code == 0
        assert response.base_offset == 0
    
    async def test_ack_mode_leader(self):
        """Test acks=1 (leader acknowledgment)."""
        replica_manager = ReplicaManager()
        ack_manager = AckManager(replica_manager)
        
        request = ProduceRequest(
            topic="test",
            partition=0,
            messages=[b"msg1"],
            acks=AckMode.LEADER,
        )
        
        response = await ack_manager.wait_for_ack(
            request=request,
            base_offset=0,
            log_end_offset=1,
        )
        
        assert response.error_code == 0
        assert response.base_offset == 0
    
    async def test_ack_mode_all_success(self):
        """Test acks=all with ISR replication."""
        replica_manager = ReplicaManager()
        
        # Create partition with replicas
        await replica_manager.create_partition_replica_set(
            topic="test",
            partition=0,
            leader_id=1,
            replica_ids=[1, 2, 3],
        )
        
        ack_manager = AckManager(replica_manager)
        
        request = ProduceRequest(
            topic="test",
            partition=0,
            messages=[b"msg1"],
            acks=AckMode.ALL,
            timeout_ms=1000,
        )
        
        # Simulate replicas catching up in background
        async def simulate_replication():
            await asyncio.sleep(0.1)
            
            # Update follower offsets
            await replica_manager.update_follower_offset("test", 0, 1, 1)
            await replica_manager.update_follower_offset("test", 0, 2, 1)
            await replica_manager.update_follower_offset("test", 0, 3, 1)
        
        task = asyncio.create_task(simulate_replication())
        
        response = await ack_manager.wait_for_ack(
            request=request,
            base_offset=0,
            log_end_offset=1,
        )
        
        await task
        
        # Should succeed when ISR replicates
        assert response.error_code == 0
        assert response.base_offset == 0
    
    async def test_ack_mode_all_timeout(self):
        """Test acks=all timeout."""
        replica_manager = ReplicaManager()
        
        # Create partition
        await replica_manager.create_partition_replica_set(
            topic="test",
            partition=0,
            leader_id=1,
            replica_ids=[1, 2, 3],
        )
        
        ack_manager = AckManager(replica_manager)
        
        request = ProduceRequest(
            topic="test",
            partition=0,
            messages=[b"msg1"],
            acks=AckMode.ALL,
            timeout_ms=100,  # Short timeout
        )
        
        # Don't simulate replication - should timeout
        response = await ack_manager.wait_for_ack(
            request=request,
            base_offset=0,
            log_end_offset=1,
        )
        
        # Should timeout
        assert response.error_code == 2


@pytest.mark.asyncio
class TestReplicationMonitor:
    """Test ReplicationMonitor."""
    
    async def test_start_stop(self):
        """Test starting and stopping monitor."""
        replica_manager = ReplicaManager()
        monitor = ReplicationMonitor(replica_manager)
        
        await monitor.start(interval_s=1)
        
        assert monitor._monitoring_task is not None
        
        await monitor.stop()
        
        assert monitor._monitoring_task is None
    
    async def test_get_under_replicated_partitions(self):
        """Test getting under-replicated partitions."""
        replica_manager = ReplicaManager()
        monitor = ReplicationMonitor(replica_manager)
        
        partitions = await monitor.get_under_replicated_partitions()
        
        assert isinstance(partitions, set)
    
    async def test_get_replica_lag(self):
        """Test getting replica lag."""
        replica_manager = ReplicaManager()
        monitor = ReplicationMonitor(replica_manager)
        
        lag = await monitor.get_replica_lag("test", 0, 1)
        
        assert lag is None  # No data yet


class TestProduceRequest:
    """Test ProduceRequest."""
    
    def test_create_request(self):
        """Test creating produce request."""
        request = ProduceRequest(
            topic="test",
            partition=0,
            messages=[b"msg1", b"msg2"],
            acks=AckMode.ALL,
        )
        
        assert request.topic == "test"
        assert request.partition == 0
        assert len(request.messages) == 2
        assert request.acks == AckMode.ALL
        assert request.timeout_ms == 30000


class TestProduceResponse:
    """Test ProduceResponse."""
    
    def test_create_response(self):
        """Test creating produce response."""
        response = ProduceResponse(
            topic="test",
            partition=0,
            base_offset=100,
            timestamp=int(time.time() * 1000),
        )
        
        assert response.topic == "test"
        assert response.partition == 0
        assert response.base_offset == 100
        assert response.error_code == 0
