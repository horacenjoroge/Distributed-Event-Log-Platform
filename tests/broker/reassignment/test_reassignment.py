"""
Tests for partition reassignment.
"""

import asyncio
import tempfile
from pathlib import Path

import pytest

from distributedlog.broker.controller.controller import ClusterController, PartitionAssignment
from distributedlog.broker.metadata import BrokerMetadata
from distributedlog.broker.reassignment import (
    ReassignmentExecutor,
    ReassignmentMonitor,
    ReassignmentPhase,
    ReassignmentState,
    ReassignmentTracker,
)
from distributedlog.broker.reassignment.throttle import ReplicationThrottle
from distributedlog.consensus.raft.node import RaftNode


class TestReassignmentState:
    """Test ReassignmentState."""
    
    def test_create_state(self):
        """Test creating reassignment state."""
        state = ReassignmentState(
            topic="test",
            partition=0,
            old_replicas=[1, 2, 3],
            new_replicas=[2, 3, 4],
        )
        
        assert state.topic == "test"
        assert state.partition == 0
        assert state.old_replicas == [1, 2, 3]
        assert state.new_replicas == [2, 3, 4]
        assert state.phase == ReassignmentPhase.PENDING
    
    def test_get_replicas_to_add(self):
        """Test getting replicas to add."""
        state = ReassignmentState(
            topic="test",
            partition=0,
            old_replicas=[1, 2, 3],
            new_replicas=[2, 3, 4],
        )
        
        to_add = state.get_replicas_to_add()
        
        assert to_add == [4]
    
    def test_get_replicas_to_remove(self):
        """Test getting replicas to remove."""
        state = ReassignmentState(
            topic="test",
            partition=0,
            old_replicas=[1, 2, 3],
            new_replicas=[2, 3, 4],
        )
        
        to_remove = state.get_replicas_to_remove()
        
        assert to_remove == [1]
    
    def test_is_complete(self):
        """Test checking if complete."""
        state = ReassignmentState(
            topic="test",
            partition=0,
            old_replicas=[1, 2, 3],
            new_replicas=[2, 3, 4],
        )
        
        assert not state.is_complete()
        
        state.phase = ReassignmentPhase.COMPLETED
        assert state.is_complete()


class TestReassignmentTracker:
    """Test ReassignmentTracker."""
    
    def test_add_reassignment(self):
        """Test adding reassignment."""
        tracker = ReassignmentTracker()
        
        state = tracker.add_reassignment(
            topic="test",
            partition=0,
            old_replicas=[1, 2, 3],
            new_replicas=[2, 3, 4],
        )
        
        assert state.topic == "test"
        assert state.partition == 0
    
    def test_get_reassignment(self):
        """Test getting reassignment."""
        tracker = ReassignmentTracker()
        
        tracker.add_reassignment(
            topic="test",
            partition=0,
            old_replicas=[1, 2, 3],
            new_replicas=[2, 3, 4],
        )
        
        state = tracker.get_reassignment("test", 0)
        
        assert state is not None
        assert state.topic == "test"
    
    def test_remove_reassignment(self):
        """Test removing reassignment."""
        tracker = ReassignmentTracker()
        
        tracker.add_reassignment(
            topic="test",
            partition=0,
            old_replicas=[1, 2, 3],
            new_replicas=[2, 3, 4],
        )
        
        tracker.remove_reassignment("test", 0)
        
        # Should be in completed
        completed = tracker.get_completed_reassignments()
        assert len(completed) == 1
    
    def test_cancel_reassignment(self):
        """Test cancelling reassignment."""
        tracker = ReassignmentTracker()
        
        tracker.add_reassignment(
            topic="test",
            partition=0,
            old_replicas=[1, 2, 3],
            new_replicas=[2, 3, 4],
        )
        
        cancelled = tracker.cancel_reassignment("test", 0)
        
        assert cancelled
        
        state = tracker.get_reassignment("test", 0)
        assert state.phase == ReassignmentPhase.CANCELLED


@pytest.mark.asyncio
class TestReassignmentExecutor:
    """Test ReassignmentExecutor."""
    
    async def test_initialize(self):
        """Test initializing executor."""
        with tempfile.TemporaryDirectory() as tmpdir:
            raft_node = RaftNode(
                node_id=1,
                peer_ids=[],
                state_dir=Path(tmpdir),
            )
            
            controller = ClusterController(
                broker_id=1,
                raft_node=raft_node,
            )
            
            executor = ReassignmentExecutor(controller)
            
            assert executor.controller == controller
            assert executor.tracker is not None
    
    async def test_start_reassignment(self):
        """Test starting reassignment."""
        with tempfile.TemporaryDirectory() as tmpdir:
            raft_node = RaftNode(
                node_id=1,
                peer_ids=[],
                state_dir=Path(tmpdir),
            )
            
            controller = ClusterController(
                broker_id=1,
                raft_node=raft_node,
            )
            
            # Add partition assignment
            controller.partition_assignments[("test", 0)] = PartitionAssignment(
                topic="test",
                partition=0,
                replicas=[1, 2, 3],
                leader=1,
                isr={1, 2, 3},
            )
            
            executor = ReassignmentExecutor(controller)
            
            success = await executor.start_reassignment(
                topic="test",
                partition=0,
                new_replicas=[2, 3, 4],
            )
            
            assert success
            
            # Check tracker
            state = executor.tracker.get_reassignment("test", 0)
            assert state is not None
            assert state.new_replicas == [2, 3, 4]
    
    async def test_cancel_reassignment(self):
        """Test cancelling reassignment."""
        with tempfile.TemporaryDirectory() as tmpdir:
            raft_node = RaftNode(
                node_id=1,
                peer_ids=[],
                state_dir=Path(tmpdir),
            )
            
            controller = ClusterController(
                broker_id=1,
                raft_node=raft_node,
            )
            
            controller.partition_assignments[("test", 0)] = PartitionAssignment(
                topic="test",
                partition=0,
                replicas=[1, 2, 3],
                leader=1,
                isr={1, 2, 3},
            )
            
            executor = ReassignmentExecutor(controller)
            
            await executor.start_reassignment(
                topic="test",
                partition=0,
                new_replicas=[2, 3, 4],
            )
            
            cancelled = await executor.cancel_reassignment("test", 0)
            
            assert cancelled
    
    async def test_get_reassignment_status(self):
        """Test getting reassignment status."""
        with tempfile.TemporaryDirectory() as tmpdir:
            raft_node = RaftNode(
                node_id=1,
                peer_ids=[],
                state_dir=Path(tmpdir),
            )
            
            controller = ClusterController(
                broker_id=1,
                raft_node=raft_node,
            )
            
            controller.partition_assignments[("test", 0)] = PartitionAssignment(
                topic="test",
                partition=0,
                replicas=[1, 2, 3],
                leader=1,
                isr={1, 2, 3},
            )
            
            executor = ReassignmentExecutor(controller)
            
            await executor.start_reassignment(
                topic="test",
                partition=0,
                new_replicas=[2, 3, 4],
            )
            
            status = executor.get_reassignment_status("test", 0)
            
            assert status is not None
            assert status["topic"] == "test"
            assert status["partition"] == 0
            assert "progress_percent" in status


@pytest.mark.asyncio
class TestReplicationThrottle:
    """Test ReplicationThrottle."""
    
    async def test_acquire_within_limit(self):
        """Test acquiring tokens within limit."""
        throttle = ReplicationThrottle(bytes_per_second=1000000)  # 1 MB/s
        
        # Should not block for small request
        await throttle.acquire(1000)  # 1 KB
        
        assert throttle.total_bytes_throttled == 1000
    
    async def test_acquire_exceeds_limit(self):
        """Test acquiring tokens that exceeds limit."""
        throttle = ReplicationThrottle(bytes_per_second=1000)  # 1 KB/s
        
        import time
        start = time.time()
        
        # Request 2 KB (should take ~1 second)
        await throttle.acquire(2000)
        
        elapsed = time.time() - start
        
        # Should have waited
        assert elapsed >= 1.0
    
    async def test_set_rate(self):
        """Test changing throttle rate."""
        throttle = ReplicationThrottle(bytes_per_second=1000)
        
        throttle.set_rate(2000)
        
        assert throttle.bytes_per_second == 2000
    
    async def test_get_stats(self):
        """Test getting throttle statistics."""
        throttle = ReplicationThrottle(bytes_per_second=1000000)
        
        await throttle.acquire(1000)
        
        stats = throttle.get_stats()
        
        assert stats["total_bytes_throttled"] == 1000
        assert "bytes_per_second" in stats


class TestReassignmentMonitor:
    """Test ReassignmentMonitor."""
    
    def test_update_metrics(self):
        """Test updating metrics."""
        tracker = ReassignmentTracker()
        monitor = ReassignmentMonitor(tracker)
        
        tracker.add_reassignment(
            topic="test",
            partition=0,
            old_replicas=[1, 2, 3],
            new_replicas=[2, 3, 4],
        )
        
        monitor.update_metrics(
            topic="test",
            partition=0,
            bytes_replicated=1000,
        )
        
        metrics = monitor.get_metrics("test", 0)
        
        assert metrics is not None
        assert metrics.bytes_replicated == 1000
    
    def test_get_summary(self):
        """Test getting summary."""
        tracker = ReassignmentTracker()
        monitor = ReassignmentMonitor(tracker)
        
        tracker.add_reassignment(
            topic="test",
            partition=0,
            old_replicas=[1, 2, 3],
            new_replicas=[2, 3, 4],
        )
        
        summary = monitor.get_summary()
        
        assert summary["active_count"] == 1
        assert "total_bytes_replicated" in summary
