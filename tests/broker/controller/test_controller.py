"""
Tests for cluster controller.
"""

import asyncio
import tempfile
from pathlib import Path

import pytest

from distributedlog.broker.controller.controller import ClusterController, PartitionAssignment
from distributedlog.broker.metadata import BrokerMetadata
from distributedlog.consensus.raft.node import RaftNode


@pytest.mark.asyncio
class TestClusterController:
    """Test ClusterController."""
    
    async def test_initialize(self):
        """Test controller initialization."""
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
            
            assert controller.broker_id == 1
            assert not controller.state.is_active
            assert controller.state.controller_epoch == 0
    
    async def test_become_controller(self):
        """Test becoming controller."""
        with tempfile.TemporaryDirectory() as tmpdir:
            raft_node = RaftNode(
                node_id=1,
                peer_ids=[],
                state_dir=Path(tmpdir),
            )
            
            # Mock RPC handlers
            raft_node.send_request_vote = lambda *args: None
            raft_node.send_append_entries = lambda *args: None
            
            controller = ClusterController(
                broker_id=1,
                raft_node=raft_node,
            )
            
            await controller.start()
            
            # Make Raft node leader
            raft_node.state_machine.become_leader()
            
            # Wait for controller to detect leadership
            await asyncio.sleep(0.2)
            
            # Should become controller
            assert controller.state.is_active
            assert controller.state.controller_id == 1
            assert controller.state.controller_epoch > 0
            
            await controller.stop()
    
    async def test_register_broker(self):
        """Test broker registration."""
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
            
            metadata = BrokerMetadata(
                broker_id=2,
                host="localhost",
                port=9092,
            )
            
            await controller.register_broker(2, metadata)
            
            assert 2 in controller.broker_last_heartbeat
            assert 2 in controller.cluster_metadata.brokers
    
    async def test_broker_heartbeat(self):
        """Test broker heartbeat tracking."""
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
            
            # Register broker
            metadata = BrokerMetadata(
                broker_id=2,
                host="localhost",
                port=9092,
            )
            await controller.register_broker(2, metadata)
            
            # Send heartbeat
            await controller.broker_heartbeat(2)
            
            # Broker should be alive
            assert controller._is_broker_alive(2)
    
    async def test_broker_failure_detection(self):
        """Test broker failure detection."""
        with tempfile.TemporaryDirectory() as tmpdir:
            raft_node = RaftNode(
                node_id=1,
                peer_ids=[],
                state_dir=Path(tmpdir),
            )
            
            controller = ClusterController(
                broker_id=1,
                raft_node=raft_node,
                min_isr=1,
            )
            
            # Register broker
            metadata = BrokerMetadata(
                broker_id=2,
                host="localhost",
                port=9092,
            )
            await controller.register_broker(2, metadata)
            
            # Make old heartbeat
            controller.broker_last_heartbeat[2] = 0
            
            # Broker should be considered failed
            assert not controller._is_broker_alive(2)
    
    async def test_create_partition(self):
        """Test partition creation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            raft_node = RaftNode(
                node_id=1,
                peer_ids=[],
                state_dir=Path(tmpdir),
            )
            
            # Mock RPC handlers
            raft_node.send_request_vote = lambda *args: None
            raft_node.send_append_entries = lambda *args: None
            
            controller = ClusterController(
                broker_id=1,
                raft_node=raft_node,
            )
            
            await controller.start()
            
            # Make controller
            raft_node.state_machine.become_leader()
            await asyncio.sleep(0.1)
            
            # Register brokers
            for i in range(1, 4):
                metadata = BrokerMetadata(
                    broker_id=i,
                    host=f"broker{i}",
                    port=9092,
                )
                await controller.register_broker(i, metadata)
            
            # Create partition
            success = await controller.create_partition(
                topic="test",
                partition=0,
                replication_factor=3,
            )
            
            assert success
            
            key = ("test", 0)
            assert key in controller.partition_assignments
            
            assignment = controller.partition_assignments[key]
            assert len(assignment.replicas) == 3
            assert assignment.leader is not None
            
            await controller.stop()
    
    async def test_partition_leader_election(self):
        """Test partition leader election."""
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
            
            # Register brokers
            for i in range(1, 4):
                metadata = BrokerMetadata(
                    broker_id=i,
                    host=f"broker{i}",
                    port=9092,
                )
                await controller.register_broker(i, metadata)
            
            # Create partition assignment
            assignment = PartitionAssignment(
                topic="test",
                partition=0,
                replicas=[1, 2, 3],
                leader=1,
                isr={1, 2, 3},
            )
            
            controller.partition_assignments[("test", 0)] = assignment
            
            # Elect new leader
            new_leader = await controller._elect_partition_leader("test", 0)
            
            assert new_leader in [1, 2, 3]
            assert assignment.leader == new_leader
    
    async def test_partition_leader_election_from_isr(self):
        """Test leader election prefers ISR."""
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
            
            # Register brokers
            for i in range(1, 4):
                metadata = BrokerMetadata(
                    broker_id=i,
                    host=f"broker{i}",
                    port=9092,
                )
                await controller.register_broker(i, metadata)
            
            # Create partition with only broker 2 in ISR
            assignment = PartitionAssignment(
                topic="test",
                partition=0,
                replicas=[1, 2, 3],
                leader=None,
                isr={2},  # Only broker 2 in ISR
            )
            
            controller.partition_assignments[("test", 0)] = assignment
            
            # Elect leader
            new_leader = await controller._elect_partition_leader("test", 0)
            
            # Should elect broker 2 (only ISR member)
            assert new_leader == 2
    
    async def test_unclean_leader_election(self):
        """Test unclean leader election when no ISR available."""
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
            
            # Register brokers
            for i in range(1, 4):
                metadata = BrokerMetadata(
                    broker_id=i,
                    host=f"broker{i}",
                    port=9092,
                )
                await controller.register_broker(i, metadata)
            
            # Create partition with empty ISR
            assignment = PartitionAssignment(
                topic="test",
                partition=0,
                replicas=[1, 2, 3],
                leader=None,
                isr=set(),  # Empty ISR
            )
            
            controller.partition_assignments[("test", 0)] = assignment
            
            # Elect leader (unclean)
            new_leader = await controller._elect_partition_leader("test", 0)
            
            # Should elect from any alive replica
            assert new_leader in [1, 2, 3]
    
    async def test_broker_failure_triggers_reelection(self):
        """Test broker failure triggers partition leader reelection."""
        with tempfile.TemporaryDirectory() as tmpdir:
            raft_node = RaftNode(
                node_id=1,
                peer_ids=[],
                state_dir=Path(tmpdir),
            )
            
            # Mock RPC handlers
            raft_node.send_request_vote = lambda *args: None
            raft_node.send_append_entries = lambda *args: None
            
            controller = ClusterController(
                broker_id=1,
                raft_node=raft_node,
            )
            
            await controller.start()
            
            # Make controller
            raft_node.state_machine.become_leader()
            await asyncio.sleep(0.1)
            
            # Register brokers
            for i in range(1, 4):
                metadata = BrokerMetadata(
                    broker_id=i,
                    host=f"broker{i}",
                    port=9092,
                )
                await controller.register_broker(i, metadata)
            
            # Create partition with broker 2 as leader
            assignment = PartitionAssignment(
                topic="test",
                partition=0,
                replicas=[1, 2, 3],
                leader=2,
                isr={1, 2, 3},
            )
            
            controller.partition_assignments[("test", 0)] = assignment
            
            # Simulate broker 2 failure
            await controller._handle_broker_failure(2)
            
            # Leader should change
            assert assignment.leader != 2
            assert assignment.leader in [1, 3]
            
            # Broker 2 should be removed from ISR
            assert 2 not in assignment.isr
            
            await controller.stop()
    
    async def test_controller_epoch_increments(self):
        """Test controller epoch increments on controller change."""
        with tempfile.TemporaryDirectory() as tmpdir:
            raft_node = RaftNode(
                node_id=1,
                peer_ids=[],
                state_dir=Path(tmpdir),
            )
            
            # Mock RPC handlers
            raft_node.send_request_vote = lambda *args: None
            raft_node.send_append_entries = lambda *args: None
            
            controller = ClusterController(
                broker_id=1,
                raft_node=raft_node,
            )
            
            await controller.start()
            
            initial_epoch = controller.state.controller_epoch
            
            # Make controller
            raft_node.state_machine.become_leader()
            await asyncio.sleep(0.2)
            
            # Epoch should increment
            assert controller.state.controller_epoch > initial_epoch
            
            await controller.stop()
    
    async def test_query_partition_leader(self):
        """Test querying partition leader."""
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
            
            # Create partition
            assignment = PartitionAssignment(
                topic="test",
                partition=0,
                replicas=[1, 2, 3],
                leader=2,
                isr={1, 2, 3},
            )
            
            controller.partition_assignments[("test", 0)] = assignment
            
            # Query leader
            leader = controller.get_partition_leader("test", 0)
            
            assert leader == 2
    
    async def test_query_partition_isr(self):
        """Test querying partition ISR."""
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
            
            # Create partition
            assignment = PartitionAssignment(
                topic="test",
                partition=0,
                replicas=[1, 2, 3],
                leader=2,
                isr={1, 2},
            )
            
            controller.partition_assignments[("test", 0)] = assignment
            
            # Query ISR
            isr = controller.get_partition_isr("test", 0)
            
            assert isr == {1, 2}


class TestPartitionAssignment:
    """Test PartitionAssignment."""
    
    def test_create_assignment(self):
        """Test creating partition assignment."""
        assignment = PartitionAssignment(
            topic="test",
            partition=0,
            replicas=[1, 2, 3],
            leader=1,
            isr={1, 2},
        )
        
        assert assignment.topic == "test"
        assert assignment.partition == 0
        assert assignment.replicas == [1, 2, 3]
        assert assignment.leader == 1
        assert assignment.isr == {1, 2}
