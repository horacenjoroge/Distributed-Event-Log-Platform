"""
Tests for Raft node implementation.
"""

import asyncio
import tempfile
from pathlib import Path

import pytest

from distributedlog.consensus.raft.node import RaftNode
from distributedlog.consensus.raft.rpc import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    RequestVoteRequest,
    RequestVoteResponse,
)
from distributedlog.consensus.raft.state import LogEntry, RaftState


class MockTransport:
    """Mock transport layer for testing."""
    
    def __init__(self):
        self.nodes: dict[int, RaftNode] = {}
        self.network_partition: set[tuple[int, int]] = set()
    
    def add_node(self, node: RaftNode) -> None:
        """Add node to transport."""
        self.nodes[node.node_id] = node
        
        # Set RPC handlers
        node.send_request_vote = self._make_request_vote_handler(node.node_id)
        node.send_append_entries = self._make_append_entries_handler(node.node_id)
    
    def partition(self, node1_id: int, node2_id: int) -> None:
        """Create network partition between two nodes."""
        self.network_partition.add((node1_id, node2_id))
        self.network_partition.add((node2_id, node1_id))
    
    def heal_partition(self, node1_id: int, node2_id: int) -> None:
        """Heal network partition."""
        self.network_partition.discard((node1_id, node2_id))
        self.network_partition.discard((node2_id, node1_id))
    
    def is_partitioned(self, from_id: int, to_id: int) -> bool:
        """Check if partition exists."""
        return (from_id, to_id) in self.network_partition
    
    def _make_request_vote_handler(self, from_id: int):
        """Create RequestVote RPC handler."""
        async def handler(to_id: int, request: RequestVoteRequest):
            if self.is_partitioned(from_id, to_id):
                raise Exception("Network partition")
            
            if to_id not in self.nodes:
                raise Exception("Node not found")
            
            return await self.nodes[to_id].handle_request_vote(request)
        
        return handler
    
    def _make_append_entries_handler(self, from_id: int):
        """Create AppendEntries RPC handler."""
        async def handler(to_id: int, request: AppendEntriesRequest):
            if self.is_partitioned(from_id, to_id):
                raise Exception("Network partition")
            
            if to_id not in self.nodes:
                raise Exception("Node not found")
            
            return await self.nodes[to_id].handle_append_entries(request)
        
        return handler


@pytest.mark.asyncio
class TestRaftNode:
    """Test RaftNode."""
    
    async def test_initialize(self):
        """Test node initialization."""
        with tempfile.TemporaryDirectory() as tmpdir:
            node = RaftNode(
                node_id=1,
                peer_ids=[2, 3],
                state_dir=Path(tmpdir),
            )
            
            assert node.node_id == 1
            assert node.peer_ids == [2, 3]
            assert node.state_machine.state == RaftState.FOLLOWER
    
    async def test_single_node_election(self):
        """Test election with single node."""
        with tempfile.TemporaryDirectory() as tmpdir:
            node = RaftNode(
                node_id=1,
                peer_ids=[],
                state_dir=Path(tmpdir),
                heartbeat_interval=0.01,
            )
            
            # Mock RPC handlers (no peers)
            node.send_request_vote = lambda *args: None
            node.send_append_entries = lambda *args: None
            
            await node.start()
            
            # Trigger election
            node.state_machine.last_heartbeat_time = 0
            
            await asyncio.sleep(0.5)
            
            # Should become leader
            assert node.is_leader()
            
            await node.stop()
    
    async def test_three_node_election(self):
        """Test election with three nodes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create three nodes
            nodes = []
            for i in range(1, 4):
                node_dir = Path(tmpdir) / str(i)
                node = RaftNode(
                    node_id=i,
                    peer_ids=[j for j in range(1, 4) if j != i],
                    state_dir=node_dir,
                    heartbeat_interval=0.01,
                )
                nodes.append(node)
            
            # Setup transport
            transport = MockTransport()
            for node in nodes:
                transport.add_node(node)
            
            # Start all nodes
            for node in nodes:
                await node.start()
            
            # Trigger election on node 1
            nodes[0].state_machine.last_heartbeat_time = 0
            
            # Wait for election
            await asyncio.sleep(0.5)
            
            # One node should be leader
            leaders = [node for node in nodes if node.is_leader()]
            assert len(leaders) == 1
            
            # Cleanup
            for node in nodes:
                await node.stop()
    
    async def test_leader_heartbeat(self):
        """Test leader sends heartbeats."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create two nodes
            node1 = RaftNode(
                node_id=1,
                peer_ids=[2],
                state_dir=Path(tmpdir) / "1",
                heartbeat_interval=0.01,
            )
            
            node2 = RaftNode(
                node_id=2,
                peer_ids=[1],
                state_dir=Path(tmpdir) / "2",
            )
            
            # Setup transport
            transport = MockTransport()
            transport.add_node(node1)
            transport.add_node(node2)
            
            # Make node1 leader
            node1.state_machine.become_leader()
            
            await node1.start()
            await node2.start()
            
            # Wait for heartbeats
            await asyncio.sleep(0.2)
            
            # Node2 should recognize node1 as leader
            assert node2.current_leader == 1
            assert node2.state_machine.state == RaftState.FOLLOWER
            
            await node1.stop()
            await node2.stop()
    
    async def test_log_replication(self):
        """Test log replication from leader to followers."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create three nodes
            nodes = []
            for i in range(1, 4):
                node = RaftNode(
                    node_id=i,
                    peer_ids=[j for j in range(1, 4) if j != i],
                    state_dir=Path(tmpdir) / str(i),
                    heartbeat_interval=0.01,
                )
                nodes.append(node)
            
            # Setup transport
            transport = MockTransport()
            for node in nodes:
                transport.add_node(node)
            
            # Make node1 leader
            nodes[0].state_machine.become_leader()
            
            # Start all nodes
            for node in nodes:
                await node.start()
            
            # Leader proposes commands
            await nodes[0].propose("command1", {"data": "test1"})
            await nodes[0].propose("command2", {"data": "test2"})
            
            # Wait for replication
            await asyncio.sleep(0.3)
            
            # All nodes should have the commands
            for node in nodes:
                assert node.state_machine.get_last_log_index() >= 2
                entry1 = node.state_machine.get_log_entry(1)
                assert entry1.command == "command1"
            
            # Cleanup
            for node in nodes:
                await node.stop()
    
    async def test_commit_after_majority_replication(self):
        """Test commit index advances after majority replication."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create three nodes
            nodes = []
            for i in range(1, 4):
                node = RaftNode(
                    node_id=i,
                    peer_ids=[j for j in range(1, 4) if j != i],
                    state_dir=Path(tmpdir) / str(i),
                    heartbeat_interval=0.01,
                )
                nodes.append(node)
            
            # Setup transport
            transport = MockTransport()
            for node in nodes:
                transport.add_node(node)
            
            # Track applied entries
            applied_entries = []
            
            async def apply_callback(entry: LogEntry):
                applied_entries.append(entry)
            
            nodes[0].apply_callback = apply_callback
            
            # Make node1 leader
            nodes[0].state_machine.become_leader()
            
            # Start all nodes
            for node in nodes:
                await node.start()
            
            # Leader proposes command
            await nodes[0].propose("test_command")
            
            # Wait for replication and commit
            await asyncio.sleep(0.3)
            
            # Command should be committed and applied
            assert nodes[0].state_machine.volatile.commit_index >= 1
            assert len(applied_entries) >= 1
            assert applied_entries[0].command == "test_command"
            
            # Cleanup
            for node in nodes:
                await node.stop()
    
    async def test_network_partition(self):
        """Test behavior during network partition."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create three nodes
            nodes = []
            for i in range(1, 4):
                node = RaftNode(
                    node_id=i,
                    peer_ids=[j for j in range(1, 4) if j != i],
                    state_dir=Path(tmpdir) / str(i),
                    heartbeat_interval=0.01,
                )
                nodes.append(node)
            
            # Setup transport
            transport = MockTransport()
            for node in nodes:
                transport.add_node(node)
            
            # Make node1 leader
            nodes[0].state_machine.become_leader()
            
            # Start all nodes
            for node in nodes:
                await node.start()
            
            # Wait for stable state
            await asyncio.sleep(0.2)
            
            # Partition node1 from others
            transport.partition(1, 2)
            transport.partition(1, 3)
            
            # Wait for new election
            await asyncio.sleep(0.5)
            
            # Node2 or node3 should become leader
            assert not nodes[0].is_leader()
            assert nodes[1].is_leader() or nodes[2].is_leader()
            
            # Cleanup
            for node in nodes:
                await node.stop()
    
    async def test_handle_request_vote(self):
        """Test RequestVote RPC handling."""
        with tempfile.TemporaryDirectory() as tmpdir:
            node = RaftNode(
                node_id=1,
                peer_ids=[2, 3],
                state_dir=Path(tmpdir),
            )
            
            # Request vote
            request = RequestVoteRequest(
                term=5,
                candidate_id=2,
                last_log_index=0,
                last_log_term=0,
            )
            
            response = await node.handle_request_vote(request)
            
            # Should grant vote
            assert response.vote_granted
            assert response.term == 5
            assert node.state_machine.persistent.voted_for == 2
    
    async def test_reject_vote_already_voted(self):
        """Test rejecting vote when already voted."""
        with tempfile.TemporaryDirectory() as tmpdir:
            node = RaftNode(
                node_id=1,
                peer_ids=[2, 3],
                state_dir=Path(tmpdir),
            )
            
            # Vote for candidate 2
            request1 = RequestVoteRequest(
                term=5,
                candidate_id=2,
                last_log_index=0,
                last_log_term=0,
            )
            
            await node.handle_request_vote(request1)
            
            # Try to vote for candidate 3
            request2 = RequestVoteRequest(
                term=5,
                candidate_id=3,
                last_log_index=0,
                last_log_term=0,
            )
            
            response = await node.handle_request_vote(request2)
            
            # Should reject vote
            assert not response.vote_granted
    
    async def test_handle_append_entries(self):
        """Test AppendEntries RPC handling."""
        with tempfile.TemporaryDirectory() as tmpdir:
            node = RaftNode(
                node_id=1,
                peer_ids=[2],
                state_dir=Path(tmpdir),
            )
            
            # Receive heartbeat
            request = AppendEntriesRequest(
                term=5,
                leader_id=2,
                prev_log_index=0,
                prev_log_term=0,
                entries=[],
                leader_commit=0,
            )
            
            response = await node.handle_append_entries(request)
            
            # Should accept
            assert response.success
            assert node.current_leader == 2
    
    async def test_reject_old_term_append_entries(self):
        """Test rejecting AppendEntries from old term."""
        with tempfile.TemporaryDirectory() as tmpdir:
            node = RaftNode(
                node_id=1,
                peer_ids=[2],
                state_dir=Path(tmpdir),
            )
            
            # Set current term to 5
            node.state_machine.persistent.current_term = 5
            
            # Receive append entries from old term
            request = AppendEntriesRequest(
                term=3,
                leader_id=2,
                prev_log_index=0,
                prev_log_term=0,
                entries=[],
                leader_commit=0,
            )
            
            response = await node.handle_append_entries(request)
            
            # Should reject
            assert not response.success


class TestRaftIntegration:
    """Integration tests for Raft."""
    
    @pytest.mark.asyncio
    async def test_five_node_cluster(self):
        """Test five-node cluster election and replication."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create five nodes
            nodes = []
            for i in range(1, 6):
                node = RaftNode(
                    node_id=i,
                    peer_ids=[j for j in range(1, 6) if j != i],
                    state_dir=Path(tmpdir) / str(i),
                    heartbeat_interval=0.01,
                )
                nodes.append(node)
            
            # Setup transport
            transport = MockTransport()
            for node in nodes:
                transport.add_node(node)
            
            # Start all nodes
            for node in nodes:
                await node.start()
            
            # Trigger election
            nodes[0].state_machine.last_heartbeat_time = 0
            
            # Wait for election
            await asyncio.sleep(0.6)
            
            # One leader should emerge
            leaders = [n for n in nodes if n.is_leader()]
            assert len(leaders) == 1
            
            leader = leaders[0]
            
            # Propose commands
            for i in range(10):
                await leader.propose(f"cmd{i}")
            
            # Wait for replication
            await asyncio.sleep(0.5)
            
            # All nodes should have the commands
            for node in nodes:
                assert node.state_machine.get_last_log_index() >= 10
            
            # Cleanup
            for node in nodes:
                await node.stop()
