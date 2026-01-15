"""
Tests for Raft state machine and persistent state.
"""

import tempfile
from pathlib import Path

import pytest

from distributedlog.consensus.raft.state import (
    LogEntry,
    PersistentState,
    RaftState,
    RaftStateMachine,
)


class TestLogEntry:
    """Test LogEntry."""
    
    def test_create_entry(self):
        """Test creating log entry."""
        entry = LogEntry(
            term=1,
            index=5,
            command="assign_partition",
            data={"topic": "test", "partition": 0, "broker": 1},
        )
        
        assert entry.term == 1
        assert entry.index == 5
        assert entry.command == "assign_partition"
        assert entry.data["broker"] == 1
    
    def test_serialize_deserialize(self):
        """Test serialization round-trip."""
        entry = LogEntry(
            term=2,
            index=10,
            command="test_command",
            data={"key": "value"},
        )
        
        entry_dict = entry.to_dict()
        restored = LogEntry.from_dict(entry_dict)
        
        assert restored.term == entry.term
        assert restored.index == entry.index
        assert restored.command == entry.command
        assert restored.data == entry.data


class TestPersistentState:
    """Test PersistentState."""
    
    def test_create_state(self):
        """Test creating persistent state."""
        state = PersistentState()
        
        assert state.current_term == 0
        assert state.voted_for is None
        assert len(state.log) == 0
    
    def test_save_and_load(self):
        """Test saving and loading state."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "state.json"
            
            # Create and save state
            state = PersistentState(
                current_term=5,
                voted_for=2,
            )
            state.log.append(LogEntry(term=1, index=1, command="cmd1"))
            state.log.append(LogEntry(term=2, index=2, command="cmd2"))
            
            state.save(path)
            
            # Load state
            loaded = PersistentState.load(path)
            
            assert loaded.current_term == 5
            assert loaded.voted_for == 2
            assert len(loaded.log) == 2
            assert loaded.log[0].command == "cmd1"
            assert loaded.log[1].command == "cmd2"
    
    def test_load_nonexistent(self):
        """Test loading non-existent file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "nonexistent.json"
            
            state = PersistentState.load(path)
            
            assert state.current_term == 0
            assert state.voted_for is None


class TestRaftStateMachine:
    """Test RaftStateMachine."""
    
    def test_initialize(self):
        """Test initialization."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sm = RaftStateMachine(
                node_id=1,
                peer_ids=[2, 3],
                state_dir=Path(tmpdir),
            )
            
            assert sm.node_id == 1
            assert sm.peer_ids == [2, 3]
            assert sm.state == RaftState.FOLLOWER
            assert sm.persistent.current_term == 0
    
    def test_become_follower(self):
        """Test transition to follower."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sm = RaftStateMachine(1, [2, 3], Path(tmpdir))
            
            sm.become_follower(5)
            
            assert sm.state == RaftState.FOLLOWER
            assert sm.persistent.current_term == 5
            assert sm.persistent.voted_for is None
    
    def test_become_candidate(self):
        """Test transition to candidate."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sm = RaftStateMachine(1, [2, 3], Path(tmpdir))
            
            sm.become_candidate()
            
            assert sm.state == RaftState.CANDIDATE
            assert sm.persistent.current_term == 1
            assert sm.persistent.voted_for == 1
    
    def test_become_leader(self):
        """Test transition to leader."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sm = RaftStateMachine(1, [2, 3], Path(tmpdir))
            
            sm.become_leader()
            
            assert sm.state == RaftState.LEADER
            assert sm.leader_state is not None
            assert 2 in sm.leader_state.next_index
            assert 3 in sm.leader_state.next_index
    
    def test_election_timeout(self):
        """Test election timeout."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sm = RaftStateMachine(1, [2, 3], Path(tmpdir))
            
            # Initially not timed out
            assert not sm.is_election_timeout()
            
            # Manually expire timeout
            sm.last_heartbeat_time = 0
            
            assert sm.is_election_timeout()
    
    def test_append_log_entry(self):
        """Test appending log entry."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sm = RaftStateMachine(1, [2, 3], Path(tmpdir))
            sm.persistent.current_term = 5
            
            entry = sm.append_log_entry("test_command", {"key": "value"})
            
            assert entry.term == 5
            assert entry.index == 1
            assert entry.command == "test_command"
            assert len(sm.persistent.log) == 1
    
    def test_get_log_entry(self):
        """Test getting log entry."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sm = RaftStateMachine(1, [2, 3], Path(tmpdir))
            
            sm.append_log_entry("cmd1")
            sm.append_log_entry("cmd2")
            
            entry1 = sm.get_log_entry(1)
            entry2 = sm.get_log_entry(2)
            entry3 = sm.get_log_entry(3)
            
            assert entry1.command == "cmd1"
            assert entry2.command == "cmd2"
            assert entry3 is None
    
    def test_get_last_log_index(self):
        """Test getting last log index."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sm = RaftStateMachine(1, [2, 3], Path(tmpdir))
            
            assert sm.get_last_log_index() == 0
            
            sm.append_log_entry("cmd1")
            assert sm.get_last_log_index() == 1
            
            sm.append_log_entry("cmd2")
            assert sm.get_last_log_index() == 2
    
    def test_get_last_log_term(self):
        """Test getting last log term."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sm = RaftStateMachine(1, [2, 3], Path(tmpdir))
            
            assert sm.get_last_log_term() == 0
            
            sm.persistent.current_term = 5
            sm.append_log_entry("cmd1")
            
            assert sm.get_last_log_term() == 5
    
    def test_delete_entries_from(self):
        """Test deleting log entries."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sm = RaftStateMachine(1, [2, 3], Path(tmpdir))
            
            sm.append_log_entry("cmd1")
            sm.append_log_entry("cmd2")
            sm.append_log_entry("cmd3")
            
            assert sm.get_last_log_index() == 3
            
            sm.delete_entries_from(2)
            
            assert sm.get_last_log_index() == 1
            assert sm.get_log_entry(1).command == "cmd1"
            assert sm.get_log_entry(2) is None
    
    def test_persistent_state_survives_restart(self):
        """Test persistent state survives restart."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            
            # Create state machine and add entries
            sm1 = RaftStateMachine(1, [2, 3], state_dir)
            sm1.persistent.current_term = 5
            sm1.persistent.voted_for = 2
            sm1.append_log_entry("cmd1")
            sm1.append_log_entry("cmd2")
            
            # Create new state machine (simulates restart)
            sm2 = RaftStateMachine(1, [2, 3], state_dir)
            
            # State should be restored
            assert sm2.persistent.current_term == 5
            assert sm2.persistent.voted_for == 2
            assert sm2.get_last_log_index() == 2
            assert sm2.get_log_entry(1).command == "cmd1"
