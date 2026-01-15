"""
Raft state machine and persistent state.

Implements the three Raft states: Follower, Candidate, Leader.
"""

import json
import time
from dataclasses import asdict, dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class RaftState(str, Enum):
    """Raft node states."""
    
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


@dataclass
class LogEntry:
    """
    Single entry in the Raft log.
    
    Note: This is NOT the commit log - this is Raft's internal log
    for consensus on cluster operations.
    
    Attributes:
        term: Term when entry was received by leader
        index: Position in log (1-indexed)
        command: State machine command (e.g., "assign partition X to broker Y")
        data: Additional command data
    """
    term: int
    index: int
    command: str
    data: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "LogEntry":
        """Deserialize from dictionary."""
        return cls(**d)


@dataclass
class PersistentState:
    """
    Persistent Raft state (must survive crashes).
    
    Attributes:
        current_term: Latest term server has seen
        voted_for: Candidate ID that received vote in current term
        log: Log entries (command and term for each entry)
    """
    current_term: int = 0
    voted_for: Optional[int] = None
    log: List[LogEntry] = None
    
    def __post_init__(self):
        if self.log is None:
            self.log = []
    
    def save(self, path: Path) -> None:
        """
        Save state to disk.
        
        Args:
            path: File path to save to
        """
        state_dict = {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "log": [entry.to_dict() for entry in self.log],
        }
        
        # Write atomically
        tmp_path = path.with_suffix(".tmp")
        with open(tmp_path, "w") as f:
            json.dump(state_dict, f, indent=2)
        
        tmp_path.rename(path)
        
        logger.debug(
            "Saved persistent state",
            term=self.current_term,
            voted_for=self.voted_for,
            log_size=len(self.log),
        )
    
    @classmethod
    def load(cls, path: Path) -> "PersistentState":
        """
        Load state from disk.
        
        Args:
            path: File path to load from
        
        Returns:
            Persistent state
        """
        if not path.exists():
            return cls()
        
        with open(path) as f:
            state_dict = json.load(f)
        
        log_entries = [
            LogEntry.from_dict(entry_dict)
            for entry_dict in state_dict.get("log", [])
        ]
        
        state = cls(
            current_term=state_dict.get("current_term", 0),
            voted_for=state_dict.get("voted_for"),
            log=log_entries,
        )
        
        logger.info(
            "Loaded persistent state",
            term=state.current_term,
            voted_for=state.voted_for,
            log_size=len(state.log),
        )
        
        return state


@dataclass
class VolatileState:
    """
    Volatile Raft state (recomputed on restart).
    
    All servers maintain:
    - commit_index: Index of highest log entry known to be committed
    - last_applied: Index of highest log entry applied to state machine
    """
    commit_index: int = 0
    last_applied: int = 0


@dataclass
class LeaderState:
    """
    Volatile state maintained only on leaders.
    
    Reinitialized after election.
    
    Attributes:
        next_index: For each server, index of next log entry to send
        match_index: For each server, index of highest log entry known to be replicated
    """
    next_index: Dict[int, int]
    match_index: Dict[int, int]
    
    @classmethod
    def initialize(cls, peer_ids: List[int], last_log_index: int) -> "LeaderState":
        """
        Initialize leader state after election.
        
        Args:
            peer_ids: IDs of peer nodes
            last_log_index: Index of last log entry
        
        Returns:
            Initialized leader state
        """
        # Initialize next_index to last log index + 1
        next_index = {peer_id: last_log_index + 1 for peer_id in peer_ids}
        
        # Initialize match_index to 0
        match_index = {peer_id: 0 for peer_id in peer_ids}
        
        return cls(next_index=next_index, match_index=match_index)


class RaftStateMachine:
    """
    Raft state machine managing state transitions.
    
    Handles transitions between Follower, Candidate, and Leader states.
    """
    
    def __init__(
        self,
        node_id: int,
        peer_ids: List[int],
        state_dir: Optional[Path] = None,
    ):
        """
        Initialize Raft state machine.
        
        Args:
            node_id: This node's ID
            peer_ids: IDs of peer nodes
            state_dir: Directory for persistent state
        """
        self.node_id = node_id
        self.peer_ids = peer_ids
        self.state_dir = state_dir or Path("/tmp/raft") / str(node_id)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        
        # Current state
        self.state = RaftState.FOLLOWER
        
        # Persistent state
        state_path = self.state_dir / "state.json"
        self.persistent = PersistentState.load(state_path)
        
        # Volatile state
        self.volatile = VolatileState()
        
        # Leader state (only when leader)
        self.leader_state: Optional[LeaderState] = None
        
        # Election timing
        self.last_heartbeat_time = time.time()
        self.election_timeout = self._random_election_timeout()
        
        logger.info(
            "RaftStateMachine initialized",
            node_id=node_id,
            peer_ids=peer_ids,
            state=self.state,
            term=self.persistent.current_term,
        )
    
    def _random_election_timeout(self) -> float:
        """
        Generate random election timeout.
        
        Randomness prevents split votes.
        
        Returns:
            Timeout in seconds (between 150ms and 300ms)
        """
        import random
        return random.uniform(0.15, 0.30)
    
    def become_follower(self, term: int) -> None:
        """
        Transition to follower state.
        
        Args:
            term: New term number
        """
        if term > self.persistent.current_term:
            self.persistent.current_term = term
            self.persistent.voted_for = None
            self._save_state()
        
        self.state = RaftState.FOLLOWER
        self.leader_state = None
        self.reset_election_timer()
        
        logger.info(
            "Became follower",
            node_id=self.node_id,
            term=self.persistent.current_term,
        )
    
    def become_candidate(self) -> None:
        """Transition to candidate state."""
        self.state = RaftState.CANDIDATE
        
        # Increment term
        self.persistent.current_term += 1
        
        # Vote for self
        self.persistent.voted_for = self.node_id
        
        self._save_state()
        self.reset_election_timer()
        
        logger.info(
            "Became candidate",
            node_id=self.node_id,
            term=self.persistent.current_term,
        )
    
    def become_leader(self) -> None:
        """Transition to leader state."""
        self.state = RaftState.LEADER
        
        # Initialize leader state
        last_log_index = self.get_last_log_index()
        self.leader_state = LeaderState.initialize(
            peer_ids=self.peer_ids,
            last_log_index=last_log_index,
        )
        
        logger.info(
            "Became leader",
            node_id=self.node_id,
            term=self.persistent.current_term,
        )
    
    def reset_election_timer(self) -> None:
        """Reset election timeout."""
        self.last_heartbeat_time = time.time()
        self.election_timeout = self._random_election_timeout()
    
    def is_election_timeout(self) -> bool:
        """
        Check if election timeout has elapsed.
        
        Returns:
            True if timeout elapsed
        """
        elapsed = time.time() - self.last_heartbeat_time
        return elapsed > self.election_timeout
    
    def append_log_entry(self, command: str, data: Optional[Dict] = None) -> LogEntry:
        """
        Append entry to log.
        
        Args:
            command: Command to append
            data: Additional command data
        
        Returns:
            Appended log entry
        """
        index = self.get_last_log_index() + 1
        
        entry = LogEntry(
            term=self.persistent.current_term,
            index=index,
            command=command,
            data=data,
        )
        
        self.persistent.log.append(entry)
        self._save_state()
        
        logger.debug(
            "Appended log entry",
            index=index,
            term=entry.term,
            command=command,
        )
        
        return entry
    
    def get_log_entry(self, index: int) -> Optional[LogEntry]:
        """
        Get log entry at index.
        
        Args:
            index: Log index (1-indexed)
        
        Returns:
            Log entry or None
        """
        if index < 1 or index > len(self.persistent.log):
            return None
        
        return self.persistent.log[index - 1]
    
    def get_last_log_index(self) -> int:
        """
        Get index of last log entry.
        
        Returns:
            Last log index (0 if empty)
        """
        return len(self.persistent.log)
    
    def get_last_log_term(self) -> int:
        """
        Get term of last log entry.
        
        Returns:
            Last log term (0 if empty)
        """
        if not self.persistent.log:
            return 0
        
        return self.persistent.log[-1].term
    
    def delete_entries_from(self, index: int) -> None:
        """
        Delete log entries from index onwards.
        
        Used to handle log conflicts.
        
        Args:
            index: Starting index (1-indexed)
        """
        if index < 1:
            return
        
        self.persistent.log = self.persistent.log[:index - 1]
        self._save_state()
        
        logger.warning(
            "Deleted log entries",
            from_index=index,
            new_last_index=self.get_last_log_index(),
        )
    
    def _save_state(self) -> None:
        """Save persistent state to disk."""
        state_path = self.state_dir / "state.json"
        self.persistent.save(state_path)
