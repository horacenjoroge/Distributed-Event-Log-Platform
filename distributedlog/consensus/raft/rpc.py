"""
Raft RPC messages.

Implements RequestVote and AppendEntries RPCs.
"""

from dataclasses import dataclass
from typing import List, Optional

from distributedlog.consensus.raft.state import LogEntry


@dataclass
class RequestVoteRequest:
    """
    RequestVote RPC request.
    
    Invoked by candidates to gather votes.
    
    Attributes:
        term: Candidate's term
        candidate_id: Candidate requesting vote
        last_log_index: Index of candidate's last log entry
        last_log_term: Term of candidate's last log entry
    """
    term: int
    candidate_id: int
    last_log_index: int
    last_log_term: int


@dataclass
class RequestVoteResponse:
    """
    RequestVote RPC response.
    
    Attributes:
        term: Current term, for candidate to update itself
        vote_granted: True if candidate received vote
    """
    term: int
    vote_granted: bool


@dataclass
class AppendEntriesRequest:
    """
    AppendEntries RPC request.
    
    Invoked by leader to replicate log entries.
    Also used as heartbeat (empty entries).
    
    Attributes:
        term: Leader's term
        leader_id: Leader ID (for followers to redirect clients)
        prev_log_index: Index of log entry immediately preceding new ones
        prev_log_term: Term of prev_log_index entry
        entries: Log entries to store (empty for heartbeat)
        leader_commit: Leader's commit_index
    """
    term: int
    leader_id: int
    prev_log_index: int
    prev_log_term: int
    entries: List[LogEntry]
    leader_commit: int


@dataclass
class AppendEntriesResponse:
    """
    AppendEntries RPC response.
    
    Attributes:
        term: Current term, for leader to update itself
        success: True if follower contained entry matching prev_log_index and prev_log_term
        match_index: Index of last matching entry (for leader to update match_index)
    """
    term: int
    success: bool
    match_index: int = 0
