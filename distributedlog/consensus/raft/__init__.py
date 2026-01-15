"""
Raft consensus implementation.

Implements the Raft consensus algorithm for distributed coordination.
"""

from distributedlog.consensus.raft.node import RaftNode
from distributedlog.consensus.raft.rpc import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    RequestVoteRequest,
    RequestVoteResponse,
)
from distributedlog.consensus.raft.state import (
    LogEntry,
    PersistentState,
    RaftState,
    RaftStateMachine,
)

__all__ = [
    # Node
    "RaftNode",
    # RPC
    "RequestVoteRequest",
    "RequestVoteResponse",
    "AppendEntriesRequest",
    "AppendEntriesResponse",
    # State
    "RaftState",
    "RaftStateMachine",
    "PersistentState",
    "LogEntry",
]
