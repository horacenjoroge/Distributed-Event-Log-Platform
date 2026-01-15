"""
Raft consensus node implementation.

Implements leader election, log replication, and commit logic.
"""

import asyncio
import time
from pathlib import Path
from typing import Callable, Dict, List, Optional

from distributedlog.consensus.raft.rpc import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    RequestVoteRequest,
    RequestVoteResponse,
)
from distributedlog.consensus.raft.state import LogEntry, RaftState, RaftStateMachine
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class RaftNode:
    """
    Raft consensus node.
    
    Implements the Raft consensus algorithm for distributed agreement.
    """
    
    def __init__(
        self,
        node_id: int,
        peer_ids: List[int],
        state_dir: Optional[Path] = None,
        heartbeat_interval: float = 0.05,  # 50ms
    ):
        """
        Initialize Raft node.
        
        Args:
            node_id: This node's ID
            peer_ids: IDs of peer nodes
            state_dir: Directory for persistent state
            heartbeat_interval: Leader heartbeat interval in seconds
        """
        self.node_id = node_id
        self.peer_ids = peer_ids
        self.heartbeat_interval = heartbeat_interval
        
        # State machine
        self.state_machine = RaftStateMachine(
            node_id=node_id,
            peer_ids=peer_ids,
            state_dir=state_dir,
        )
        
        # Current leader (if known)
        self.current_leader: Optional[int] = None
        
        # RPC handlers (set by external transport layer)
        self.send_request_vote: Optional[Callable] = None
        self.send_append_entries: Optional[Callable] = None
        
        # State machine application callback
        self.apply_callback: Optional[Callable] = None
        
        # Background tasks
        self._election_task: Optional[asyncio.Task] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._running = False
        
        logger.info(
            "RaftNode initialized",
            node_id=node_id,
            peers=peer_ids,
        )
    
    async def start(self) -> None:
        """Start Raft node."""
        if self._running:
            return
        
        self._running = True
        
        # Start election timer
        self._election_task = asyncio.create_task(self._election_loop())
        
        logger.info("RaftNode started", node_id=self.node_id)
    
    async def stop(self) -> None:
        """Stop Raft node."""
        self._running = False
        
        if self._election_task:
            self._election_task.cancel()
            try:
                await self._election_task
            except asyncio.CancelledError:
                pass
        
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        
        logger.info("RaftNode stopped", node_id=self.node_id)
    
    async def _election_loop(self) -> None:
        """Election timer loop."""
        while self._running:
            try:
                # Check if election timeout elapsed
                if self.state_machine.is_election_timeout():
                    if self.state_machine.state != RaftState.LEADER:
                        # Start election
                        await self._start_election()
                
                # Check every 10ms
                await asyncio.sleep(0.01)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    "Error in election loop",
                    node_id=self.node_id,
                    error=str(e),
                )
                await asyncio.sleep(0.1)
    
    async def _start_election(self) -> None:
        """Start leader election."""
        # Become candidate
        self.state_machine.become_candidate()
        
        logger.info(
            "Starting election",
            node_id=self.node_id,
            term=self.state_machine.persistent.current_term,
        )
        
        # Request votes from all peers
        votes_received = 1  # Vote for self
        votes_needed = (len(self.peer_ids) + 1) // 2 + 1  # Majority
        
        # Send RequestVote RPCs
        request = RequestVoteRequest(
            term=self.state_machine.persistent.current_term,
            candidate_id=self.node_id,
            last_log_index=self.state_machine.get_last_log_index(),
            last_log_term=self.state_machine.get_last_log_term(),
        )
        
        # Collect votes concurrently
        vote_tasks = []
        for peer_id in self.peer_ids:
            if self.send_request_vote:
                task = asyncio.create_task(
                    self._request_vote_from_peer(peer_id, request)
                )
                vote_tasks.append(task)
        
        if vote_tasks:
            # Wait for responses with timeout
            try:
                responses = await asyncio.wait_for(
                    asyncio.gather(*vote_tasks, return_exceptions=True),
                    timeout=self.state_machine.election_timeout,
                )
                
                # Count votes
                for response in responses:
                    if isinstance(response, RequestVoteResponse):
                        if response.vote_granted:
                            votes_received += 1
                        
                        # Update term if higher
                        if response.term > self.state_machine.persistent.current_term:
                            self.state_machine.become_follower(response.term)
                            return
                
            except asyncio.TimeoutError:
                logger.warning(
                    "Election timeout",
                    node_id=self.node_id,
                    term=self.state_machine.persistent.current_term,
                )
        
        # Check if won election
        if votes_received >= votes_needed:
            if self.state_machine.state == RaftState.CANDIDATE:
                await self._become_leader()
        else:
            logger.info(
                "Election failed",
                node_id=self.node_id,
                votes_received=votes_received,
                votes_needed=votes_needed,
            )
    
    async def _request_vote_from_peer(
        self,
        peer_id: int,
        request: RequestVoteRequest,
    ) -> Optional[RequestVoteResponse]:
        """
        Request vote from a peer.
        
        Args:
            peer_id: Peer node ID
            request: Vote request
        
        Returns:
            Vote response or None
        """
        if not self.send_request_vote:
            return None
        
        try:
            response = await self.send_request_vote(peer_id, request)
            return response
        except Exception as e:
            logger.debug(
                "Failed to request vote from peer",
                peer_id=peer_id,
                error=str(e),
            )
            return None
    
    async def _become_leader(self) -> None:
        """Become leader after winning election."""
        self.state_machine.become_leader()
        self.current_leader = self.node_id
        
        logger.info(
            "Won election - became leader",
            node_id=self.node_id,
            term=self.state_machine.persistent.current_term,
        )
        
        # Start heartbeat task
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        
        # Send initial heartbeat immediately
        await self._send_heartbeats()
    
    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeats to followers."""
        while self._running and self.state_machine.state == RaftState.LEADER:
            try:
                await self._send_heartbeats()
                await asyncio.sleep(self.heartbeat_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    "Error in heartbeat loop",
                    node_id=self.node_id,
                    error=str(e),
                )
                await asyncio.sleep(self.heartbeat_interval)
    
    async def _send_heartbeats(self) -> None:
        """Send heartbeat/log replication to all followers."""
        if self.state_machine.state != RaftState.LEADER:
            return
        
        tasks = []
        for peer_id in self.peer_ids:
            task = asyncio.create_task(self._replicate_to_follower(peer_id))
            tasks.append(task)
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        
        # Update commit index
        self._update_commit_index()
        
        # Apply committed entries
        await self._apply_committed_entries()
    
    async def _replicate_to_follower(self, peer_id: int) -> None:
        """
        Replicate log entries to a follower.
        
        Args:
            peer_id: Follower node ID
        """
        if not self.send_append_entries:
            return
        
        if not self.state_machine.leader_state:
            return
        
        # Get next index to send
        next_index = self.state_machine.leader_state.next_index[peer_id]
        prev_log_index = next_index - 1
        
        # Get prev log term
        prev_log_term = 0
        if prev_log_index > 0:
            prev_entry = self.state_machine.get_log_entry(prev_log_index)
            if prev_entry:
                prev_log_term = prev_entry.term
        
        # Get entries to send
        entries = []
        last_log_index = self.state_machine.get_last_log_index()
        if next_index <= last_log_index:
            for i in range(next_index, last_log_index + 1):
                entry = self.state_machine.get_log_entry(i)
                if entry:
                    entries.append(entry)
        
        # Create request
        request = AppendEntriesRequest(
            term=self.state_machine.persistent.current_term,
            leader_id=self.node_id,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries,
            leader_commit=self.state_machine.volatile.commit_index,
        )
        
        try:
            response = await self.send_append_entries(peer_id, request)
            
            if response:
                await self._handle_append_entries_response(peer_id, response)
                
        except Exception as e:
            logger.debug(
                "Failed to replicate to follower",
                peer_id=peer_id,
                error=str(e),
            )
    
    async def _handle_append_entries_response(
        self,
        peer_id: int,
        response: AppendEntriesResponse,
    ) -> None:
        """
        Handle AppendEntries response from follower.
        
        Args:
            peer_id: Follower node ID
            response: Response from follower
        """
        # Check if we're still leader
        if self.state_machine.state != RaftState.LEADER:
            return
        
        if not self.state_machine.leader_state:
            return
        
        # Update term if higher
        if response.term > self.state_machine.persistent.current_term:
            self.state_machine.become_follower(response.term)
            return
        
        if response.success:
            # Update next_index and match_index
            if response.match_index > 0:
                self.state_machine.leader_state.match_index[peer_id] = response.match_index
                self.state_machine.leader_state.next_index[peer_id] = response.match_index + 1
                
                logger.debug(
                    "Follower replicated successfully",
                    peer_id=peer_id,
                    match_index=response.match_index,
                )
        else:
            # Decrement next_index and retry
            self.state_machine.leader_state.next_index[peer_id] = max(
                1,
                self.state_machine.leader_state.next_index[peer_id] - 1,
            )
            
            logger.debug(
                "Follower replication failed, decrementing next_index",
                peer_id=peer_id,
                next_index=self.state_machine.leader_state.next_index[peer_id],
            )
    
    def _update_commit_index(self) -> None:
        """Update commit index based on majority replication."""
        if self.state_machine.state != RaftState.LEADER:
            return
        
        if not self.state_machine.leader_state:
            return
        
        # Find highest index replicated on majority
        match_indices = list(self.state_machine.leader_state.match_index.values())
        match_indices.append(self.state_machine.get_last_log_index())  # Include self
        match_indices.sort()
        
        # Median is the majority index
        majority_index = match_indices[len(match_indices) // 2]
        
        # Only commit entries from current term
        if majority_index > self.state_machine.volatile.commit_index:
            entry = self.state_machine.get_log_entry(majority_index)
            if entry and entry.term == self.state_machine.persistent.current_term:
                old_commit = self.state_machine.volatile.commit_index
                self.state_machine.volatile.commit_index = majority_index
                
                logger.info(
                    "Advanced commit index",
                    old_commit=old_commit,
                    new_commit=majority_index,
                )
    
    async def _apply_committed_entries(self) -> None:
        """Apply committed log entries to state machine."""
        while self.state_machine.volatile.last_applied < self.state_machine.volatile.commit_index:
            self.state_machine.volatile.last_applied += 1
            
            entry = self.state_machine.get_log_entry(
                self.state_machine.volatile.last_applied
            )
            
            if entry and self.apply_callback:
                try:
                    await self.apply_callback(entry)
                    
                    logger.debug(
                        "Applied log entry",
                        index=entry.index,
                        command=entry.command,
                    )
                except Exception as e:
                    logger.error(
                        "Failed to apply log entry",
                        index=entry.index,
                        error=str(e),
                    )
    
    # RPC Handlers
    
    async def handle_request_vote(
        self,
        request: RequestVoteRequest,
    ) -> RequestVoteResponse:
        """
        Handle RequestVote RPC.
        
        Args:
            request: Vote request from candidate
        
        Returns:
            Vote response
        """
        # Update term if higher
        if request.term > self.state_machine.persistent.current_term:
            self.state_machine.become_follower(request.term)
        
        vote_granted = False
        
        # Check if we can vote for this candidate
        if request.term == self.state_machine.persistent.current_term:
            # Haven't voted or already voted for this candidate
            if (self.state_machine.persistent.voted_for is None or
                self.state_machine.persistent.voted_for == request.candidate_id):
                
                # Candidate's log is at least as up-to-date as ours
                if self._is_log_up_to_date(
                    request.last_log_index,
                    request.last_log_term,
                ):
                    vote_granted = True
                    self.state_machine.persistent.voted_for = request.candidate_id
                    self.state_machine._save_state()
                    self.state_machine.reset_election_timer()
                    
                    logger.info(
                        "Granted vote",
                        candidate_id=request.candidate_id,
                        term=request.term,
                    )
        
        return RequestVoteResponse(
            term=self.state_machine.persistent.current_term,
            vote_granted=vote_granted,
        )
    
    def _is_log_up_to_date(self, candidate_last_index: int, candidate_last_term: int) -> bool:
        """
        Check if candidate's log is at least as up-to-date as ours.
        
        Args:
            candidate_last_index: Candidate's last log index
            candidate_last_term: Candidate's last log term
        
        Returns:
            True if candidate's log is up-to-date
        """
        our_last_term = self.state_machine.get_last_log_term()
        our_last_index = self.state_machine.get_last_log_index()
        
        # Compare terms first
        if candidate_last_term != our_last_term:
            return candidate_last_term > our_last_term
        
        # Terms equal, compare indices
        return candidate_last_index >= our_last_index
    
    async def handle_append_entries(
        self,
        request: AppendEntriesRequest,
    ) -> AppendEntriesResponse:
        """
        Handle AppendEntries RPC.
        
        Args:
            request: Append entries request from leader
        
        Returns:
            Append entries response
        """
        # Update term if higher
        if request.term > self.state_machine.persistent.current_term:
            self.state_machine.become_follower(request.term)
        
        # Reject if term is old
        if request.term < self.state_machine.persistent.current_term:
            return AppendEntriesResponse(
                term=self.state_machine.persistent.current_term,
                success=False,
            )
        
        # Valid leader, reset election timer
        self.state_machine.reset_election_timer()
        self.current_leader = request.leader_id
        
        # Become follower if not already
        if self.state_machine.state != RaftState.FOLLOWER:
            self.state_machine.become_follower(request.term)
        
        # Check prev log entry matches
        if request.prev_log_index > 0:
            prev_entry = self.state_machine.get_log_entry(request.prev_log_index)
            
            if not prev_entry or prev_entry.term != request.prev_log_term:
                # Log doesn't match
                return AppendEntriesResponse(
                    term=self.state_machine.persistent.current_term,
                    success=False,
                )
        
        # Append entries
        for i, entry in enumerate(request.entries):
            index = request.prev_log_index + i + 1
            
            existing_entry = self.state_machine.get_log_entry(index)
            
            if existing_entry:
                # Conflict - delete this and all following entries
                if existing_entry.term != entry.term:
                    self.state_machine.delete_entries_from(index)
                    self.state_machine.persistent.log.append(entry)
            else:
                # Append new entry
                self.state_machine.persistent.log.append(entry)
        
        if request.entries:
            self.state_machine._save_state()
        
        # Update commit index
        if request.leader_commit > self.state_machine.volatile.commit_index:
            self.state_machine.volatile.commit_index = min(
                request.leader_commit,
                self.state_machine.get_last_log_index(),
            )
            
            # Apply committed entries
            await self._apply_committed_entries()
        
        return AppendEntriesResponse(
            term=self.state_machine.persistent.current_term,
            success=True,
            match_index=self.state_machine.get_last_log_index(),
        )
    
    # Client API
    
    async def propose(self, command: str, data: Optional[Dict] = None) -> bool:
        """
        Propose a command to the cluster.
        
        Args:
            command: Command to execute
            data: Command data
        
        Returns:
            True if successfully proposed (leader only)
        """
        if self.state_machine.state != RaftState.LEADER:
            return False
        
        # Append to log
        entry = self.state_machine.append_log_entry(command, data)
        
        logger.info(
            "Proposed command",
            command=command,
            index=entry.index,
            term=entry.term,
        )
        
        # Will be replicated in next heartbeat
        return True
    
    def get_leader(self) -> Optional[int]:
        """
        Get current leader ID.
        
        Returns:
            Leader ID or None
        """
        if self.state_machine.state == RaftState.LEADER:
            return self.node_id
        
        return self.current_leader
    
    def is_leader(self) -> bool:
        """
        Check if this node is the leader.
        
        Returns:
            True if leader
        """
        return self.state_machine.state == RaftState.LEADER
