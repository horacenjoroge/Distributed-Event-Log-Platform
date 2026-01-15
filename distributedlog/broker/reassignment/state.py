"""
Partition reassignment state management.

Tracks reassignment progress through multiple phases.
"""

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Set

from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class ReassignmentPhase(str, Enum):
    """Phases of partition reassignment."""
    
    PENDING = "pending"              # Waiting to start
    ADDING_REPLICAS = "adding_replicas"  # Adding new replicas
    WAITING_CATCHUP = "waiting_catchup"  # Waiting for new replicas to catch up
    REMOVING_REPLICAS = "removing_replicas"  # Removing old replicas
    COMPLETED = "completed"          # Reassignment completed
    FAILED = "failed"                # Reassignment failed
    CANCELLED = "cancelled"          # Reassignment cancelled


@dataclass
class ReassignmentState:
    """
    State of a partition reassignment.
    
    Attributes:
        topic: Topic name
        partition: Partition number
        old_replicas: Original replica set
        new_replicas: Target replica set
        current_replicas: Current replica set during transition
        phase: Current reassignment phase
        start_time: Reassignment start timestamp (ms)
        end_time: Reassignment end timestamp (ms)
        error_message: Error message if failed
        progress_percent: Progress percentage (0-100)
    """
    topic: str
    partition: int
    old_replicas: List[int]
    new_replicas: List[int]
    current_replicas: List[int] = field(default_factory=list)
    phase: ReassignmentPhase = ReassignmentPhase.PENDING
    start_time: int = 0
    end_time: int = 0
    error_message: str = ""
    progress_percent: int = 0
    
    def __post_init__(self):
        if not self.current_replicas:
            self.current_replicas = self.old_replicas.copy()
        if self.start_time == 0:
            self.start_time = int(time.time() * 1000)
    
    def get_replicas_to_add(self) -> List[int]:
        """
        Get replicas to add (in new but not in old).
        
        Returns:
            List of broker IDs to add
        """
        return [r for r in self.new_replicas if r not in self.old_replicas]
    
    def get_replicas_to_remove(self) -> List[int]:
        """
        Get replicas to remove (in old but not in new).
        
        Returns:
            List of broker IDs to remove
        """
        return [r for r in self.old_replicas if r not in self.new_replicas]
    
    def is_complete(self) -> bool:
        """
        Check if reassignment is complete.
        
        Returns:
            True if completed or failed
        """
        return self.phase in (
            ReassignmentPhase.COMPLETED,
            ReassignmentPhase.FAILED,
            ReassignmentPhase.CANCELLED,
        )
    
    def duration_ms(self) -> int:
        """
        Get reassignment duration in milliseconds.
        
        Returns:
            Duration in ms
        """
        if self.end_time > 0:
            return self.end_time - self.start_time
        
        return int(time.time() * 1000) - self.start_time


@dataclass
class ReassignmentConfig:
    """
    Configuration for partition reassignment.
    
    Attributes:
        throttle_bytes_per_sec: Replication bandwidth limit
        catchup_lag_threshold: Max lag to consider caught up
        catchup_timeout_ms: Max time to wait for catch-up
        check_interval_ms: Interval between progress checks
    """
    throttle_bytes_per_sec: int = 10485760  # 10 MB/s
    catchup_lag_threshold: int = 100        # 100 messages
    catchup_timeout_ms: int = 300000        # 5 minutes
    check_interval_ms: int = 1000           # 1 second


class ReassignmentTracker:
    """
    Tracks active partition reassignments.
    
    Maintains state for all ongoing reassignments.
    """
    
    def __init__(self):
        """Initialize reassignment tracker."""
        # Active reassignments: (topic, partition) -> ReassignmentState
        self._reassignments: Dict[tuple, ReassignmentState] = {}
        
        # Completed reassignments (keep for history)
        self._completed: Dict[tuple, ReassignmentState] = {}
        
        logger.info("ReassignmentTracker initialized")
    
    def add_reassignment(
        self,
        topic: str,
        partition: int,
        old_replicas: List[int],
        new_replicas: List[int],
    ) -> ReassignmentState:
        """
        Add a new reassignment.
        
        Args:
            topic: Topic name
            partition: Partition number
            old_replicas: Current replicas
            new_replicas: Target replicas
        
        Returns:
            Created reassignment state
        """
        key = (topic, partition)
        
        if key in self._reassignments:
            logger.warning(
                "Reassignment already exists",
                topic=topic,
                partition=partition,
            )
            return self._reassignments[key]
        
        state = ReassignmentState(
            topic=topic,
            partition=partition,
            old_replicas=old_replicas,
            new_replicas=new_replicas,
        )
        
        self._reassignments[key] = state
        
        logger.info(
            "Added reassignment",
            topic=topic,
            partition=partition,
            old_replicas=old_replicas,
            new_replicas=new_replicas,
        )
        
        return state
    
    def get_reassignment(
        self,
        topic: str,
        partition: int,
    ) -> ReassignmentState:
        """
        Get reassignment state.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            Reassignment state or None
        """
        key = (topic, partition)
        
        if key in self._reassignments:
            return self._reassignments[key]
        
        if key in self._completed:
            return self._completed[key]
        
        return None
    
    def remove_reassignment(
        self,
        topic: str,
        partition: int,
    ) -> None:
        """
        Remove reassignment (move to completed).
        
        Args:
            topic: Topic name
            partition: Partition number
        """
        key = (topic, partition)
        
        if key in self._reassignments:
            state = self._reassignments[key]
            state.end_time = int(time.time() * 1000)
            
            # Move to completed
            self._completed[key] = state
            del self._reassignments[key]
            
            logger.info(
                "Completed reassignment",
                topic=topic,
                partition=partition,
                phase=state.phase,
                duration_ms=state.duration_ms(),
            )
    
    def get_active_reassignments(self) -> List[ReassignmentState]:
        """
        Get all active reassignments.
        
        Returns:
            List of active reassignment states
        """
        return list(self._reassignments.values())
    
    def get_completed_reassignments(self) -> List[ReassignmentState]:
        """
        Get completed reassignments.
        
        Returns:
            List of completed reassignment states
        """
        return list(self._completed.values())
    
    def cancel_reassignment(
        self,
        topic: str,
        partition: int,
    ) -> bool:
        """
        Cancel an active reassignment.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            True if cancelled
        """
        key = (topic, partition)
        
        if key in self._reassignments:
            state = self._reassignments[key]
            state.phase = ReassignmentPhase.CANCELLED
            state.error_message = "Cancelled by user"
            
            self.remove_reassignment(topic, partition)
            
            logger.info(
                "Cancelled reassignment",
                topic=topic,
                partition=partition,
            )
            
            return True
        
        return False
