"""
Reassignment progress monitoring.

Provides metrics and progress tracking for ongoing reassignments.
"""

import time
from dataclasses import dataclass
from typing import Dict, List, Optional

from distributedlog.broker.reassignment.state import ReassignmentPhase, ReassignmentState
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ReassignmentMetrics:
    """
    Metrics for a partition reassignment.
    
    Attributes:
        topic: Topic name
        partition: Partition number
        phase: Current phase
        progress_percent: Progress percentage
        duration_ms: Time since start
        bytes_replicated: Total bytes replicated
        bytes_remaining: Estimated bytes remaining
        estimated_time_remaining_ms: Estimated time to complete
        replicas_added: Number of replicas added
        replicas_removed: Number of replicas removed
        errors: List of errors encountered
    """
    topic: str
    partition: int
    phase: ReassignmentPhase
    progress_percent: int
    duration_ms: int
    bytes_replicated: int = 0
    bytes_remaining: int = 0
    estimated_time_remaining_ms: int = 0
    replicas_added: int = 0
    replicas_removed: int = 0
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []


class ReassignmentMonitor:
    """
    Monitors reassignment progress and provides metrics.
    
    Tracks progress, estimates completion time, and provides observability.
    """
    
    def __init__(self, tracker):
        """
        Initialize reassignment monitor.
        
        Args:
            tracker: ReassignmentTracker instance
        """
        self.tracker = tracker
        
        # Per-partition metrics: (topic, partition) -> ReassignmentMetrics
        self._metrics: Dict[tuple, ReassignmentMetrics] = {}
        
        logger.info("ReassignmentMonitor initialized")
    
    def update_metrics(
        self,
        topic: str,
        partition: int,
        bytes_replicated: int = 0,
        error: Optional[str] = None,
    ) -> None:
        """
        Update metrics for a reassignment.
        
        Args:
            topic: Topic name
            partition: Partition number
            bytes_replicated: Bytes replicated (increment)
            error: Error message if any
        """
        key = (topic, partition)
        
        # Get or create metrics
        if key not in self._metrics:
            state = self.tracker.get_reassignment(topic, partition)
            if not state:
                return
            
            self._metrics[key] = ReassignmentMetrics(
                topic=topic,
                partition=partition,
                phase=state.phase,
                progress_percent=state.progress_percent,
                duration_ms=state.duration_ms(),
            )
        
        metrics = self._metrics[key]
        
        # Update from state
        state = self.tracker.get_reassignment(topic, partition)
        if state:
            metrics.phase = state.phase
            metrics.progress_percent = state.progress_percent
            metrics.duration_ms = state.duration_ms()
            metrics.replicas_added = len(state.get_replicas_to_add())
            metrics.replicas_removed = len(state.get_replicas_to_remove())
        
        # Update bytes replicated
        if bytes_replicated > 0:
            metrics.bytes_replicated += bytes_replicated
        
        # Add error
        if error:
            metrics.errors.append(error)
    
    def get_metrics(
        self,
        topic: str,
        partition: int,
    ) -> Optional[ReassignmentMetrics]:
        """
        Get metrics for a reassignment.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            Metrics or None
        """
        key = (topic, partition)
        
        if key in self._metrics:
            # Update duration
            metrics = self._metrics[key]
            state = self.tracker.get_reassignment(topic, partition)
            if state:
                metrics.duration_ms = state.duration_ms()
            
            return metrics
        
        return None
    
    def get_all_metrics(self) -> List[ReassignmentMetrics]:
        """
        Get metrics for all active reassignments.
        
        Returns:
            List of metrics
        """
        # Update all metrics
        for state in self.tracker.get_active_reassignments():
            self.update_metrics(state.topic, state.partition)
        
        return list(self._metrics.values())
    
    def estimate_completion_time(
        self,
        topic: str,
        partition: int,
    ) -> Optional[int]:
        """
        Estimate time to completion.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            Estimated ms to completion or None
        """
        metrics = self.get_metrics(topic, partition)
        
        if not metrics:
            return None
        
        if metrics.progress_percent == 0:
            return None
        
        # Estimate based on current progress
        elapsed = metrics.duration_ms
        estimated_total = (elapsed * 100) / metrics.progress_percent
        estimated_remaining = estimated_total - elapsed
        
        metrics.estimated_time_remaining_ms = int(estimated_remaining)
        
        return int(estimated_remaining)
    
    def get_summary(self) -> Dict:
        """
        Get summary of all reassignments.
        
        Returns:
            Summary dict
        """
        active = self.tracker.get_active_reassignments()
        completed = self.tracker.get_completed_reassignments()
        
        total_bytes_replicated = sum(
            m.bytes_replicated for m in self._metrics.values()
        )
        
        failed = [s for s in completed if s.phase == ReassignmentPhase.FAILED]
        succeeded = [s for s in completed if s.phase == ReassignmentPhase.COMPLETED]
        
        return {
            "active_count": len(active),
            "completed_count": len(succeeded),
            "failed_count": len(failed),
            "total_bytes_replicated": total_bytes_replicated,
            "active_reassignments": [
                {
                    "topic": s.topic,
                    "partition": s.partition,
                    "phase": s.phase,
                    "progress_percent": s.progress_percent,
                }
                for s in active
            ],
        }
    
    def cleanup_metrics(
        self,
        topic: str,
        partition: int,
    ) -> None:
        """
        Clean up metrics for completed reassignment.
        
        Args:
            topic: Topic name
            partition: Partition number
        """
        key = (topic, partition)
        
        if key in self._metrics:
            del self._metrics[key]
            
            logger.debug(
                "Cleaned up metrics",
                topic=topic,
                partition=partition,
            )
