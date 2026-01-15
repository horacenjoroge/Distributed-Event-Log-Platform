"""
Partition reassignment executor.

Orchestrates the multi-phase reassignment process.
"""

import asyncio
from typing import Dict, Optional

from distributedlog.broker.reassignment.state import (
    ReassignmentConfig,
    ReassignmentPhase,
    ReassignmentState,
    ReassignmentTracker,
)
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class ReassignmentExecutor:
    """
    Executes partition reassignments.
    
    Handles the multi-phase process:
    1. Add new replicas
    2. Wait for catch-up
    3. Remove old replicas
    """
    
    def __init__(
        self,
        controller,
        config: Optional[ReassignmentConfig] = None,
    ):
        """
        Initialize reassignment executor.
        
        Args:
            controller: ClusterController instance
            config: Reassignment configuration
        """
        self.controller = controller
        self.config = config or ReassignmentConfig()
        self.tracker = ReassignmentTracker()
        
        # Background task
        self._executor_task: Optional[asyncio.Task] = None
        self._running = False
        
        logger.info(
            "ReassignmentExecutor initialized",
            throttle_bytes_per_sec=self.config.throttle_bytes_per_sec,
        )
    
    async def start(self) -> None:
        """Start reassignment executor."""
        if self._running:
            return
        
        self._running = True
        
        self._executor_task = asyncio.create_task(self._executor_loop())
        
        logger.info("ReassignmentExecutor started")
    
    async def stop(self) -> None:
        """Stop reassignment executor."""
        self._running = False
        
        if self._executor_task:
            self._executor_task.cancel()
            try:
                await self._executor_task
            except asyncio.CancelledError:
                pass
        
        logger.info("ReassignmentExecutor stopped")
    
    async def _executor_loop(self) -> None:
        """Execute reassignments in background."""
        while self._running:
            try:
                # Process active reassignments
                for state in self.tracker.get_active_reassignments():
                    await self._process_reassignment(state)
                
                # Wait before next check
                await asyncio.sleep(self.config.check_interval_ms / 1000)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    "Error in executor loop",
                    error=str(e),
                )
                await asyncio.sleep(1.0)
    
    async def _process_reassignment(
        self,
        state: ReassignmentState,
    ) -> None:
        """
        Process a single reassignment through its phases.
        
        Args:
            state: Reassignment state
        """
        try:
            if state.phase == ReassignmentPhase.PENDING:
                await self._phase_start(state)
            
            elif state.phase == ReassignmentPhase.ADDING_REPLICAS:
                await self._phase_add_replicas(state)
            
            elif state.phase == ReassignmentPhase.WAITING_CATCHUP:
                await self._phase_wait_catchup(state)
            
            elif state.phase == ReassignmentPhase.REMOVING_REPLICAS:
                await self._phase_remove_replicas(state)
            
        except Exception as e:
            logger.error(
                "Error processing reassignment",
                topic=state.topic,
                partition=state.partition,
                phase=state.phase,
                error=str(e),
            )
            
            state.phase = ReassignmentPhase.FAILED
            state.error_message = str(e)
            self.tracker.remove_reassignment(state.topic, state.partition)
    
    async def _phase_start(self, state: ReassignmentState) -> None:
        """
        Start reassignment.
        
        Args:
            state: Reassignment state
        """
        logger.info(
            "Starting reassignment",
            topic=state.topic,
            partition=state.partition,
            old_replicas=state.old_replicas,
            new_replicas=state.new_replicas,
        )
        
        state.phase = ReassignmentPhase.ADDING_REPLICAS
        state.progress_percent = 10
    
    async def _phase_add_replicas(self, state: ReassignmentState) -> None:
        """
        Phase 1: Add new replicas.
        
        Args:
            state: Reassignment state
        """
        replicas_to_add = state.get_replicas_to_add()
        
        if not replicas_to_add:
            # No replicas to add, skip to removal
            state.phase = ReassignmentPhase.REMOVING_REPLICAS
            state.progress_percent = 60
            return
        
        logger.info(
            "Adding replicas",
            topic=state.topic,
            partition=state.partition,
            replicas_to_add=replicas_to_add,
        )
        
        # Add new replicas to partition
        for broker_id in replicas_to_add:
            if broker_id not in state.current_replicas:
                state.current_replicas.append(broker_id)
                
                logger.info(
                    "Added replica",
                    topic=state.topic,
                    partition=state.partition,
                    broker_id=broker_id,
                )
        
        # TODO: Integrate with replication system to start replication
        # For now, just move to next phase
        
        state.phase = ReassignmentPhase.WAITING_CATCHUP
        state.progress_percent = 30
    
    async def _phase_wait_catchup(self, state: ReassignmentState) -> None:
        """
        Phase 2: Wait for new replicas to catch up.
        
        Args:
            state: Reassignment state
        """
        replicas_to_add = state.get_replicas_to_add()
        
        if not replicas_to_add:
            # No new replicas, skip to removal
            state.phase = ReassignmentPhase.REMOVING_REPLICAS
            state.progress_percent = 60
            return
        
        # Check if new replicas are caught up
        all_caught_up = True
        
        for broker_id in replicas_to_add:
            lag = await self._get_replica_lag(
                state.topic,
                state.partition,
                broker_id,
            )
            
            if lag is None or lag > self.config.catchup_lag_threshold:
                all_caught_up = False
                break
        
        if all_caught_up:
            logger.info(
                "New replicas caught up",
                topic=state.topic,
                partition=state.partition,
                replicas=replicas_to_add,
            )
            
            state.phase = ReassignmentPhase.REMOVING_REPLICAS
            state.progress_percent = 60
        else:
            # Check timeout
            duration = state.duration_ms()
            if duration > self.config.catchup_timeout_ms:
                raise Exception(
                    f"Catch-up timeout after {duration}ms"
                )
            
            # Update progress based on average lag
            state.progress_percent = 30 + min(30, int(30 * (1 - lag / self.config.catchup_lag_threshold)))
    
    async def _phase_remove_replicas(self, state: ReassignmentState) -> None:
        """
        Phase 3: Remove old replicas.
        
        Args:
            state: Reassignment state
        """
        replicas_to_remove = state.get_replicas_to_remove()
        
        if not replicas_to_remove:
            # No replicas to remove, complete
            await self._phase_complete(state)
            return
        
        logger.info(
            "Removing replicas",
            topic=state.topic,
            partition=state.partition,
            replicas_to_remove=replicas_to_remove,
        )
        
        # Remove old replicas from partition
        for broker_id in replicas_to_remove:
            if broker_id in state.current_replicas:
                state.current_replicas.remove(broker_id)
                
                logger.info(
                    "Removed replica",
                    topic=state.topic,
                    partition=state.partition,
                    broker_id=broker_id,
                )
        
        # Update controller partition assignment
        # TODO: Integrate with controller to update metadata
        
        await self._phase_complete(state)
    
    async def _phase_complete(self, state: ReassignmentState) -> None:
        """
        Complete reassignment.
        
        Args:
            state: Reassignment state
        """
        state.phase = ReassignmentPhase.COMPLETED
        state.progress_percent = 100
        
        logger.info(
            "Reassignment completed",
            topic=state.topic,
            partition=state.partition,
            duration_ms=state.duration_ms(),
        )
        
        self.tracker.remove_reassignment(state.topic, state.partition)
    
    async def _get_replica_lag(
        self,
        topic: str,
        partition: int,
        broker_id: int,
    ) -> Optional[int]:
        """
        Get replica lag.
        
        Args:
            topic: Topic name
            partition: Partition number
            broker_id: Broker ID
        
        Returns:
            Lag in messages or None
        """
        # TODO: Integrate with replication system
        # For now, return 0 (caught up)
        return 0
    
    # Public API
    
    async def start_reassignment(
        self,
        topic: str,
        partition: int,
        new_replicas: list,
    ) -> bool:
        """
        Start partition reassignment.
        
        Args:
            topic: Topic name
            partition: Partition number
            new_replicas: Target replica list
        
        Returns:
            True if started
        """
        # Get current replicas from controller
        key = (topic, partition)
        
        if key not in self.controller.partition_assignments:
            logger.error(
                "Partition not found",
                topic=topic,
                partition=partition,
            )
            return False
        
        assignment = self.controller.partition_assignments[key]
        old_replicas = assignment.replicas.copy()
        
        # Validate new replicas
        if not new_replicas:
            logger.error("New replicas list is empty")
            return False
        
        # Check if already in progress
        if self.tracker.get_reassignment(topic, partition):
            logger.warning(
                "Reassignment already in progress",
                topic=topic,
                partition=partition,
            )
            return False
        
        # Create reassignment
        state = self.tracker.add_reassignment(
            topic=topic,
            partition=partition,
            old_replicas=old_replicas,
            new_replicas=new_replicas,
        )
        
        logger.info(
            "Started reassignment",
            topic=topic,
            partition=partition,
            old_replicas=old_replicas,
            new_replicas=new_replicas,
        )
        
        return True
    
    async def cancel_reassignment(
        self,
        topic: str,
        partition: int,
    ) -> bool:
        """
        Cancel active reassignment.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            True if cancelled
        """
        return self.tracker.cancel_reassignment(topic, partition)
    
    def get_reassignment_status(
        self,
        topic: str,
        partition: int,
    ) -> Optional[Dict]:
        """
        Get reassignment status.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            Status dict or None
        """
        state = self.tracker.get_reassignment(topic, partition)
        
        if not state:
            return None
        
        return {
            "topic": state.topic,
            "partition": state.partition,
            "old_replicas": state.old_replicas,
            "new_replicas": state.new_replicas,
            "current_replicas": state.current_replicas,
            "phase": state.phase,
            "progress_percent": state.progress_percent,
            "duration_ms": state.duration_ms(),
            "error_message": state.error_message,
        }
    
    def list_reassignments(self) -> list:
        """
        List all active reassignments.
        
        Returns:
            List of reassignment status dicts
        """
        return [
            self.get_reassignment_status(s.topic, s.partition)
            for s in self.tracker.get_active_reassignments()
        ]
