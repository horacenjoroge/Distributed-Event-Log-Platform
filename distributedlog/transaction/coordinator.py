"""
Transaction coordinator for two-phase commit.

Coordinates distributed transactions across multiple partitions.
"""

import time
from typing import Dict, List, Optional, Set

from distributedlog.transaction.log import TransactionLog
from distributedlog.transaction.state import (
    TransactionMetadata,
    TransactionPartition,
    TransactionState,
    TransactionStateManager,
)
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class TransactionMarker:
    """
    Transaction marker written to partition log.
    
    Types:
    - PREPARE_COMMIT: Phase 1 of 2PC (commit)
    - COMMIT: Phase 2 of 2PC (finalize commit)
    - PREPARE_ABORT: Phase 1 of 2PC (abort)
    - ABORT: Phase 2 of 2PC (finalize abort)
    """
    
    PREPARE_COMMIT = "PREPARE_COMMIT"
    COMMIT = "COMMIT"
    PREPARE_ABORT = "PREPARE_ABORT"
    ABORT = "ABORT"
    
    def __init__(
        self,
        transaction_id: str,
        producer_id: int,
        marker_type: str,
        timestamp: int,
    ):
        """
        Initialize transaction marker.
        
        Args:
            transaction_id: Transaction ID
            producer_id: Producer ID
            marker_type: Marker type
            timestamp: Marker timestamp (ms)
        """
        self.transaction_id = transaction_id
        self.producer_id = producer_id
        self.marker_type = marker_type
        self.timestamp = timestamp


class TwoPhaseCommitResult:
    """Result of two-phase commit operation."""
    
    def __init__(
        self,
        success: bool,
        error: Optional[str] = None,
        failed_partitions: Optional[List[TransactionPartition]] = None,
    ):
        """
        Initialize 2PC result.
        
        Args:
            success: Whether operation succeeded
            error: Error message (if failed)
            failed_partitions: Partitions that failed
        """
        self.success = success
        self.error = error
        self.failed_partitions = failed_partitions or []


class TransactionCoordinator:
    """
    Coordinates distributed transactions using two-phase commit.
    
    Responsibilities:
    1. Track transaction state
    2. Coordinate two-phase commit
    3. Write markers to partition logs
    4. Handle coordinator failures (recovery)
    5. Detect and abort zombie transactions
    """
    
    def __init__(
        self,
        coordinator_id: str,
        transaction_timeout_ms: int = 60000,  # 1 minute
    ):
        """
        Initialize transaction coordinator.
        
        Args:
            coordinator_id: Coordinator identifier
            transaction_timeout_ms: Default transaction timeout
        """
        self.coordinator_id = coordinator_id
        self.transaction_timeout_ms = transaction_timeout_ms
        
        # State management
        self.state_manager = TransactionStateManager()
        self.transaction_log = TransactionLog()
        
        # Track in-flight 2PC operations
        self._in_flight_commits: Set[str] = set()
        
        logger.info(
            "TransactionCoordinator initialized",
            coordinator_id=coordinator_id,
            timeout_ms=transaction_timeout_ms,
        )
    
    def begin_transaction(
        self,
        transaction_id: str,
        producer_id: int,
        producer_epoch: int,
        timeout_ms: Optional[int] = None,
    ) -> TransactionMetadata:
        """
        Begin a new transaction.
        
        Args:
            transaction_id: Transaction ID
            producer_id: Producer ID
            producer_epoch: Producer epoch
            timeout_ms: Transaction timeout (optional)
        
        Returns:
            Transaction metadata
        """
        start_time = int(time.time() * 1000)
        timeout = timeout_ms or self.transaction_timeout_ms
        
        # Begin transaction in memory
        metadata = self.state_manager.begin_transaction(
            transaction_id=transaction_id,
            producer_id=producer_id,
            producer_epoch=producer_epoch,
            timeout_ms=timeout,
            start_time=start_time,
        )
        
        # Persist to transaction log
        self.transaction_log.append_transaction_state(
            transaction_id=transaction_id,
            producer_id=producer_id,
            producer_epoch=producer_epoch,
            state=TransactionState.ONGOING,
            partitions=[],
            timeout_ms=timeout,
        )
        
        logger.info(
            "Transaction began",
            transaction_id=transaction_id,
            producer_id=producer_id,
        )
        
        return metadata
    
    def add_partitions_to_transaction(
        self,
        transaction_id: str,
        partitions: List[tuple],
    ) -> None:
        """
        Add partitions to transaction.
        
        Args:
            transaction_id: Transaction ID
            partitions: List of (topic, partition) tuples
        """
        metadata = self.state_manager.get_transaction(transaction_id)
        
        if not metadata:
            raise ValueError(f"Transaction {transaction_id} not found")
        
        if metadata.state != TransactionState.ONGOING:
            raise ValueError(
                f"Cannot add partitions to transaction in state {metadata.state}"
            )
        
        # Add partitions to metadata
        for topic, partition in partitions:
            metadata.add_partition(topic, partition)
        
        logger.debug(
            "Partitions added to transaction",
            transaction_id=transaction_id,
            partitions=len(partitions),
        )
    
    def commit_transaction(
        self,
        transaction_id: str,
    ) -> TwoPhaseCommitResult:
        """
        Commit transaction using two-phase commit.
        
        Phase 1 (PREPARE_COMMIT):
        - Write PREPARE_COMMIT to transaction log
        - Write PREPARE_COMMIT markers to all partitions
        - If any fail, abort
        
        Phase 2 (COMMIT):
        - Write COMMIT to transaction log
        - Write COMMIT markers to all partitions
        - Transaction complete
        
        Args:
            transaction_id: Transaction ID
        
        Returns:
            TwoPhaseCommitResult
        """
        metadata = self.state_manager.get_transaction(transaction_id)
        
        if not metadata:
            return TwoPhaseCommitResult(
                success=False,
                error=f"Transaction {transaction_id} not found",
            )
        
        if metadata.state != TransactionState.ONGOING:
            return TwoPhaseCommitResult(
                success=False,
                error=f"Transaction in invalid state: {metadata.state}",
            )
        
        # Check timeout
        current_time = int(time.time() * 1000)
        if metadata.is_timed_out(current_time):
            logger.warning(
                "Transaction timed out, aborting",
                transaction_id=transaction_id,
            )
            return self.abort_transaction(transaction_id)
        
        self._in_flight_commits.add(transaction_id)
        
        try:
            # Phase 1: PREPARE_COMMIT
            result = self._phase1_prepare_commit(metadata, current_time)
            
            if not result.success:
                # Phase 1 failed, abort
                logger.warning(
                    "Phase 1 failed, aborting transaction",
                    transaction_id=transaction_id,
                    error=result.error,
                )
                return self.abort_transaction(transaction_id)
            
            # Phase 2: COMMIT
            result = self._phase2_commit(metadata, current_time)
            
            return result
        
        finally:
            self._in_flight_commits.discard(transaction_id)
    
    def _phase1_prepare_commit(
        self,
        metadata: TransactionMetadata,
        timestamp: int,
    ) -> TwoPhaseCommitResult:
        """
        Phase 1 of 2PC: Prepare to commit.
        
        Args:
            metadata: Transaction metadata
            timestamp: Current timestamp
        
        Returns:
            TwoPhaseCommitResult
        """
        # Update state to PREPARE_COMMIT
        self.state_manager.update_state(
            transaction_id=metadata.transaction_id,
            new_state=TransactionState.PREPARE_COMMIT,
            update_time=timestamp,
        )
        
        # Write to transaction log
        self.transaction_log.append_transaction_state(
            transaction_id=metadata.transaction_id,
            producer_id=metadata.producer_id,
            producer_epoch=metadata.producer_epoch,
            state=TransactionState.PREPARE_COMMIT,
            partitions=[
                (p.topic, p.partition)
                for p in metadata.partitions
            ],
            timeout_ms=metadata.timeout_ms,
        )
        
        # Write PREPARE_COMMIT markers to all partitions
        failed_partitions = self._write_markers_to_partitions(
            metadata,
            TransactionMarker.PREPARE_COMMIT,
            timestamp,
        )
        
        if failed_partitions:
            return TwoPhaseCommitResult(
                success=False,
                error="Failed to write PREPARE_COMMIT markers",
                failed_partitions=failed_partitions,
            )
        
        logger.info(
            "Phase 1 (PREPARE_COMMIT) complete",
            transaction_id=metadata.transaction_id,
        )
        
        return TwoPhaseCommitResult(success=True)
    
    def _phase2_commit(
        self,
        metadata: TransactionMetadata,
        timestamp: int,
    ) -> TwoPhaseCommitResult:
        """
        Phase 2 of 2PC: Finalize commit.
        
        Args:
            metadata: Transaction metadata
            timestamp: Current timestamp
        
        Returns:
            TwoPhaseCommitResult
        """
        # Update state to COMMITTED
        self.state_manager.update_state(
            transaction_id=metadata.transaction_id,
            new_state=TransactionState.COMMITTED,
            update_time=timestamp,
        )
        
        # Write to transaction log
        self.transaction_log.append_transaction_state(
            transaction_id=metadata.transaction_id,
            producer_id=metadata.producer_id,
            producer_epoch=metadata.producer_epoch,
            state=TransactionState.COMMITTED,
            partitions=[
                (p.topic, p.partition)
                for p in metadata.partitions
            ],
            timeout_ms=metadata.timeout_ms,
        )
        
        # Write COMMIT markers to all partitions
        failed_partitions = self._write_markers_to_partitions(
            metadata,
            TransactionMarker.COMMIT,
            timestamp,
        )
        
        # Note: Even if writing COMMIT markers fails, transaction is committed
        # (Phase 2 is non-blocking in 2PC)
        
        logger.info(
            "Transaction committed",
            transaction_id=metadata.transaction_id,
            partitions=len(metadata.partitions),
        )
        
        return TwoPhaseCommitResult(success=True)
    
    def abort_transaction(
        self,
        transaction_id: str,
    ) -> TwoPhaseCommitResult:
        """
        Abort transaction using two-phase commit.
        
        Similar to commit but with PREPARE_ABORT and ABORT markers.
        
        Args:
            transaction_id: Transaction ID
        
        Returns:
            TwoPhaseCommitResult
        """
        metadata = self.state_manager.get_transaction(transaction_id)
        
        if not metadata:
            return TwoPhaseCommitResult(
                success=False,
                error=f"Transaction {transaction_id} not found",
            )
        
        current_time = int(time.time() * 1000)
        
        # Phase 1: PREPARE_ABORT
        self.state_manager.update_state(
            transaction_id=metadata.transaction_id,
            new_state=TransactionState.PREPARE_ABORT,
            update_time=current_time,
        )
        
        self.transaction_log.append_transaction_state(
            transaction_id=metadata.transaction_id,
            producer_id=metadata.producer_id,
            producer_epoch=metadata.producer_epoch,
            state=TransactionState.PREPARE_ABORT,
            partitions=[
                (p.topic, p.partition)
                for p in metadata.partitions
            ],
            timeout_ms=metadata.timeout_ms,
        )
        
        # Phase 2: ABORTED
        self.state_manager.update_state(
            transaction_id=metadata.transaction_id,
            new_state=TransactionState.ABORTED,
            update_time=current_time,
        )
        
        self.transaction_log.append_transaction_state(
            transaction_id=metadata.transaction_id,
            producer_id=metadata.producer_id,
            producer_epoch=metadata.producer_epoch,
            state=TransactionState.ABORTED,
            partitions=[
                (p.topic, p.partition)
                for p in metadata.partitions
            ],
            timeout_ms=metadata.timeout_ms,
        )
        
        # Write ABORT markers to partitions
        self._write_markers_to_partitions(
            metadata,
            TransactionMarker.ABORT,
            current_time,
        )
        
        logger.info(
            "Transaction aborted",
            transaction_id=metadata.transaction_id,
        )
        
        return TwoPhaseCommitResult(success=True)
    
    def _write_markers_to_partitions(
        self,
        metadata: TransactionMetadata,
        marker_type: str,
        timestamp: int,
    ) -> List[TransactionPartition]:
        """
        Write transaction markers to all partitions.
        
        Args:
            metadata: Transaction metadata
            marker_type: Marker type
            timestamp: Marker timestamp
        
        Returns:
            List of partitions that failed
        """
        failed = []
        
        for partition in metadata.partitions:
            try:
                marker = TransactionMarker(
                    transaction_id=metadata.transaction_id,
                    producer_id=metadata.producer_id,
                    marker_type=marker_type,
                    timestamp=timestamp,
                )
                
                # TODO: Write marker to actual partition log
                # In production: log.append(partition, marker)
                
                logger.debug(
                    "Wrote transaction marker",
                    transaction_id=metadata.transaction_id,
                    partition=partition,
                    marker_type=marker_type,
                )
            
            except Exception as e:
                logger.error(
                    "Failed to write transaction marker",
                    transaction_id=metadata.transaction_id,
                    partition=partition,
                    marker_type=marker_type,
                    error=str(e),
                )
                failed.append(partition)
        
        return failed
    
    def recover_transactions(self) -> int:
        """
        Recover transactions after coordinator restart.
        
        Reads transaction log and completes in-flight transactions.
        
        Returns:
            Number of transactions recovered
        """
        # Recover state from transaction log
        transactions = self.transaction_log.recover_state()
        
        recovered = 0
        current_time = int(time.time() * 1000)
        
        for txn_id, entry in transactions.items():
            # Skip terminal states
            if entry.state.is_terminal():
                continue
            
            # Recreate metadata
            metadata = TransactionMetadata(
                transaction_id=entry.transaction_id,
                producer_id=entry.producer_id,
                producer_epoch=entry.producer_epoch,
                state=entry.state,
                partitions=set(
                    TransactionPartition(t, p)
                    for t, p in entry.partitions
                ),
                start_time=entry.timestamp,
                timeout_ms=entry.timeout_ms,
                last_update_time=entry.timestamp,
            )
            
            # Check if timed out
            if metadata.is_timed_out(current_time):
                logger.warning(
                    "Recovered transaction timed out, aborting",
                    transaction_id=txn_id,
                )
                self.abort_transaction(txn_id)
                recovered += 1
                continue
            
            # Complete in-flight 2PC
            if entry.state == TransactionState.PREPARE_COMMIT:
                logger.info(
                    "Completing PREPARE_COMMIT transaction",
                    transaction_id=txn_id,
                )
                self._phase2_commit(metadata, current_time)
                recovered += 1
            
            elif entry.state == TransactionState.PREPARE_ABORT:
                logger.info(
                    "Completing PREPARE_ABORT transaction",
                    transaction_id=txn_id,
                )
                # Finish abort
                self.state_manager.update_state(
                    txn_id,
                    TransactionState.ABORTED,
                    current_time,
                )
                recovered += 1
        
        logger.info(
            "Transaction recovery complete",
            recovered=recovered,
        )
        
        return recovered
    
    def abort_zombie_transactions(self) -> int:
        """
        Abort timed-out (zombie) transactions.
        
        Returns:
            Number of transactions aborted
        """
        current_time = int(time.time() * 1000)
        zombies = self.state_manager.get_timed_out_transactions(current_time)
        
        for zombie in zombies:
            logger.warning(
                "Aborting zombie transaction",
                transaction_id=zombie.transaction_id,
                age_ms=current_time - zombie.start_time,
            )
            
            self.abort_transaction(zombie.transaction_id)
        
        return len(zombies)
    
    def get_stats(self) -> Dict:
        """
        Get coordinator statistics.
        
        Returns:
            Statistics dict
        """
        return {
            "coordinator_id": self.coordinator_id,
            "state_manager": self.state_manager.get_stats(),
            "transaction_log": self.transaction_log.get_stats(),
            "in_flight_commits": len(self._in_flight_commits),
        }
