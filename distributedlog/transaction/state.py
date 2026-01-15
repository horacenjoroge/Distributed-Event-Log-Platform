"""
Transaction state management.

Defines transaction states and state transitions.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set

from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class TransactionState(Enum):
    """
    Transaction lifecycle states.
    
    State transitions:
    EMPTY → ONGOING → PREPARE_COMMIT → COMMITTED
                   ↘ PREPARE_ABORT → ABORTED
    """
    
    EMPTY = "EMPTY"  # No transaction started
    ONGOING = "ONGOING"  # Transaction in progress
    PREPARE_COMMIT = "PREPARE_COMMIT"  # Phase 1 of 2PC (commit)
    PREPARE_ABORT = "PREPARE_ABORT"  # Phase 1 of 2PC (abort)
    COMMITTED = "COMMITTED"  # Transaction committed
    ABORTED = "ABORTED"  # Transaction aborted
    
    def is_terminal(self) -> bool:
        """Check if state is terminal (done)."""
        return self in (TransactionState.COMMITTED, TransactionState.ABORTED)
    
    def can_transition_to(self, new_state: 'TransactionState') -> bool:
        """
        Check if transition to new state is valid.
        
        Args:
            new_state: Target state
        
        Returns:
            True if transition is valid
        """
        valid_transitions = {
            TransactionState.EMPTY: {TransactionState.ONGOING},
            TransactionState.ONGOING: {
                TransactionState.PREPARE_COMMIT,
                TransactionState.PREPARE_ABORT,
            },
            TransactionState.PREPARE_COMMIT: {TransactionState.COMMITTED},
            TransactionState.PREPARE_ABORT: {TransactionState.ABORTED},
            TransactionState.COMMITTED: set(),  # Terminal
            TransactionState.ABORTED: set(),  # Terminal
        }
        
        return new_state in valid_transitions.get(self, set())


@dataclass
class TransactionPartition:
    """
    Partition involved in a transaction.
    
    Attributes:
        topic: Topic name
        partition: Partition number
    """
    topic: str
    partition: int
    
    def __hash__(self):
        return hash((self.topic, self.partition))
    
    def __eq__(self, other):
        if not isinstance(other, TransactionPartition):
            return False
        return self.topic == other.topic and self.partition == other.partition
    
    def __repr__(self):
        return f"{self.topic}-{self.partition}"


@dataclass
class TransactionMetadata:
    """
    Metadata for a transaction.
    
    Attributes:
        transaction_id: Unique transaction ID
        producer_id: Producer ID (for idempotence)
        producer_epoch: Producer epoch
        state: Current transaction state
        partitions: Partitions involved in transaction
        start_time: Transaction start timestamp (ms)
        timeout_ms: Transaction timeout
        last_update_time: Last state update timestamp (ms)
    """
    transaction_id: str
    producer_id: int
    producer_epoch: int
    state: TransactionState
    partitions: Set[TransactionPartition] = field(default_factory=set)
    start_time: int = 0
    timeout_ms: int = 60000  # 1 minute default
    last_update_time: int = 0
    
    def is_timed_out(self, current_time: int) -> bool:
        """
        Check if transaction has timed out.
        
        Args:
            current_time: Current timestamp (ms)
        
        Returns:
            True if timed out
        """
        if self.state.is_terminal():
            return False
        
        elapsed = current_time - self.start_time
        return elapsed > self.timeout_ms
    
    def add_partition(self, topic: str, partition: int) -> None:
        """
        Add partition to transaction.
        
        Args:
            topic: Topic name
            partition: Partition number
        """
        self.partitions.add(TransactionPartition(topic, partition))
    
    def get_partition_list(self) -> List[TransactionPartition]:
        """
        Get list of partitions.
        
        Returns:
            List of partitions
        """
        return list(self.partitions)


class TransactionStateManager:
    """
    Manages transaction state in memory.
    
    Tracks active transactions and their states.
    """
    
    def __init__(self):
        """Initialize transaction state manager."""
        # transaction_id -> TransactionMetadata
        self._transactions: Dict[str, TransactionMetadata] = {}
        
        logger.info("TransactionStateManager initialized")
    
    def begin_transaction(
        self,
        transaction_id: str,
        producer_id: int,
        producer_epoch: int,
        timeout_ms: int,
        start_time: int,
    ) -> TransactionMetadata:
        """
        Begin a new transaction.
        
        Args:
            transaction_id: Transaction ID
            producer_id: Producer ID
            producer_epoch: Producer epoch
            timeout_ms: Transaction timeout
            start_time: Start timestamp (ms)
        
        Returns:
            Transaction metadata
        """
        if transaction_id in self._transactions:
            existing = self._transactions[transaction_id]
            
            # Allow restarting if terminal state
            if not existing.state.is_terminal():
                raise ValueError(f"Transaction {transaction_id} already active")
        
        metadata = TransactionMetadata(
            transaction_id=transaction_id,
            producer_id=producer_id,
            producer_epoch=producer_epoch,
            state=TransactionState.ONGOING,
            start_time=start_time,
            timeout_ms=timeout_ms,
            last_update_time=start_time,
        )
        
        self._transactions[transaction_id] = metadata
        
        logger.info(
            "Transaction started",
            transaction_id=transaction_id,
            producer_id=producer_id,
        )
        
        return metadata
    
    def get_transaction(
        self,
        transaction_id: str,
    ) -> Optional[TransactionMetadata]:
        """
        Get transaction metadata.
        
        Args:
            transaction_id: Transaction ID
        
        Returns:
            Transaction metadata or None
        """
        return self._transactions.get(transaction_id)
    
    def update_state(
        self,
        transaction_id: str,
        new_state: TransactionState,
        update_time: int,
    ) -> None:
        """
        Update transaction state.
        
        Args:
            transaction_id: Transaction ID
            new_state: New state
            update_time: Update timestamp (ms)
        """
        if transaction_id not in self._transactions:
            raise ValueError(f"Transaction {transaction_id} not found")
        
        metadata = self._transactions[transaction_id]
        
        # Validate state transition
        if not metadata.state.can_transition_to(new_state):
            raise ValueError(
                f"Invalid state transition: {metadata.state} → {new_state}"
            )
        
        old_state = metadata.state
        metadata.state = new_state
        metadata.last_update_time = update_time
        
        logger.info(
            "Transaction state updated",
            transaction_id=transaction_id,
            old_state=old_state.value,
            new_state=new_state.value,
        )
    
    def remove_transaction(self, transaction_id: str) -> None:
        """
        Remove transaction (after completion).
        
        Args:
            transaction_id: Transaction ID
        """
        if transaction_id in self._transactions:
            del self._transactions[transaction_id]
            
            logger.info(
                "Transaction removed",
                transaction_id=transaction_id,
            )
    
    def get_active_transactions(self) -> List[TransactionMetadata]:
        """
        Get all active transactions.
        
        Returns:
            List of active transaction metadata
        """
        return [
            txn for txn in self._transactions.values()
            if not txn.state.is_terminal()
        ]
    
    def get_timed_out_transactions(self, current_time: int) -> List[TransactionMetadata]:
        """
        Get timed out transactions.
        
        Args:
            current_time: Current timestamp (ms)
        
        Returns:
            List of timed out transactions
        """
        return [
            txn for txn in self._transactions.values()
            if txn.is_timed_out(current_time)
        ]
    
    def get_stats(self) -> Dict:
        """
        Get manager statistics.
        
        Returns:
            Statistics dict
        """
        state_counts = {}
        for txn in self._transactions.values():
            state = txn.state.value
            state_counts[state] = state_counts.get(state, 0) + 1
        
        return {
            "total_transactions": len(self._transactions),
            "state_counts": state_counts,
        }
