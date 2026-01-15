"""
Transaction support for exactly-once delivery.

Provides atomic multi-partition writes using two-phase commit.
"""

from distributedlog.transaction.coordinator import (
    TransactionCoordinator,
    TransactionMarker,
    TwoPhaseCommitResult,
)
from distributedlog.transaction.log import (
    TransactionLog,
    TransactionLogEntry,
)
from distributedlog.transaction.state import (
    TransactionMetadata,
    TransactionPartition,
    TransactionState,
    TransactionStateManager,
)

__all__ = [
    # State Management
    "TransactionState",
    "TransactionPartition",
    "TransactionMetadata",
    "TransactionStateManager",
    # Transaction Log
    "TransactionLog",
    "TransactionLogEntry",
    # Coordinator
    "TransactionCoordinator",
    "TransactionMarker",
    "TwoPhaseCommitResult",
]
