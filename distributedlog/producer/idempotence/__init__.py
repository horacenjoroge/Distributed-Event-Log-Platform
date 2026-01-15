"""
Producer idempotence for exactly-once delivery.

Prevents duplicate messages from producer retries.
"""

from distributedlog.producer.idempotence.deduplication import (
    DeduplicationManager,
    IdempotentProduceResult,
)
from distributedlog.producer.idempotence.pid import (
    ProducerIdInfo,
    ProducerIdManager,
)
from distributedlog.producer.idempotence.sequence import (
    ProducerSequenceTracker,
    SequenceNumberManager,
    SequenceState,
)

__all__ = [
    # PID Management
    "ProducerIdManager",
    "ProducerIdInfo",
    # Sequence Tracking
    "SequenceNumberManager",
    "SequenceState",
    "ProducerSequenceTracker",
    # Deduplication
    "DeduplicationManager",
    "IdempotentProduceResult",
]
