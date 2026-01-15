"""
Transaction log for persistent transaction state.

Similar to __consumer_offsets, this is an internal compacted topic.
"""

import json
import time
from dataclasses import asdict
from typing import Dict, Optional

from distributedlog.transaction.state import (
    TransactionMetadata,
    TransactionPartition,
    TransactionState,
)
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class TransactionLogEntry:
    """
    Entry in the transaction log.
    
    Each entry represents a transaction state update.
    """
    
    def __init__(
        self,
        transaction_id: str,
        producer_id: int,
        producer_epoch: int,
        state: TransactionState,
        partitions: list,
        timestamp: int,
        timeout_ms: int = 60000,
    ):
        """
        Initialize transaction log entry.
        
        Args:
            transaction_id: Transaction ID
            producer_id: Producer ID
            producer_epoch: Producer epoch
            state: Transaction state
            partitions: List of (topic, partition) tuples
            timestamp: Entry timestamp (ms)
            timeout_ms: Transaction timeout
        """
        self.transaction_id = transaction_id
        self.producer_id = producer_id
        self.producer_epoch = producer_epoch
        self.state = state
        self.partitions = partitions
        self.timestamp = timestamp
        self.timeout_ms = timeout_ms
    
    def to_bytes(self) -> bytes:
        """
        Serialize to bytes.
        
        Returns:
            Serialized entry
        """
        data = {
            "transaction_id": self.transaction_id,
            "producer_id": self.producer_id,
            "producer_epoch": self.producer_epoch,
            "state": self.state.value,
            "partitions": [
                {"topic": t, "partition": p}
                for t, p in self.partitions
            ],
            "timestamp": self.timestamp,
            "timeout_ms": self.timeout_ms,
        }
        
        return json.dumps(data).encode('utf-8')
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'TransactionLogEntry':
        """
        Deserialize from bytes.
        
        Args:
            data: Serialized entry
        
        Returns:
            TransactionLogEntry
        """
        obj = json.loads(data.decode('utf-8'))
        
        return cls(
            transaction_id=obj["transaction_id"],
            producer_id=obj["producer_id"],
            producer_epoch=obj["producer_epoch"],
            state=TransactionState(obj["state"]),
            partitions=[
                (p["topic"], p["partition"])
                for p in obj["partitions"]
            ],
            timestamp=obj["timestamp"],
            timeout_ms=obj.get("timeout_ms", 60000),
        )


class TransactionLog:
    """
    Persistent transaction log.
    
    Stores transaction state in an internal compacted topic (__transaction_state).
    
    Similar to Kafka's transaction log, this is:
    - Compacted (only latest state per transaction_id)
    - Partitioned by transaction_id hash
    - Replicated for durability
    """
    
    TRANSACTION_TOPIC = "__transaction_state"
    NUM_PARTITIONS = 50  # Default number of transaction log partitions
    
    def __init__(self, log_dir: Optional[str] = None):
        """
        Initialize transaction log.
        
        Args:
            log_dir: Directory for log storage
        """
        self.log_dir = log_dir or "/tmp/distributedlog/txn_log"
        
        # In-memory cache: transaction_id -> TransactionLogEntry
        self._cache: Dict[str, TransactionLogEntry] = {}
        
        logger.info(
            "TransactionLog initialized",
            topic=self.TRANSACTION_TOPIC,
            num_partitions=self.NUM_PARTITIONS,
        )
    
    def append_transaction_state(
        self,
        transaction_id: str,
        producer_id: int,
        producer_epoch: int,
        state: TransactionState,
        partitions: list,
        timeout_ms: int = 60000,
    ) -> None:
        """
        Append transaction state to log.
        
        Args:
            transaction_id: Transaction ID
            producer_id: Producer ID
            producer_epoch: Producer epoch
            state: Transaction state
            partitions: List of (topic, partition) tuples
            timeout_ms: Transaction timeout
        """
        timestamp = int(time.time() * 1000)
        
        entry = TransactionLogEntry(
            transaction_id=transaction_id,
            producer_id=producer_id,
            producer_epoch=producer_epoch,
            state=state,
            partitions=partitions,
            timestamp=timestamp,
            timeout_ms=timeout_ms,
        )
        
        # Write to log (simplified: in-memory for now)
        self._write_entry(entry)
        
        # Update cache
        self._cache[transaction_id] = entry
        
        logger.debug(
            "Transaction state appended to log",
            transaction_id=transaction_id,
            state=state.value,
        )
    
    def _write_entry(self, entry: TransactionLogEntry) -> None:
        """
        Write entry to persistent storage.
        
        In production, this would write to the actual log.
        For now, we keep in memory.
        
        Args:
            entry: Transaction log entry
        """
        # TODO: Write to actual log partition
        # partition = hash(entry.transaction_id) % self.NUM_PARTITIONS
        # log.append(partition, entry.to_bytes())
        pass
    
    def get_transaction_state(
        self,
        transaction_id: str,
    ) -> Optional[TransactionLogEntry]:
        """
        Get transaction state from log.
        
        Args:
            transaction_id: Transaction ID
        
        Returns:
            Transaction log entry or None
        """
        return self._cache.get(transaction_id)
    
    def recover_state(self) -> Dict[str, TransactionLogEntry]:
        """
        Recover transaction state on startup.
        
        Reads compacted log and rebuilds in-memory state.
        
        Returns:
            Dictionary of transaction_id -> entry
        """
        # TODO: Read from actual log partitions
        # For now, return current cache
        
        logger.info(
            "Transaction state recovered",
            num_transactions=len(self._cache),
        )
        
        return self._cache.copy()
    
    def cleanup_completed_transactions(
        self,
        retention_ms: int = 86400000,  # 24 hours
    ) -> int:
        """
        Clean up old completed transactions.
        
        Args:
            retention_ms: Retention time for completed transactions
        
        Returns:
            Number of transactions cleaned up
        """
        current_time = int(time.time() * 1000)
        to_remove = []
        
        for txn_id, entry in self._cache.items():
            # Only clean up terminal states
            if not entry.state.is_terminal():
                continue
            
            age = current_time - entry.timestamp
            if age > retention_ms:
                to_remove.append(txn_id)
        
        for txn_id in to_remove:
            del self._cache[txn_id]
        
        if to_remove:
            logger.info(
                "Cleaned up completed transactions",
                count=len(to_remove),
            )
        
        return len(to_remove)
    
    def get_stats(self) -> Dict:
        """
        Get log statistics.
        
        Returns:
            Statistics dict
        """
        state_counts = {}
        for entry in self._cache.values():
            state = entry.state.value
            state_counts[state] = state_counts.get(state, 0) + 1
        
        return {
            "cached_transactions": len(self._cache),
            "state_counts": state_counts,
        }
