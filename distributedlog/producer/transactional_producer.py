"""
Transactional producer API.

Producer with transaction support for atomic multi-partition writes.
"""

import uuid
from typing import Optional

from distributedlog.producer.idempotent_producer import IdempotentProducer
from distributedlog.transaction.coordinator import TransactionCoordinator
from distributedlog.transaction.state import TransactionState
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class TransactionalProducerConfig:
    """
    Configuration for transactional producer.
    
    Attributes:
        transactional_id: Unique transactional ID
        transaction_timeout_ms: Transaction timeout
        enable_idempotence: Enable idempotence (required for transactions)
    """
    
    def __init__(
        self,
        transactional_id: str,
        transaction_timeout_ms: int = 60000,  # 1 minute
        enable_idempotence: bool = True,
    ):
        """
        Initialize transactional producer config.
        
        Args:
            transactional_id: Transactional ID (must be unique)
            transaction_timeout_ms: Transaction timeout
            enable_idempotence: Enable idempotence
        """
        if not enable_idempotence:
            raise ValueError("Idempotence must be enabled for transactional producer")
        
        self.transactional_id = transactional_id
        self.transaction_timeout_ms = transaction_timeout_ms
        self.enable_idempotence = enable_idempotence


class TransactionalProducer(IdempotentProducer):
    """
    Producer with transaction support.
    
    Provides atomic multi-partition writes using two-phase commit.
    
    API:
    ```
    producer.begin_transaction()
    producer.send('topic1', partition=0, message='msg1')
    producer.send('topic2', partition=1, message='msg2')
    producer.commit_transaction()  # Atomic commit to both partitions
    ```
    
    Isolation guarantees:
    - read_committed: Consumers only see committed messages
    - Exactly-once semantics with idempotence
    """
    
    def __init__(
        self,
        config: TransactionalProducerConfig,
        coordinator: Optional[TransactionCoordinator] = None,
    ):
        """
        Initialize transactional producer.
        
        Args:
            config: Transactional producer configuration
            coordinator: Transaction coordinator
        """
        super().__init__(client_id=config.transactional_id)
        
        self.config = config
        self.coordinator = coordinator or TransactionCoordinator(
            coordinator_id="default-coordinator",
            transaction_timeout_ms=config.transaction_timeout_ms,
        )
        
        # Current transaction
        self._current_transaction_id: Optional[str] = None
        self._transaction_partitions: list = []
        
        logger.info(
            "TransactionalProducer initialized",
            transactional_id=config.transactional_id,
        )
    
    def begin_transaction(self) -> str:
        """
        Begin a new transaction.
        
        Returns:
            Transaction ID
        
        Raises:
            RuntimeError: If transaction already active
        """
        if self._current_transaction_id:
            raise RuntimeError("Transaction already active")
        
        if not self.producer_id_info:
            raise RuntimeError("Producer ID not initialized")
        
        # Generate transaction ID
        transaction_id = f"{self.config.transactional_id}-{uuid.uuid4().hex[:8]}"
        
        # Begin transaction via coordinator
        metadata = self.coordinator.begin_transaction(
            transaction_id=transaction_id,
            producer_id=self.producer_id_info.producer_id,
            producer_epoch=self.producer_id_info.producer_epoch,
            timeout_ms=self.config.transaction_timeout_ms,
        )
        
        self._current_transaction_id = transaction_id
        self._transaction_partitions = []
        
        logger.info(
            "Transaction began",
            transaction_id=transaction_id,
        )
        
        return transaction_id
    
    def send_transactional(
        self,
        topic: str,
        partition: int,
        message: bytes,
    ) -> None:
        """
        Send message within transaction.
        
        Args:
            topic: Topic name
            partition: Partition number
            message: Message data
        
        Raises:
            RuntimeError: If no active transaction
        """
        if not self._current_transaction_id:
            raise RuntimeError("No active transaction")
        
        # Track partition for 2PC
        partition_tuple = (topic, partition)
        if partition_tuple not in self._transaction_partitions:
            self._transaction_partitions.append(partition_tuple)
            
            # Add partition to transaction
            self.coordinator.add_partitions_to_transaction(
                transaction_id=self._current_transaction_id,
                partitions=[partition_tuple],
            )
        
        # Send message (idempotent)
        metadata = self.prepare_send(topic, partition)
        
        logger.debug(
            "Transactional send",
            transaction_id=self._current_transaction_id,
            topic=topic,
            partition=partition,
            producer_id=metadata["producer_id"],
            sequence=metadata["sequence"],
        )
        
        # TODO: Actually send to partition log
        # In production: broker.produce(topic, partition, message, metadata)
    
    def commit_transaction(self) -> bool:
        """
        Commit current transaction.
        
        Uses two-phase commit to atomically commit to all partitions.
        
        Returns:
            True if committed successfully
        
        Raises:
            RuntimeError: If no active transaction
        """
        if not self._current_transaction_id:
            raise RuntimeError("No active transaction")
        
        transaction_id = self._current_transaction_id
        
        try:
            # Execute 2PC via coordinator
            result = self.coordinator.commit_transaction(transaction_id)
            
            if result.success:
                logger.info(
                    "Transaction committed",
                    transaction_id=transaction_id,
                    partitions=len(self._transaction_partitions),
                )
                return True
            else:
                logger.error(
                    "Transaction commit failed",
                    transaction_id=transaction_id,
                    error=result.error,
                )
                return False
        
        finally:
            # Clear transaction state
            self._current_transaction_id = None
            self._transaction_partitions = []
    
    def abort_transaction(self) -> bool:
        """
        Abort current transaction.
        
        Returns:
            True if aborted successfully
        
        Raises:
            RuntimeError: If no active transaction
        """
        if not self._current_transaction_id:
            raise RuntimeError("No active transaction")
        
        transaction_id = self._current_transaction_id
        
        try:
            # Execute abort via coordinator
            result = self.coordinator.abort_transaction(transaction_id)
            
            if result.success:
                logger.info(
                    "Transaction aborted",
                    transaction_id=transaction_id,
                )
                return True
            else:
                logger.error(
                    "Transaction abort failed",
                    transaction_id=transaction_id,
                    error=result.error,
                )
                return False
        
        finally:
            # Clear transaction state
            self._current_transaction_id = None
            self._transaction_partitions = []
    
    def get_current_transaction_id(self) -> Optional[str]:
        """
        Get current transaction ID.
        
        Returns:
            Transaction ID or None
        """
        return self._current_transaction_id
    
    def is_in_transaction(self) -> bool:
        """
        Check if in active transaction.
        
        Returns:
            True if in transaction
        """
        return self._current_transaction_id is not None
