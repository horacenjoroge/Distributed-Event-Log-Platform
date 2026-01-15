"""
Consumer transaction isolation levels.

Controls which messages consumers can see.
"""

from enum import Enum
from typing import Optional

from distributedlog.transaction.coordinator import TransactionMarker
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class IsolationLevel(Enum):
    """
    Consumer isolation levels.
    
    - READ_UNCOMMITTED: See all messages (including uncommitted)
    - READ_COMMITTED: Only see committed messages (skip aborted)
    """
    
    READ_UNCOMMITTED = "read_uncommitted"
    READ_COMMITTED = "read_committed"


class TransactionFilter:
    """
    Filters messages based on transaction state.
    
    For read_committed isolation, skips messages from aborted transactions.
    """
    
    def __init__(self, isolation_level: IsolationLevel):
        """
        Initialize transaction filter.
        
        Args:
            isolation_level: Consumer isolation level
        """
        self.isolation_level = isolation_level
        
        # Track aborted transactions (transaction_id -> True)
        self._aborted_transactions = set()
        
        logger.info(
            "TransactionFilter initialized",
            isolation_level=isolation_level.value,
        )
    
    def should_filter_message(
        self,
        transaction_id: Optional[str],
        transaction_marker: Optional[str],
    ) -> bool:
        """
        Check if message should be filtered (hidden from consumer).
        
        Args:
            transaction_id: Transaction ID (if transactional message)
            transaction_marker: Transaction marker type (if marker message)
        
        Returns:
            True if message should be filtered
        """
        # READ_UNCOMMITTED: See everything
        if self.isolation_level == IsolationLevel.READ_UNCOMMITTED:
            return False
        
        # READ_COMMITTED: Filter based on transaction state
        
        # If this is a transaction marker
        if transaction_marker:
            # Track aborted transactions
            if transaction_marker == TransactionMarker.ABORT:
                if transaction_id:
                    self._aborted_transactions.add(transaction_id)
            
            # Always filter marker messages (consumers don't see markers)
            return True
        
        # If message is part of aborted transaction
        if transaction_id and transaction_id in self._aborted_transactions:
            logger.debug(
                "Filtering message from aborted transaction",
                transaction_id=transaction_id,
            )
            return True
        
        return False
    
    def clear_aborted_transaction(self, transaction_id: str) -> None:
        """
        Clear aborted transaction from tracking.
        
        Called after transaction cleanup.
        
        Args:
            transaction_id: Transaction ID
        """
        self._aborted_transactions.discard(transaction_id)


class TransactionalConsumerConfig:
    """
    Configuration for transactional consumer.
    
    Attributes:
        isolation_level: Transaction isolation level
    """
    
    def __init__(
        self,
        isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED,
    ):
        """
        Initialize transactional consumer config.
        
        Args:
            isolation_level: Isolation level (default: read_committed)
        """
        self.isolation_level = isolation_level


class TransactionalConsumer:
    """
    Consumer with transaction isolation support.
    
    Filters messages based on isolation level:
    - read_committed: Only see committed messages
    - read_uncommitted: See all messages
    """
    
    def __init__(
        self,
        config: TransactionalConsumerConfig,
    ):
        """
        Initialize transactional consumer.
        
        Args:
            config: Transactional consumer configuration
        """
        self.config = config
        self.filter = TransactionFilter(config.isolation_level)
        
        logger.info(
            "TransactionalConsumer initialized",
            isolation_level=config.isolation_level.value,
        )
    
    def poll_with_isolation(
        self,
        messages: list,
    ) -> list:
        """
        Poll messages with transaction isolation.
        
        Filters messages based on isolation level.
        
        Args:
            messages: Raw messages from log
        
        Returns:
            Filtered messages
        """
        if self.config.isolation_level == IsolationLevel.READ_UNCOMMITTED:
            return messages
        
        # Filter messages
        filtered = []
        
        for msg in messages:
            # Extract transaction metadata
            transaction_id = msg.get("transaction_id")
            transaction_marker = msg.get("transaction_marker")
            
            # Check if should filter
            if not self.filter.should_filter_message(
                transaction_id,
                transaction_marker,
            ):
                filtered.append(msg)
        
        if len(filtered) < len(messages):
            logger.debug(
                "Filtered messages",
                original=len(messages),
                filtered_count=len(messages) - len(filtered),
            )
        
        return filtered
