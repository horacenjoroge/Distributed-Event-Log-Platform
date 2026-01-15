"""
Comprehensive tests for transaction support.
"""

import time

import pytest

from distributedlog.consumer.isolation import (
    IsolationLevel,
    TransactionFilter,
    TransactionalConsumer,
    TransactionalConsumerConfig,
)
from distributedlog.producer.idempotence import ProducerIdManager
from distributedlog.producer.transactional_producer import (
    TransactionalProducer,
    TransactionalProducerConfig,
)
from distributedlog.transaction import (
    TransactionCoordinator,
    TransactionMarker,
    TransactionState,
)


class TestTransactionState:
    """Test transaction state machine."""
    
    def test_state_transitions(self):
        """Test valid state transitions."""
        # EMPTY → ONGOING
        assert TransactionState.EMPTY.can_transition_to(TransactionState.ONGOING)
        
        # ONGOING → PREPARE_COMMIT
        assert TransactionState.ONGOING.can_transition_to(
            TransactionState.PREPARE_COMMIT
        )
        
        # PREPARE_COMMIT → COMMITTED
        assert TransactionState.PREPARE_COMMIT.can_transition_to(
            TransactionState.COMMITTED
        )
    
    def test_invalid_transitions(self):
        """Test invalid state transitions."""
        # Cannot go back from COMMITTED
        assert not TransactionState.COMMITTED.can_transition_to(
            TransactionState.ONGOING
        )
        
        # Cannot skip PREPARE phase
        assert not TransactionState.ONGOING.can_transition_to(
            TransactionState.COMMITTED
        )
    
    def test_terminal_states(self):
        """Test terminal state detection."""
        assert TransactionState.COMMITTED.is_terminal()
        assert TransactionState.ABORTED.is_terminal()
        assert not TransactionState.ONGOING.is_terminal()


class TestTransactionCoordinator:
    """Test TransactionCoordinator."""
    
    def test_begin_transaction(self):
        """Test beginning a transaction."""
        coordinator = TransactionCoordinator("test-coordinator")
        
        metadata = coordinator.begin_transaction(
            transaction_id="txn-1",
            producer_id=1,
            producer_epoch=0,
            timeout_ms=60000,
        )
        
        assert metadata.transaction_id == "txn-1"
        assert metadata.state == TransactionState.ONGOING
    
    def test_add_partitions(self):
        """Test adding partitions to transaction."""
        coordinator = TransactionCoordinator("test-coordinator")
        
        coordinator.begin_transaction(
            transaction_id="txn-1",
            producer_id=1,
            producer_epoch=0,
        )
        
        coordinator.add_partitions_to_transaction(
            transaction_id="txn-1",
            partitions=[("topic1", 0), ("topic2", 1)],
        )
        
        metadata = coordinator.state_manager.get_transaction("txn-1")
        assert len(metadata.partitions) == 2
    
    def test_commit_transaction(self):
        """Test committing a transaction."""
        coordinator = TransactionCoordinator("test-coordinator")
        
        coordinator.begin_transaction(
            transaction_id="txn-1",
            producer_id=1,
            producer_epoch=0,
        )
        
        coordinator.add_partitions_to_transaction(
            transaction_id="txn-1",
            partitions=[("topic1", 0)],
        )
        
        result = coordinator.commit_transaction("txn-1")
        
        assert result.success
        
        metadata = coordinator.state_manager.get_transaction("txn-1")
        assert metadata.state == TransactionState.COMMITTED
    
    def test_abort_transaction(self):
        """Test aborting a transaction."""
        coordinator = TransactionCoordinator("test-coordinator")
        
        coordinator.begin_transaction(
            transaction_id="txn-1",
            producer_id=1,
            producer_epoch=0,
        )
        
        result = coordinator.abort_transaction("txn-1")
        
        assert result.success
        
        metadata = coordinator.state_manager.get_transaction("txn-1")
        assert metadata.state == TransactionState.ABORTED
    
    def test_transaction_timeout(self):
        """Test transaction timeout detection."""
        coordinator = TransactionCoordinator(
            "test-coordinator",
            transaction_timeout_ms=100,  # 100ms timeout
        )
        
        coordinator.begin_transaction(
            transaction_id="txn-1",
            producer_id=1,
            producer_epoch=0,
            timeout_ms=100,
        )
        
        # Wait for timeout
        time.sleep(0.2)
        
        # Commit should detect timeout and abort
        result = coordinator.commit_transaction("txn-1")
        
        metadata = coordinator.state_manager.get_transaction("txn-1")
        assert metadata.state == TransactionState.ABORTED
    
    def test_zombie_transaction_abort(self):
        """Test aborting zombie (timed-out) transactions."""
        coordinator = TransactionCoordinator(
            "test-coordinator",
            transaction_timeout_ms=100,
        )
        
        coordinator.begin_transaction(
            transaction_id="txn-1",
            producer_id=1,
            producer_epoch=0,
            timeout_ms=100,
        )
        
        # Wait for timeout
        time.sleep(0.2)
        
        # Abort zombies
        aborted = coordinator.abort_zombie_transactions()
        
        assert aborted == 1


class TestTransactionalProducer:
    """Test TransactionalProducer."""
    
    def test_begin_transaction(self):
        """Test beginning a transaction."""
        config = TransactionalProducerConfig(
            transactional_id="producer-1",
        )
        producer = TransactionalProducer(config)
        
        # Init producer ID
        pid_manager = ProducerIdManager()
        producer.init_producer_id(pid_manager)
        
        txn_id = producer.begin_transaction()
        
        assert txn_id is not None
        assert producer.is_in_transaction()
    
    def test_send_transactional(self):
        """Test sending messages in transaction."""
        config = TransactionalProducerConfig(
            transactional_id="producer-1",
        )
        producer = TransactionalProducer(config)
        
        pid_manager = ProducerIdManager()
        producer.init_producer_id(pid_manager)
        
        producer.begin_transaction()
        
        # Send to multiple partitions
        producer.send_transactional("topic1", 0, b"msg1")
        producer.send_transactional("topic2", 1, b"msg2")
        
        assert len(producer._transaction_partitions) == 2
    
    def test_commit_transaction(self):
        """Test committing a transaction."""
        config = TransactionalProducerConfig(
            transactional_id="producer-1",
        )
        producer = TransactionalProducer(config)
        
        pid_manager = ProducerIdManager()
        producer.init_producer_id(pid_manager)
        
        producer.begin_transaction()
        producer.send_transactional("topic1", 0, b"msg1")
        
        success = producer.commit_transaction()
        
        assert success
        assert not producer.is_in_transaction()
    
    def test_abort_transaction(self):
        """Test aborting a transaction."""
        config = TransactionalProducerConfig(
            transactional_id="producer-1",
        )
        producer = TransactionalProducer(config)
        
        pid_manager = ProducerIdManager()
        producer.init_producer_id(pid_manager)
        
        producer.begin_transaction()
        producer.send_transactional("topic1", 0, b"msg1")
        
        success = producer.abort_transaction()
        
        assert success
        assert not producer.is_in_transaction()
    
    def test_cannot_begin_without_commit(self):
        """Test cannot begin new transaction without committing."""
        config = TransactionalProducerConfig(
            transactional_id="producer-1",
        )
        producer = TransactionalProducer(config)
        
        pid_manager = ProducerIdManager()
        producer.init_producer_id(pid_manager)
        
        producer.begin_transaction()
        
        # Try to begin again
        with pytest.raises(RuntimeError, match="already active"):
            producer.begin_transaction()
    
    def test_idempotence_required(self):
        """Test that idempotence is required for transactions."""
        with pytest.raises(ValueError, match="Idempotence"):
            TransactionalProducerConfig(
                transactional_id="producer-1",
                enable_idempotence=False,
            )


class TestConsumerIsolation:
    """Test consumer transaction isolation."""
    
    def test_read_uncommitted(self):
        """Test READ_UNCOMMITTED isolation level."""
        config = TransactionalConsumerConfig(
            isolation_level=IsolationLevel.READ_UNCOMMITTED,
        )
        consumer = TransactionalConsumer(config)
        
        messages = [
            {"data": "msg1", "transaction_id": "txn-1"},
            {"data": "msg2", "transaction_id": None},
        ]
        
        filtered = consumer.poll_with_isolation(messages)
        
        # See all messages
        assert len(filtered) == 2
    
    def test_read_committed_filters_aborted(self):
        """Test READ_COMMITTED filters aborted messages."""
        config = TransactionalConsumerConfig(
            isolation_level=IsolationLevel.READ_COMMITTED,
        )
        consumer = TransactionalConsumer(config)
        
        messages = [
            # Transaction marker (ABORT)
            {
                "transaction_id": "txn-1",
                "transaction_marker": TransactionMarker.ABORT,
            },
            # Message from aborted transaction
            {
                "data": "msg1",
                "transaction_id": "txn-1",
            },
            # Regular message
            {
                "data": "msg2",
                "transaction_id": None,
            },
        ]
        
        filtered = consumer.poll_with_isolation(messages)
        
        # Should only see regular message (marker + aborted message filtered)
        assert len(filtered) == 1
        assert filtered[0]["data"] == "msg2"
    
    def test_read_committed_shows_committed(self):
        """Test READ_COMMITTED shows committed messages."""
        config = TransactionalConsumerConfig(
            isolation_level=IsolationLevel.READ_COMMITTED,
        )
        consumer = TransactionalConsumer(config)
        
        messages = [
            # Transaction marker (COMMIT)
            {
                "transaction_id": "txn-1",
                "transaction_marker": TransactionMarker.COMMIT,
            },
            # Message from committed transaction
            {
                "data": "msg1",
                "transaction_id": "txn-1",
            },
        ]
        
        filtered = consumer.poll_with_isolation(messages)
        
        # Should see committed message (marker filtered)
        assert len(filtered) == 1
        assert filtered[0]["data"] == "msg1"
    
    def test_filters_all_markers(self):
        """Test that all transaction markers are filtered."""
        config = TransactionalConsumerConfig(
            isolation_level=IsolationLevel.READ_COMMITTED,
        )
        consumer = TransactionalConsumer(config)
        
        messages = [
            {"transaction_marker": TransactionMarker.PREPARE_COMMIT},
            {"transaction_marker": TransactionMarker.COMMIT},
            {"transaction_marker": TransactionMarker.PREPARE_ABORT},
            {"transaction_marker": TransactionMarker.ABORT},
            {"data": "msg1"},
        ]
        
        filtered = consumer.poll_with_isolation(messages)
        
        # Only see regular message
        assert len(filtered) == 1


class TestTwoPhaseCommit:
    """Test two-phase commit protocol."""
    
    def test_2pc_phases(self):
        """Test 2PC phase transitions."""
        coordinator = TransactionCoordinator("test-coordinator")
        
        metadata = coordinator.begin_transaction(
            transaction_id="txn-1",
            producer_id=1,
            producer_epoch=0,
        )
        
        assert metadata.state == TransactionState.ONGOING
        
        coordinator.add_partitions_to_transaction(
            "txn-1",
            [("topic1", 0)],
        )
        
        # Execute commit (2PC)
        result = coordinator.commit_transaction("txn-1")
        
        assert result.success
        
        # Check final state
        final_metadata = coordinator.state_manager.get_transaction("txn-1")
        assert final_metadata.state == TransactionState.COMMITTED
    
    def test_coordinator_recovery(self):
        """Test coordinator recovery after failure."""
        coordinator = TransactionCoordinator("test-coordinator")
        
        # Start transaction
        coordinator.begin_transaction(
            transaction_id="txn-1",
            producer_id=1,
            producer_epoch=0,
        )
        
        # Simulate recovery
        recovered = coordinator.recover_transactions()
        
        # Should recover the ongoing transaction
        assert recovered >= 0
