"""
Tests for producer idempotence.
"""

import pytest

from distributedlog.producer.idempotence import (
    DeduplicationManager,
    ProducerIdManager,
    ProducerSequenceTracker,
    SequenceNumberManager,
)
from distributedlog.producer.idempotent_producer import (
    IdempotentProducer,
    IdempotentProducerConfig,
)


class TestProducerIdManager:
    """Test ProducerIdManager."""
    
    def test_assign_producer_id(self):
        """Test assigning producer ID."""
        manager = ProducerIdManager(start_id=0)
        
        pid_info = manager.assign_producer_id(client_id="client1")
        
        assert pid_info.producer_id == 0
        assert pid_info.producer_epoch == 0
    
    def test_assign_multiple_pids(self):
        """Test assigning multiple producer IDs."""
        manager = ProducerIdManager(start_id=0)
        
        pid1 = manager.assign_producer_id(client_id="client1")
        pid2 = manager.assign_producer_id(client_id="client2")
        
        assert pid1.producer_id == 0
        assert pid2.producer_id == 1
    
    def test_reuse_pid_for_same_client(self):
        """Test reusing PID for same client."""
        manager = ProducerIdManager(start_id=0)
        
        pid1 = manager.assign_producer_id(client_id="client1")
        pid2 = manager.assign_producer_id(client_id="client1")
        
        # Should reuse same PID
        assert pid1.producer_id == pid2.producer_id
    
    def test_pid_overflow(self):
        """Test PID overflow wraps around."""
        manager = ProducerIdManager(start_id=2**31 - 1)
        
        pid1 = manager.assign_producer_id()
        pid2 = manager.assign_producer_id()
        
        assert pid1.producer_id == 2**31 - 1
        assert pid2.producer_id == 0  # Wrapped around


class TestSequenceNumberManager:
    """Test SequenceNumberManager."""
    
    def test_first_message_accepted(self):
        """Test first message is always accepted."""
        manager = SequenceNumberManager()
        
        is_valid, error = manager.check_sequence(
            producer_id=1,
            topic="test",
            partition=0,
            sequence=0,
        )
        
        assert is_valid
        assert error is None
    
    def test_in_order_message_accepted(self):
        """Test in-order message is accepted."""
        manager = SequenceNumberManager()
        
        # First message
        manager.update_sequence(1, "test", 0, 0, 100, 1000)
        
        # Next message (sequence + 1)
        is_valid, error = manager.check_sequence(
            producer_id=1,
            topic="test",
            partition=0,
            sequence=1,
        )
        
        assert is_valid
        assert error is None
    
    def test_duplicate_message_rejected(self):
        """Test duplicate message is rejected."""
        manager = SequenceNumberManager()
        
        # First message
        manager.update_sequence(1, "test", 0, 5, 100, 1000)
        
        # Duplicate (same sequence)
        is_valid, error = manager.check_sequence(
            producer_id=1,
            topic="test",
            partition=0,
            sequence=5,
        )
        
        assert not is_valid
        assert "Duplicate" in error
    
    def test_gap_detected(self):
        """Test gap detection (out-of-order)."""
        manager = SequenceNumberManager()
        
        # First message
        manager.update_sequence(1, "test", 0, 5, 100, 1000)
        
        # Gap (sequence 7, expected 6)
        is_valid, error = manager.check_sequence(
            producer_id=1,
            topic="test",
            partition=0,
            sequence=7,
        )
        
        assert not is_valid
        assert "Out-of-order" in error or "expected" in error.lower()
    
    def test_old_message_rejected(self):
        """Test old message is rejected."""
        manager = SequenceNumberManager()
        
        # First message
        manager.update_sequence(1, "test", 0, 5, 100, 1000)
        
        # Old message (sequence 3 < 5)
        is_valid, error = manager.check_sequence(
            producer_id=1,
            topic="test",
            partition=0,
            sequence=3,
        )
        
        assert not is_valid
        assert "Old" in error or "duplicate" in error.lower()
    
    def test_different_partitions_independent(self):
        """Test sequences for different partitions are independent."""
        manager = SequenceNumberManager()
        
        # Message to partition 0
        manager.update_sequence(1, "test", 0, 5, 100, 1000)
        
        # First message to partition 1 (different partition)
        is_valid, error = manager.check_sequence(
            producer_id=1,
            topic="test",
            partition=1,
            sequence=0,
        )
        
        # Should be accepted (first message for this partition)
        assert is_valid


class TestProducerSequenceTracker:
    """Test ProducerSequenceTracker."""
    
    def test_get_next_sequence(self):
        """Test getting next sequence number."""
        tracker = ProducerSequenceTracker(producer_id=1)
        
        seq1 = tracker.get_next_sequence("test", 0)
        seq2 = tracker.get_next_sequence("test", 0)
        
        assert seq1 == 0
        assert seq2 == 1
    
    def test_different_partitions_independent(self):
        """Test sequences for different partitions are independent."""
        tracker = ProducerSequenceTracker(producer_id=1)
        
        seq_p0 = tracker.get_next_sequence("test", 0)
        seq_p1 = tracker.get_next_sequence("test", 1)
        
        assert seq_p0 == 0
        assert seq_p1 == 0  # Independent
    
    def test_reset_sequence(self):
        """Test resetting sequence."""
        tracker = ProducerSequenceTracker(producer_id=1)
        
        tracker.get_next_sequence("test", 0)
        tracker.get_next_sequence("test", 0)
        
        tracker.reset_sequence("test", 0)
        
        seq = tracker.get_next_sequence("test", 0)
        
        assert seq == 0  # Reset to 0


class TestDeduplicationManager:
    """Test DeduplicationManager."""
    
    def test_validate_first_request(self):
        """Test validating first request from producer."""
        manager = DeduplicationManager()
        
        # Assign PID
        pid_info = manager.pid_manager.assign_producer_id("client1")
        
        # Validate first request
        is_valid, error = manager.validate_produce_request(
            producer_id=pid_info.producer_id,
            producer_epoch=0,
            topic="test",
            partition=0,
            sequence=0,
        )
        
        assert is_valid
        assert error is None
    
    def test_reject_unknown_pid(self):
        """Test rejecting unknown producer ID."""
        manager = DeduplicationManager()
        
        # Validate with unknown PID
        is_valid, error = manager.validate_produce_request(
            producer_id=999,
            producer_epoch=0,
            topic="test",
            partition=0,
            sequence=0,
        )
        
        assert not is_valid
        assert "Unknown" in error
    
    def test_handle_produce_success(self):
        """Test handling successful produce."""
        manager = DeduplicationManager()
        
        # Assign PID and produce
        pid_info = manager.pid_manager.assign_producer_id("client1")
        
        manager.handle_produce_success(
            producer_id=pid_info.producer_id,
            topic="test",
            partition=0,
            sequence=0,
            offset=100,
        )
        
        # Check sequence was updated
        last_seq = manager.sequence_manager.get_last_sequence(
            pid_info.producer_id,
            "test",
            0,
        )
        
        assert last_seq == 0


class TestIdempotentProducerConfig:
    """Test IdempotentProducerConfig."""
    
    def test_default_config(self):
        """Test default configuration."""
        config = IdempotentProducerConfig()
        
        assert config.enable_idempotence is True
        assert config.max_in_flight_requests == 5
        assert config.acks == "all"
    
    def test_validate_max_in_flight(self):
        """Test validation of max_in_flight_requests."""
        with pytest.raises(ValueError, match="max_in_flight_requests"):
            IdempotentProducerConfig(
                enable_idempotence=True,
                max_in_flight_requests=10,  # Too high
            )
    
    def test_validate_acks(self):
        """Test validation of acks requirement."""
        with pytest.raises(ValueError, match="acks"):
            IdempotentProducerConfig(
                enable_idempotence=True,
                acks="1",  # Must be "all"
            )


class TestIdempotentProducer:
    """Test IdempotentProducer."""
    
    def test_initialize(self):
        """Test producer initialization."""
        producer = IdempotentProducer(client_id="test-producer")
        
        assert producer.client_id == "test-producer"
        assert not producer.is_initialized()
    
    def test_init_producer_id(self):
        """Test initializing producer ID."""
        producer = IdempotentProducer(client_id="test-producer")
        pid_manager = ProducerIdManager()
        
        producer.init_producer_id(pid_manager)
        
        assert producer.is_initialized()
        assert producer.get_producer_id() is not None
    
    def test_prepare_send(self):
        """Test preparing idempotent send."""
        producer = IdempotentProducer(client_id="test-producer")
        pid_manager = ProducerIdManager()
        
        producer.init_producer_id(pid_manager)
        
        metadata = producer.prepare_send("test", 0)
        
        assert "producer_id" in metadata
        assert "producer_epoch" in metadata
        assert "sequence" in metadata
        assert metadata["sequence"] == 0
    
    def test_sequence_increments(self):
        """Test sequence number increments."""
        producer = IdempotentProducer(client_id="test-producer")
        pid_manager = ProducerIdManager()
        
        producer.init_producer_id(pid_manager)
        
        metadata1 = producer.prepare_send("test", 0)
        producer.handle_send_complete("test", 0, success=True)
        
        metadata2 = producer.prepare_send("test", 0)
        producer.handle_send_complete("test", 0, success=True)
        
        assert metadata2["sequence"] == metadata1["sequence"] + 1
    
    def test_max_in_flight_enforced(self):
        """Test max in-flight requests is enforced."""
        config = IdempotentProducerConfig(max_in_flight_requests=2)
        producer = IdempotentProducer(client_id="test-producer", config=config)
        pid_manager = ProducerIdManager()
        
        producer.init_producer_id(pid_manager)
        
        # Send up to max
        producer.prepare_send("test", 0)
        producer.prepare_send("test", 0)
        
        # Next should fail
        with pytest.raises(RuntimeError, match="Too many"):
            producer.prepare_send("test", 0)
