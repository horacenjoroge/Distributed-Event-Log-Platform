"""Tests for producer batching."""

import pytest

from distributedlog.producer.batch import (
    ProducerBatch,
    ProducerRecord,
    RecordAccumulator,
)


class TestProducerBatch:
    """Test ProducerBatch."""
    
    def test_batch_creation(self):
        """Test creating batch."""
        batch = ProducerBatch("test-topic", 0)
        
        assert batch.topic == "test-topic"
        assert batch.partition == 0
        assert len(batch.records) == 0
    
    def test_batch_append(self):
        """Test appending to batch."""
        batch = ProducerBatch("test-topic", 0)
        
        record = ProducerRecord(
            topic="test-topic",
            partition=0,
            key=b"key",
            value=b"value",
        )
        
        batch.append(record)
        
        assert len(batch.records) == 1
        assert batch.records[0] == record
    
    def test_batch_size(self):
        """Test batch size calculation."""
        batch = ProducerBatch("test-topic", 0)
        
        batch.append(ProducerRecord(
            topic="test-topic",
            partition=0,
            key=b"key",
            value=b"x" * 100,
        ))
        
        assert batch.size_bytes() == 103
    
    def test_batch_full(self):
        """Test batch full detection."""
        batch = ProducerBatch("test-topic", 0)
        
        batch.append(ProducerRecord(
            topic="test-topic",
            partition=0,
            key=None,
            value=b"x" * 100,
        ))
        
        assert not batch.is_full(1000)
        assert batch.is_full(50)


class TestRecordAccumulator:
    """Test RecordAccumulator."""
    
    def test_accumulator_creation(self):
        """Test creating accumulator."""
        accumulator = RecordAccumulator(
            batch_size=1000,
            linger_ms=10,
        )
        
        assert accumulator.batch_size == 1000
        assert accumulator.linger_ms == 10
    
    def test_accumulator_append(self):
        """Test appending to accumulator."""
        accumulator = RecordAccumulator(batch_size=1000)
        
        record = ProducerRecord(
            topic="test-topic",
            partition=0,
            key=None,
            value=b"value",
        )
        
        batch = accumulator.append(record, 0)
        
        assert batch is None
        assert accumulator.pending_count() == 1
    
    def test_accumulator_batch_full(self):
        """Test batch ready when full."""
        accumulator = RecordAccumulator(batch_size=100)
        
        record = ProducerRecord(
            topic="test-topic",
            partition=0,
            key=None,
            value=b"x" * 200,
        )
        
        batch = accumulator.append(record, 0)
        
        assert batch is not None
        assert len(batch.records) == 1
        assert accumulator.pending_count() == 0
    
    def test_drain_expired_batches(self):
        """Test draining expired batches."""
        import time
        
        accumulator = RecordAccumulator(
            batch_size=10000,
            linger_ms=50,
        )
        
        record = ProducerRecord(
            topic="test-topic",
            partition=0,
            key=None,
            value=b"value",
        )
        
        accumulator.append(record, 0)
        
        expired = accumulator.drain_expired_batches()
        assert len(expired) == 0
        
        time.sleep(0.06)
        
        expired = accumulator.drain_expired_batches()
        assert len(expired) == 1
        assert expired[0].topic == "test-topic"
    
    def test_drain_all(self):
        """Test draining all batches."""
        accumulator = RecordAccumulator(batch_size=10000)
        
        for i in range(5):
            record = ProducerRecord(
                topic="test-topic",
                partition=i,
                key=None,
                value=b"value",
            )
            accumulator.append(record, i)
        
        assert accumulator.pending_count() == 5
        
        batches = accumulator.drain_all()
        
        assert len(batches) == 5
        assert accumulator.pending_count() == 0
    
    def test_in_flight_tracking(self):
        """Test in-flight request tracking."""
        accumulator = RecordAccumulator(max_in_flight=2)
        
        accumulator.increment_in_flight("topic", 0)
        assert accumulator.can_send("topic", 0)
        
        accumulator.increment_in_flight("topic", 0)
        assert not accumulator.can_send("topic", 0)
        
        accumulator.decrement_in_flight("topic", 0)
        assert accumulator.can_send("topic", 0)
