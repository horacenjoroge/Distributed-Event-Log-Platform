"""Tests for Producer client."""

import tempfile
from pathlib import Path

import pytest

from distributedlog.producer import Producer, ProducerConfig, CompressionType


class TestProducer:
    """Test Producer client."""
    
    def test_producer_creation(self):
        """Test creating producer."""
        producer = Producer(bootstrap_servers="localhost:9092")
        
        assert producer.config.bootstrap_servers == "localhost:9092"
        assert not producer._closed
        
        producer.close()
    
    def test_sync_send(self):
        """Test synchronous send."""
        producer = Producer(bootstrap_servers="localhost:9092")
        
        future = producer.send("test-topic", value=b"test-value")
        
        metadata = future.result(timeout=5.0)
        
        assert metadata.topic == "test-topic"
        assert metadata.partition == 0
        
        producer.close()
    
    def test_send_with_key(self):
        """Test send with key."""
        producer = Producer(bootstrap_servers="localhost:9092")
        
        future = producer.send(
            "test-topic",
            key=b"test-key",
            value=b"test-value",
        )
        
        metadata = future.result(timeout=5.0)
        
        assert metadata.topic == "test-topic"
        
        producer.close()
    
    def test_async_send_with_callback(self):
        """Test async send with callback."""
        producer = Producer(bootstrap_servers="localhost:9092")
        
        callback_called = []
        
        def on_success(metadata):
            callback_called.append(metadata)
        
        producer.send(
            "test-topic",
            value=b"test-value",
            callback=on_success,
        )
        
        producer.flush()
        
        assert len(callback_called) == 1
        
        producer.close()
    
    def test_flush(self):
        """Test flushing producer."""
        producer = Producer(
            bootstrap_servers="localhost:9092",
            linger_ms=10000,
        )
        
        for i in range(10):
            producer.send("test-topic", value=f"msg-{i}".encode())
        
        producer.flush()
        
        producer.close()
    
    def test_context_manager(self):
        """Test producer as context manager."""
        with Producer(bootstrap_servers="localhost:9092") as producer:
            future = producer.send("test-topic", value=b"test")
            metadata = future.result(timeout=5.0)
            assert metadata.topic == "test-topic"
    
    def test_metrics(self):
        """Test producer metrics."""
        producer = Producer(bootstrap_servers="localhost:9092")
        
        metrics = producer.metrics()
        
        assert "pending_batches" in metrics
        assert "in_flight_requests" in metrics
        assert "metadata" in metrics
        assert metrics["closed"] is False
        
        producer.close()
    
    def test_compression(self):
        """Test producer with compression."""
        producer = Producer(
            bootstrap_servers="localhost:9092",
            compression_type=CompressionType.GZIP,
        )
        
        future = producer.send("test-topic", value=b"x" * 1000)
        metadata = future.result(timeout=5.0)
        
        assert metadata.topic == "test-topic"
        
        producer.close()
    
    def test_custom_partitioner(self):
        """Test producer with custom partitioner."""
        producer = Producer(
            bootstrap_servers="localhost:9092",
            partitioner_type="round_robin",
        )
        
        future = producer.send("test-topic", value=b"test")
        metadata = future.result(timeout=5.0)
        
        assert metadata.topic == "test-topic"
        
        producer.close()
