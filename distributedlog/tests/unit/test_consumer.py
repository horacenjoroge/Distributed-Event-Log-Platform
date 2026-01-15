"""Tests for Consumer client."""

import tempfile
from pathlib import Path

import pytest

from distributedlog.consumer import Consumer, ConsumerConfig, TopicPartition
from distributedlog.producer import Producer


class TestConsumer:
    """Test Consumer client."""
    
    def test_consumer_creation(self):
        """Test creating consumer."""
        consumer = Consumer(
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            auto_commit=False,
        )
        
        assert consumer.config.group_id == "test-group"
        assert not consumer._closed
        
        consumer.close()
    
    def test_subscribe(self):
        """Test subscribing to topics."""
        consumer = Consumer(
            bootstrap_servers="localhost:9092",
            group_id="test-group",
        )
        
        consumer.subscribe(["test-topic"])
        
        assert "test-topic" in consumer.subscription()
        
        consumer.close()
    
    def test_poll_empty(self):
        """Test polling with no messages."""
        consumer = Consumer(
            bootstrap_servers="localhost:9092",
            group_id="test-group",
        )
        
        consumer.subscribe(["test-topic"])
        
        messages = consumer.poll(timeout_ms=100)
        
        assert len(messages) == 0
        
        consumer.close()
    
    def test_poll_with_messages(self):
        """Test polling with messages."""
        producer = Producer(bootstrap_servers="localhost:9092")
        
        for i in range(10):
            producer.send("test-topic", value=f"msg-{i}".encode())
        
        producer.flush()
        producer.close()
        
        consumer = Consumer(
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            auto_offset_reset="earliest",
        )
        
        consumer.subscribe(["test-topic"])
        
        messages = consumer.poll(timeout_ms=5000)
        
        assert len(messages) > 0
        
        for msg in messages:
            assert msg.topic == "test-topic"
            assert msg.value.startswith(b"msg-")
        
        consumer.close()
    
    def test_manual_commit(self):
        """Test manual commit."""
        consumer = Consumer(
            bootstrap_servers="localhost:9092",
            group_id="test-group",
            auto_commit=False,
        )
        
        consumer.subscribe(["test-topic"])
        
        consumer.commit()
        
        consumer.close()
    
    def test_seek(self):
        """Test seeking to offset."""
        consumer = Consumer(
            bootstrap_servers="localhost:9092",
            group_id="test-group",
        )
        
        consumer.subscribe(["test-topic"])
        
        tp = TopicPartition("test-topic", 0)
        consumer.seek(tp, 50)
        
        assert consumer.position(tp) == 50
        
        consumer.close()
    
    def test_seek_to_beginning(self):
        """Test seeking to beginning."""
        consumer = Consumer(
            bootstrap_servers="localhost:9092",
            group_id="test-group",
        )
        
        consumer.subscribe(["test-topic"])
        
        consumer.seek_to_beginning()
        
        tp = TopicPartition("test-topic", 0)
        assert consumer.position(tp) == 0
        
        consumer.close()
    
    def test_context_manager(self):
        """Test consumer as context manager."""
        with Consumer(
            bootstrap_servers="localhost:9092",
            group_id="test-group",
        ) as consumer:
            consumer.subscribe(["test-topic"])
            messages = consumer.poll(timeout_ms=100)
            assert isinstance(messages, list)
    
    def test_metrics(self):
        """Test consumer metrics."""
        consumer = Consumer(
            bootstrap_servers="localhost:9092",
            group_id="test-group",
        )
        
        consumer.subscribe(["test-topic"])
        
        metrics = consumer.metrics()
        
        assert metrics["group_id"] == "test-group"
        assert "test-topic" in metrics["subscriptions"]
        assert not metrics["closed"]
        
        consumer.close()
    
    def test_assignment(self):
        """Test partition assignment."""
        consumer = Consumer(
            bootstrap_servers="localhost:9092",
            group_id="test-group",
        )
        
        consumer.subscribe(["test-topic"])
        
        assignment = consumer.assignment()
        
        assert len(assignment) > 0
        assert all(isinstance(tp, TopicPartition) for tp in assignment)
        
        consumer.close()
