"""Tests for consumer offset management."""

import time

import pytest

from distributedlog.consumer.offset import (
    AutoCommitManager,
    OffsetAndMetadata,
    OffsetManager,
    TopicPartition,
)


class TestTopicPartition:
    """Test TopicPartition."""
    
    def test_creation(self):
        """Test creating topic-partition."""
        tp = TopicPartition("test-topic", 0)
        
        assert tp.topic == "test-topic"
        assert tp.partition == 0
    
    def test_equality(self):
        """Test equality comparison."""
        tp1 = TopicPartition("test-topic", 0)
        tp2 = TopicPartition("test-topic", 0)
        tp3 = TopicPartition("test-topic", 1)
        
        assert tp1 == tp2
        assert tp1 != tp3
    
    def test_hashable(self):
        """Test can be used as dict key."""
        tp1 = TopicPartition("test-topic", 0)
        tp2 = TopicPartition("test-topic", 0)
        
        d = {tp1: "value"}
        assert d[tp2] == "value"


class TestOffsetManager:
    """Test OffsetManager."""
    
    def test_creation(self):
        """Test creating offset manager."""
        manager = OffsetManager("test-group")
        
        assert manager.group_id == "test-group"
    
    def test_update_position(self):
        """Test updating position."""
        manager = OffsetManager("test-group")
        tp = TopicPartition("test-topic", 0)
        
        manager.update_position(tp, 100)
        
        assert manager.position(tp) == 100
    
    def test_commit(self):
        """Test committing offsets."""
        manager = OffsetManager("test-group")
        tp = TopicPartition("test-topic", 0)
        
        offsets = {tp: OffsetAndMetadata(offset=100)}
        manager.commit(offsets)
        
        committed = manager.committed(tp)
        assert committed.offset == 100
    
    def test_seek(self):
        """Test seeking to offset."""
        manager = OffsetManager("test-group")
        tp = TopicPartition("test-topic", 0)
        
        manager.update_position(tp, 100)
        manager.seek(tp, 50)
        
        assert manager.position(tp) == 50
    
    def test_reset_positions(self):
        """Test resetting positions."""
        manager = OffsetManager("test-group")
        tp1 = TopicPartition("test-topic", 0)
        tp2 = TopicPartition("test-topic", 1)
        
        manager.update_position(tp1, 100)
        manager.update_position(tp2, 200)
        
        manager.reset_positions({tp1})
        
        assert manager.position(tp1) is None
        assert manager.position(tp2) == 200
    
    def test_get_offsets_to_commit(self):
        """Test getting offsets to commit."""
        manager = OffsetManager("test-group")
        tp = TopicPartition("test-topic", 0)
        
        manager.update_position(tp, 100)
        
        offsets = manager.get_offsets_to_commit()
        
        assert tp in offsets
        assert offsets[tp].offset == 100


class TestAutoCommitManager:
    """Test AutoCommitManager."""
    
    def test_creation(self):
        """Test creating auto-commit manager."""
        offset_manager = OffsetManager("test-group")
        auto_commit = AutoCommitManager(offset_manager, 1000)
        
        assert auto_commit.auto_commit_interval_ms == 1000
    
    def test_auto_commit(self):
        """Test automatic commits."""
        offset_manager = OffsetManager("test-group")
        tp = TopicPartition("test-topic", 0)
        
        offset_manager.update_position(tp, 100)
        
        auto_commit = AutoCommitManager(offset_manager, 100)
        auto_commit.start()
        
        time.sleep(0.15)
        
        auto_commit.stop()
        
        committed = offset_manager.committed(tp)
        assert committed is not None
        assert committed.offset == 100
