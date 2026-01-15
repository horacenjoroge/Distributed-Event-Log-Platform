"""Tests for internal offset topic."""

import tempfile
import time
from pathlib import Path

import pytest

from distributedlog.consumer.offset import TopicPartition
from distributedlog.consumer.offset.offset_topic import (
    OFFSET_RETENTION_MS,
    OffsetKey,
    OffsetMetadata,
    OffsetTopic,
)


class TestOffsetKey:
    """Test OffsetKey."""
    
    def test_create_key(self):
        """Test creating offset key."""
        key = OffsetKey(
            group_id="test-group",
            topic="test-topic",
            partition=0,
        )
        
        assert key.group_id == "test-group"
        assert key.topic == "test-topic"
        assert key.partition == 0
    
    def test_key_to_string(self):
        """Test converting key to string."""
        key = OffsetKey("test-group", "test-topic", 5)
        
        key_str = key.to_string()
        assert key_str == "test-group:test-topic:5"
    
    def test_key_from_string(self):
        """Test parsing key from string."""
        key = OffsetKey.from_string("test-group:test-topic:5")
        
        assert key.group_id == "test-group"
        assert key.topic == "test-topic"
        assert key.partition == 5
    
    def test_key_roundtrip(self):
        """Test key serialization roundtrip."""
        original = OffsetKey("group1", "topic1", 3)
        
        key_str = original.to_string()
        parsed = OffsetKey.from_string(key_str)
        
        assert parsed == original


class TestOffsetMetadata:
    """Test OffsetMetadata."""
    
    def test_create_metadata(self):
        """Test creating offset metadata."""
        metadata = OffsetMetadata(offset=100, metadata="test")
        
        assert metadata.offset == 100
        assert metadata.metadata == "test"
        assert metadata.commit_timestamp > 0
        assert metadata.expire_timestamp > metadata.commit_timestamp
    
    def test_expiration(self):
        """Test offset expiration."""
        metadata = OffsetMetadata(offset=100)
        
        current_time = int(time.time() * 1000)
        assert not metadata.is_expired(current_time)
        
        future_time = current_time + OFFSET_RETENTION_MS + 1000
        assert metadata.is_expired(future_time)
    
    def test_metadata_to_dict(self):
        """Test converting metadata to dict."""
        metadata = OffsetMetadata(offset=200, metadata="meta")
        
        data = metadata.to_dict()
        
        assert data["offset"] == 200
        assert data["metadata"] == "meta"
        assert "commit_timestamp" in data
    
    def test_metadata_from_dict(self):
        """Test creating metadata from dict."""
        data = {
            "offset": 300,
            "metadata": "test",
            "commit_timestamp": 1000,
            "expire_timestamp": 2000,
        }
        
        metadata = OffsetMetadata.from_dict(data)
        
        assert metadata.offset == 300
        assert metadata.metadata == "test"
        assert metadata.commit_timestamp == 1000


class TestOffsetTopic:
    """Test OffsetTopic."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    @pytest.fixture
    def offset_topic(self, temp_dir):
        """Create offset topic."""
        topic = OffsetTopic(data_dir=temp_dir, num_partitions=3)
        yield topic
        topic.close()
    
    def test_create_offset_topic(self, temp_dir):
        """Test creating offset topic."""
        topic = OffsetTopic(data_dir=temp_dir, num_partitions=5)
        
        assert len(topic._partition_logs) == 5
        
        topic.close()
    
    def test_partition_for_key(self, offset_topic):
        """Test partition calculation."""
        key1 = OffsetKey("group1", "topic1", 0)
        key2 = OffsetKey("group1", "topic2", 0)
        
        partition1 = offset_topic._partition_for_key(key1)
        partition2 = offset_topic._partition_for_key(key2)
        
        assert partition1 == partition2  # Same group -> same partition
        assert 0 <= partition1 < 3
    
    def test_commit_and_fetch_offset(self, offset_topic):
        """Test committing and fetching offset."""
        tp = TopicPartition("test-topic", 0)
        
        offset_topic.commit_offset(
            group_id="test-group",
            topic_partition=tp,
            offset=100,
            metadata="test-meta",
        )
        
        fetched = offset_topic.fetch_offset(
            group_id="test-group",
            topic_partition=tp,
        )
        
        assert fetched is not None
        assert fetched.offset == 100
        assert fetched.metadata == "test-meta"
    
    def test_fetch_nonexistent_offset(self, offset_topic):
        """Test fetching nonexistent offset."""
        tp = TopicPartition("test-topic", 0)
        
        fetched = offset_topic.fetch_offset(
            group_id="nonexistent-group",
            topic_partition=tp,
        )
        
        assert fetched is None
    
    def test_update_offset(self, offset_topic):
        """Test updating committed offset."""
        tp = TopicPartition("test-topic", 0)
        
        offset_topic.commit_offset("test-group", tp, 100)
        offset_topic.commit_offset("test-group", tp, 200)
        
        fetched = offset_topic.fetch_offset("test-group", tp)
        
        assert fetched.offset == 200
    
    def test_fetch_all_offsets(self, offset_topic):
        """Test fetching all offsets for a group."""
        tp1 = TopicPartition("topic1", 0)
        tp2 = TopicPartition("topic1", 1)
        tp3 = TopicPartition("topic2", 0)
        
        offset_topic.commit_offset("test-group", tp1, 100)
        offset_topic.commit_offset("test-group", tp2, 200)
        offset_topic.commit_offset("test-group", tp3, 300)
        
        offsets = offset_topic.fetch_all_offsets("test-group")
        
        assert len(offsets) == 3
        assert offsets[tp1].offset == 100
        assert offsets[tp2].offset == 200
        assert offsets[tp3].offset == 300
    
    def test_delete_group_offsets(self, offset_topic):
        """Test deleting group offsets."""
        tp1 = TopicPartition("topic1", 0)
        tp2 = TopicPartition("topic1", 1)
        
        offset_topic.commit_offset("test-group", tp1, 100)
        offset_topic.commit_offset("test-group", tp2, 200)
        
        offset_topic.delete_group_offsets("test-group")
        
        offsets = offset_topic.fetch_all_offsets("test-group")
        
        assert len(offsets) == 0
    
    def test_get_active_groups(self, offset_topic):
        """Test getting active groups."""
        tp = TopicPartition("topic1", 0)
        
        offset_topic.commit_offset("group1", tp, 100)
        offset_topic.commit_offset("group2", tp, 200)
        offset_topic.commit_offset("group3", tp, 300)
        
        groups = offset_topic.get_active_groups()
        
        assert len(groups) == 3
        assert "group1" in groups
        assert "group2" in groups
        assert "group3" in groups
    
    def test_multiple_groups_same_partition(self, offset_topic):
        """Test multiple groups committing to same partition."""
        tp = TopicPartition("topic1", 0)
        
        offset_topic.commit_offset("group1", tp, 100)
        offset_topic.commit_offset("group2", tp, 200)
        
        offset1 = offset_topic.fetch_offset("group1", tp)
        offset2 = offset_topic.fetch_offset("group2", tp)
        
        assert offset1.offset == 100
        assert offset2.offset == 200
    
    def test_offset_compaction(self, offset_topic):
        """Test that compaction keeps only latest offset."""
        tp = TopicPartition("topic1", 0)
        
        for i in range(10):
            offset_topic.commit_offset("test-group", tp, i * 100)
        
        fetched = offset_topic.fetch_offset("test-group", tp)
        
        assert fetched.offset == 900  # Latest only
