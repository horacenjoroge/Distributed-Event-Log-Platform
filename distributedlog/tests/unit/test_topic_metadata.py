"""Tests for topic metadata."""

import tempfile
from pathlib import Path

import pytest

from distributedlog.core.topic.metadata import (
    PartitionInfo,
    TopicConfig,
    TopicMetadata,
    TopicRegistry,
)


class TestPartitionInfo:
    """Test PartitionInfo."""
    
    def test_creation(self):
        """Test creating partition info."""
        info = PartitionInfo(
            topic="test-topic",
            partition=0,
            leader=1,
            replicas=[1, 2, 3],
            isr=[1, 2],
        )
        
        assert info.topic == "test-topic"
        assert info.partition == 0
        assert info.leader == 1
        assert len(info.replicas) == 3
        assert len(info.isr) == 2
    
    def test_hash(self):
        """Test partition info is hashable."""
        info1 = PartitionInfo("test-topic", 0, 1)
        info2 = PartitionInfo("test-topic", 0, 1)
        
        assert hash(info1) == hash(info2)
    
    def test_equality(self):
        """Test partition info equality."""
        info1 = PartitionInfo("test-topic", 0, 1)
        info2 = PartitionInfo("test-topic", 0, 1)
        info3 = PartitionInfo("test-topic", 1, 1)
        
        assert info1 == info2
        assert info1 != info3


class TestTopicConfig:
    """Test TopicConfig."""
    
    def test_creation(self):
        """Test creating topic config."""
        config = TopicConfig(
            name="test-topic",
            num_partitions=3,
            replication_factor=2,
        )
        
        assert config.name == "test-topic"
        assert config.num_partitions == 3
        assert config.replication_factor == 2
    
    def test_to_dict(self):
        """Test converting to dictionary."""
        config = TopicConfig(
            name="test-topic",
            num_partitions=3,
        )
        
        data = config.to_dict()
        
        assert data['name'] == "test-topic"
        assert data['num_partitions'] == 3
    
    def test_from_dict(self):
        """Test creating from dictionary."""
        data = {
            'name': "test-topic",
            'num_partitions': 3,
            'replication_factor': 2,
        }
        
        config = TopicConfig.from_dict(data)
        
        assert config.name == "test-topic"
        assert config.num_partitions == 3
        assert config.replication_factor == 2


class TestTopicMetadata:
    """Test TopicMetadata."""
    
    def test_creation(self):
        """Test creating topic metadata."""
        config = TopicConfig("test-topic", num_partitions=2)
        partitions = [
            PartitionInfo("test-topic", 0, 0),
            PartitionInfo("test-topic", 1, 0),
        ]
        
        metadata = TopicMetadata(config=config, partitions=partitions)
        
        assert metadata.config.name == "test-topic"
        assert metadata.num_partitions() == 2
    
    def test_get_partition(self):
        """Test getting partition by number."""
        config = TopicConfig("test-topic", num_partitions=2)
        partitions = [
            PartitionInfo("test-topic", 0, 0),
            PartitionInfo("test-topic", 1, 0),
        ]
        
        metadata = TopicMetadata(config=config, partitions=partitions)
        
        partition = metadata.get_partition(1)
        assert partition is not None
        assert partition.partition == 1


class TestTopicRegistry:
    """Test TopicRegistry."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    def test_creation(self, temp_dir):
        """Test creating registry."""
        registry = TopicRegistry(temp_dir)
        
        assert registry.data_dir == temp_dir
    
    def test_create_topic(self, temp_dir):
        """Test creating topic."""
        registry = TopicRegistry(temp_dir)
        
        metadata = registry.create_topic("test-topic", num_partitions=3)
        
        assert metadata.config.name == "test-topic"
        assert metadata.num_partitions() == 3
    
    def test_create_duplicate_topic(self, temp_dir):
        """Test creating duplicate topic fails."""
        registry = TopicRegistry(temp_dir)
        
        registry.create_topic("test-topic")
        
        with pytest.raises(ValueError, match="already exists"):
            registry.create_topic("test-topic")
    
    def test_get_topic(self, temp_dir):
        """Test getting topic."""
        registry = TopicRegistry(temp_dir)
        
        registry.create_topic("test-topic")
        
        metadata = registry.get_topic("test-topic")
        assert metadata is not None
        assert metadata.config.name == "test-topic"
    
    def test_list_topics(self, temp_dir):
        """Test listing topics."""
        registry = TopicRegistry(temp_dir)
        
        registry.create_topic("topic-1")
        registry.create_topic("topic-2")
        
        topics = registry.list_topics()
        
        assert len(topics) == 2
        assert "topic-1" in topics
        assert "topic-2" in topics
    
    def test_delete_topic(self, temp_dir):
        """Test deleting topic."""
        registry = TopicRegistry(temp_dir)
        
        registry.create_topic("test-topic")
        
        success = registry.delete_topic("test-topic")
        
        assert success
        assert registry.get_topic("test-topic") is None
    
    def test_add_partitions(self, temp_dir):
        """Test adding partitions."""
        registry = TopicRegistry(temp_dir)
        
        registry.create_topic("test-topic", num_partitions=2)
        
        success = registry.add_partitions("test-topic", 5)
        
        assert success
        
        metadata = registry.get_topic("test-topic")
        assert metadata.num_partitions() == 5
    
    def test_add_partitions_cannot_reduce(self, temp_dir):
        """Test cannot reduce partition count."""
        registry = TopicRegistry(temp_dir)
        
        registry.create_topic("test-topic", num_partitions=5)
        
        success = registry.add_partitions("test-topic", 3)
        
        assert not success
    
    def test_persistence(self, temp_dir):
        """Test metadata persistence."""
        registry1 = TopicRegistry(temp_dir)
        registry1.create_topic("test-topic", num_partitions=3)
        
        registry2 = TopicRegistry(temp_dir)
        
        metadata = registry2.get_topic("test-topic")
        assert metadata is not None
        assert metadata.num_partitions() == 3
