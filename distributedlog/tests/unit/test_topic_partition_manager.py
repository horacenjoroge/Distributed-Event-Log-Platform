"""Tests for partition manager."""

import tempfile
from pathlib import Path

import pytest

from distributedlog.core.topic.metadata import TopicRegistry
from distributedlog.core.topic.partition_manager import (
    PartitionManager,
    assign_partitions_round_robin,
    calculate_partition,
)


class TestCalculatePartition:
    """Test partition calculation."""
    
    def test_consistent_hashing(self):
        """Test same key always goes to same partition."""
        key = b"test-key"
        num_partitions = 10
        
        partition1 = calculate_partition(key, num_partitions)
        partition2 = calculate_partition(key, num_partitions)
        
        assert partition1 == partition2
    
    def test_within_range(self):
        """Test partition is within valid range."""
        key = b"test-key"
        num_partitions = 5
        
        partition = calculate_partition(key, num_partitions)
        
        assert 0 <= partition < num_partitions
    
    def test_different_keys_distribute(self):
        """Test different keys distribute across partitions."""
        num_partitions = 10
        
        partitions = set()
        for i in range(100):
            key = f"key-{i}".encode()
            partition = calculate_partition(key, num_partitions)
            partitions.add(partition)
        
        assert len(partitions) >= 5


class TestAssignPartitionsRoundRobin:
    """Test round-robin partition assignment."""
    
    def test_single_replica(self):
        """Test assignment with single replica."""
        assignments = assign_partitions_round_robin(
            num_partitions=6,
            num_brokers=3,
            replication_factor=1,
        )
        
        assert len(assignments) == 6
        
        for i, replicas in enumerate(assignments):
            assert len(replicas) == 1
            assert replicas[0] == i % 3
    
    def test_multiple_replicas(self):
        """Test assignment with multiple replicas."""
        assignments = assign_partitions_round_robin(
            num_partitions=3,
            num_brokers=3,
            replication_factor=2,
        )
        
        assert len(assignments) == 3
        
        for replicas in assignments:
            assert len(replicas) == 2
            assert replicas[0] != replicas[1]


class TestPartitionManager:
    """Test PartitionManager."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    @pytest.fixture
    def manager(self, temp_dir):
        """Create partition manager."""
        metadata_dir = temp_dir / "metadata"
        partitions_dir = temp_dir / "partitions"
        
        registry = TopicRegistry(metadata_dir)
        manager = PartitionManager(partitions_dir, registry, broker_id=0)
        
        return manager
    
    def test_creation(self, manager):
        """Test creating manager."""
        assert manager.broker_id == 0
    
    def test_create_partition(self, manager):
        """Test creating partition."""
        log = manager.create_partition("test-topic", 0)
        
        assert log is not None
    
    def test_get_partition_log(self, manager):
        """Test getting partition log."""
        manager.create_partition("test-topic", 0)
        
        log = manager.get_partition_log("test-topic", 0)
        
        assert log is not None
    
    def test_get_or_create_partition(self, manager):
        """Test get or create partition."""
        log1 = manager.get_or_create_partition("test-topic", 0)
        log2 = manager.get_or_create_partition("test-topic", 0)
        
        assert log1 is log2
    
    def test_list_partitions(self, manager):
        """Test listing partitions."""
        manager.create_partition("topic-1", 0)
        manager.create_partition("topic-1", 1)
        manager.create_partition("topic-2", 0)
        
        all_partitions = manager.list_partitions()
        assert len(all_partitions) == 3
        
        topic1_partitions = manager.list_partitions("topic-1")
        assert len(topic1_partitions) == 2
    
    def test_delete_partition(self, manager):
        """Test deleting partition."""
        manager.create_partition("test-topic", 0)
        
        success = manager.delete_partition("test-topic", 0)
        
        assert success
        assert manager.get_partition_log("test-topic", 0) is None
