"""Tests for producer partitioner."""

import pytest

from distributedlog.producer.partitioner import (
    DefaultPartitioner,
    KeyHashPartitioner,
    RoundRobinPartitioner,
    StickyPartitioner,
    create_partitioner,
)


class TestDefaultPartitioner:
    """Test DefaultPartitioner."""
    
    def test_hash_with_key(self):
        """Test key-based hashing."""
        partitioner = DefaultPartitioner()
        
        partition1 = partitioner.partition("topic", b"key1", b"value", 3)
        partition2 = partitioner.partition("topic", b"key1", b"value", 3)
        
        assert partition1 == partition2
        assert 0 <= partition1 < 3
    
    def test_round_robin_without_key(self):
        """Test round-robin without key."""
        partitioner = DefaultPartitioner()
        
        partitions = [
            partitioner.partition("topic", None, b"value", 3)
            for _ in range(9)
        ]
        
        assert partitions == [0, 1, 2, 0, 1, 2, 0, 1, 2]
    
    def test_different_keys_different_partitions(self):
        """Test different keys likely go to different partitions."""
        partitioner = DefaultPartitioner()
        
        partitions = set()
        for i in range(100):
            key = f"key-{i}".encode()
            partition = partitioner.partition("topic", key, b"value", 10)
            partitions.add(partition)
        
        assert len(partitions) > 5


class TestKeyHashPartitioner:
    """Test KeyHashPartitioner."""
    
    def test_requires_key(self):
        """Test partitioner requires key."""
        partitioner = KeyHashPartitioner()
        
        with pytest.raises(ValueError, match="requires key"):
            partitioner.partition("topic", None, b"value", 3)
    
    def test_consistent_hashing(self):
        """Test consistent hashing."""
        partitioner = KeyHashPartitioner()
        
        partitions = [
            partitioner.partition("topic", b"key1", b"value", 3)
            for _ in range(10)
        ]
        
        assert len(set(partitions)) == 1


class TestRoundRobinPartitioner:
    """Test RoundRobinPartitioner."""
    
    def test_even_distribution(self):
        """Test even distribution across partitions."""
        partitioner = RoundRobinPartitioner()
        
        partitions = [
            partitioner.partition("topic", None, b"value", 3)
            for _ in range(9)
        ]
        
        assert partitions == [0, 1, 2, 0, 1, 2, 0, 1, 2]


class TestStickyPartitioner:
    """Test StickyPartitioner."""
    
    def test_sticky_behavior(self):
        """Test stickiness for batching."""
        partitioner = StickyPartitioner()
        
        partitions = [
            partitioner.partition("topic", None, b"value", 3)
            for _ in range(10)
        ]
        
        assert len(set(partitions)) == 1
    
    def test_switches_after_batch(self):
        """Test switches partition after batch full."""
        partitioner = StickyPartitioner()
        
        partitions = [
            partitioner.partition("topic", None, b"value", 3)
            for _ in range(250)
        ]
        
        assert len(set(partitions)) > 1


class TestPartitionerFactory:
    """Test partitioner factory."""
    
    def test_create_default(self):
        """Test creating default partitioner."""
        partitioner = create_partitioner("default")
        assert isinstance(partitioner, DefaultPartitioner)
    
    def test_create_key_hash(self):
        """Test creating key hash partitioner."""
        partitioner = create_partitioner("key_hash")
        assert isinstance(partitioner, KeyHashPartitioner)
    
    def test_unknown_type(self):
        """Test unknown partitioner type."""
        with pytest.raises(ValueError, match="Unknown partitioner"):
            create_partitioner("unknown")
