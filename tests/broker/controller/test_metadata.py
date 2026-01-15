"""
Tests for metadata propagation.
"""

import pytest

from distributedlog.broker.controller.metadata import (
    BrokerMetadataUpdate,
    MetadataCache,
    PartitionMetadataUpdate,
    UpdateMetadataRequest,
)


class TestMetadataCache:
    """Test MetadataCache."""
    
    def test_initialize(self):
        """Test cache initialization."""
        cache = MetadataCache(broker_id=1)
        
        assert cache.broker_id == 1
        assert cache.controller_id is None
        assert cache.controller_epoch == 0
        assert len(cache.partition_metadata) == 0
    
    @pytest.mark.asyncio
    async def test_handle_update_metadata(self):
        """Test handling metadata update."""
        cache = MetadataCache(broker_id=1)
        
        # Create update request
        partition_update = PartitionMetadataUpdate(
            topic="test",
            partition=0,
            leader=2,
            isr=[2, 3],
            replicas=[1, 2, 3],
            leader_epoch=1,
        )
        
        broker_update = BrokerMetadataUpdate(
            broker_id=2,
            host="broker2",
            port=9092,
        )
        
        request = UpdateMetadataRequest(
            controller_id=1,
            controller_epoch=1,
            partition_updates=[partition_update],
            broker_updates=[broker_update],
        )
        
        response = await cache.handle_update_metadata(request)
        
        assert response.error_code == 0
        assert cache.controller_id == 1
        assert cache.controller_epoch == 1
        
        # Check partition metadata
        assert ("test", 0) in cache.partition_metadata
        assert cache.partition_metadata[("test", 0)].leader == 2
        
        # Check broker metadata
        assert 2 in cache.broker_metadata
        assert cache.broker_metadata[2].host == "broker2"
    
    @pytest.mark.asyncio
    async def test_reject_stale_epoch(self):
        """Test rejecting stale controller epoch."""
        cache = MetadataCache(broker_id=1)
        cache.controller_epoch = 5
        
        # Create request with old epoch
        request = UpdateMetadataRequest(
            controller_id=1,
            controller_epoch=3,
            partition_updates=[],
            broker_updates=[],
        )
        
        response = await cache.handle_update_metadata(request)
        
        # Should reject
        assert response.error_code != 0
        assert "stale" in response.error_message.lower()
    
    def test_get_partition_leader(self):
        """Test getting partition leader from cache."""
        cache = MetadataCache(broker_id=1)
        
        # Add partition metadata
        update = PartitionMetadataUpdate(
            topic="test",
            partition=0,
            leader=2,
            isr=[2, 3],
            replicas=[1, 2, 3],
            leader_epoch=1,
        )
        
        cache.partition_metadata[("test", 0)] = update
        
        # Query leader
        leader = cache.get_partition_leader("test", 0)
        
        assert leader == 2
    
    def test_get_partition_replicas(self):
        """Test getting partition replicas from cache."""
        cache = MetadataCache(broker_id=1)
        
        # Add partition metadata
        update = PartitionMetadataUpdate(
            topic="test",
            partition=0,
            leader=2,
            isr=[2, 3],
            replicas=[1, 2, 3],
            leader_epoch=1,
        )
        
        cache.partition_metadata[("test", 0)] = update
        
        # Query replicas
        replicas = cache.get_partition_replicas("test", 0)
        
        assert replicas == [1, 2, 3]
    
    def test_get_broker_address(self):
        """Test getting broker address from cache."""
        cache = MetadataCache(broker_id=1)
        
        # Add broker metadata
        update = BrokerMetadataUpdate(
            broker_id=2,
            host="broker2",
            port=9092,
        )
        
        cache.broker_metadata[2] = update
        
        # Query address
        address = cache.get_broker_address(2)
        
        assert address == ("broker2", 9092)
    
    @pytest.mark.asyncio
    async def test_delete_topic_metadata(self):
        """Test deleting topic metadata."""
        cache = MetadataCache(broker_id=1)
        
        # Add partition metadata for multiple topics
        for i in range(3):
            update = PartitionMetadataUpdate(
                topic="test",
                partition=i,
                leader=1,
                isr=[1],
                replicas=[1, 2, 3],
                leader_epoch=1,
            )
            cache.partition_metadata[("test", i)] = update
        
        update2 = PartitionMetadataUpdate(
            topic="other",
            partition=0,
            leader=1,
            isr=[1],
            replicas=[1, 2, 3],
            leader_epoch=1,
        )
        cache.partition_metadata[("other", 0)] = update2
        
        # Delete topic
        request = UpdateMetadataRequest(
            controller_id=1,
            controller_epoch=2,
            partition_updates=[],
            broker_updates=[],
            deleted_topics=["test"],
        )
        
        await cache.handle_update_metadata(request)
        
        # Test topic partitions should be deleted
        assert ("test", 0) not in cache.partition_metadata
        assert ("test", 1) not in cache.partition_metadata
        assert ("test", 2) not in cache.partition_metadata
        
        # Other topic should remain
        assert ("other", 0) in cache.partition_metadata


class TestUpdateMetadataRequest:
    """Test UpdateMetadataRequest."""
    
    def test_create_request(self):
        """Test creating update request."""
        partition_update = PartitionMetadataUpdate(
            topic="test",
            partition=0,
            leader=1,
            isr=[1, 2],
            replicas=[1, 2, 3],
            leader_epoch=1,
        )
        
        request = UpdateMetadataRequest(
            controller_id=1,
            controller_epoch=5,
            partition_updates=[partition_update],
            broker_updates=[],
        )
        
        assert request.controller_id == 1
        assert request.controller_epoch == 5
        assert len(request.partition_updates) == 1
        assert request.deleted_topics == []
