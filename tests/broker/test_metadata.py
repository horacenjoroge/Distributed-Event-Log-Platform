"""Tests for broker metadata."""

import time

import pytest

from distributedlog.broker.metadata import (
    BrokerMetadata,
    BrokerState,
    ClusterMetadata,
    PartitionMetadata,
    get_local_hostname,
    get_local_ip,
)


class TestBrokerMetadata:
    """Test BrokerMetadata."""
    
    def test_create_broker(self):
        """Test creating broker metadata."""
        broker = BrokerMetadata(
            broker_id=1,
            host="localhost",
            port=9092,
            rack="rack1",
        )
        
        assert broker.broker_id == 1
        assert broker.host == "localhost"
        assert broker.port == 9092
        assert broker.rack == "rack1"
        assert broker.state == BrokerState.STARTING
    
    def test_endpoint(self):
        """Test endpoint generation."""
        broker = BrokerMetadata(
            broker_id=1,
            host="localhost",
            port=9092,
        )
        
        assert broker.endpoint() == "localhost:9092"
    
    def test_update_heartbeat(self):
        """Test heartbeat update."""
        broker = BrokerMetadata(broker_id=1, host="localhost", port=9092)
        
        old_heartbeat = broker.last_heartbeat
        time.sleep(0.01)
        broker.update_heartbeat()
        
        assert broker.last_heartbeat > old_heartbeat
    
    def test_is_healthy(self):
        """Test health checking."""
        broker = BrokerMetadata(
            broker_id=1,
            host="localhost",
            port=9092,
            state=BrokerState.RUNNING,
        )
        
        assert broker.is_healthy(timeout_ms=30000)
        
        broker.last_heartbeat = int(time.time() * 1000) - 40000
        assert not broker.is_healthy(timeout_ms=30000)
    
    def test_unhealthy_state(self):
        """Test unhealthy state."""
        broker = BrokerMetadata(
            broker_id=1,
            host="localhost",
            port=9092,
            state=BrokerState.FAILED,
        )
        
        assert not broker.is_healthy()
    
    def test_to_dict(self):
        """Test serialization to dict."""
        broker = BrokerMetadata(
            broker_id=1,
            host="localhost",
            port=9092,
        )
        
        data = broker.to_dict()
        
        assert data["broker_id"] == 1
        assert data["host"] == "localhost"
        assert data["port"] == 9092
    
    def test_from_dict(self):
        """Test deserialization from dict."""
        data = {
            "broker_id": 1,
            "host": "localhost",
            "port": 9092,
            "state": "running",
        }
        
        broker = BrokerMetadata.from_dict(data)
        
        assert broker.broker_id == 1
        assert broker.host == "localhost"
        assert broker.port == 9092
        assert broker.state == BrokerState.RUNNING


class TestPartitionMetadata:
    """Test PartitionMetadata."""
    
    def test_create_partition(self):
        """Test creating partition metadata."""
        partition = PartitionMetadata(
            topic="test-topic",
            partition=0,
            leader=1,
            replicas=[1, 2, 3],
            isr=[1, 2],
        )
        
        assert partition.topic == "test-topic"
        assert partition.partition == 0
        assert partition.leader == 1
        assert partition.replicas == [1, 2, 3]
        assert partition.isr == [1, 2]
    
    def test_is_under_replicated(self):
        """Test under-replication detection."""
        partition = PartitionMetadata(
            topic="test-topic",
            partition=0,
            leader=1,
            replicas=[1, 2, 3],
            isr=[1],  # Only 1 in-sync
        )
        
        assert partition.is_under_replicated()
        
        partition.isr = [1, 2, 3]
        assert not partition.is_under_replicated()
    
    def test_is_leader(self):
        """Test leader checking."""
        partition = PartitionMetadata(
            topic="test-topic",
            partition=0,
            leader=1,
        )
        
        assert partition.is_leader(1)
        assert not partition.is_leader(2)
    
    def test_is_replica(self):
        """Test replica checking."""
        partition = PartitionMetadata(
            topic="test-topic",
            partition=0,
            leader=1,
            replicas=[1, 2, 3],
        )
        
        assert partition.is_replica(1)
        assert partition.is_replica(2)
        assert not partition.is_replica(4)
    
    def test_is_in_sync(self):
        """Test in-sync checking."""
        partition = PartitionMetadata(
            topic="test-topic",
            partition=0,
            leader=1,
            replicas=[1, 2, 3],
            isr=[1, 2],
        )
        
        assert partition.is_in_sync(1)
        assert partition.is_in_sync(2)
        assert not partition.is_in_sync(3)


class TestClusterMetadata:
    """Test ClusterMetadata."""
    
    def test_create_cluster(self):
        """Test creating cluster metadata."""
        cluster = ClusterMetadata(cluster_id="test-cluster")
        
        assert cluster.cluster_id == "test-cluster"
        assert len(cluster.brokers) == 0
        assert len(cluster.partitions) == 0
        assert cluster.controller_id is None
    
    def test_add_broker(self):
        """Test adding broker."""
        cluster = ClusterMetadata(cluster_id="test-cluster")
        
        broker = BrokerMetadata(broker_id=1, host="localhost", port=9092)
        cluster.add_broker(broker)
        
        assert len(cluster.brokers) == 1
        assert cluster.get_broker(1) == broker
    
    def test_remove_broker(self):
        """Test removing broker."""
        cluster = ClusterMetadata(cluster_id="test-cluster")
        
        broker = BrokerMetadata(broker_id=1, host="localhost", port=9092)
        cluster.add_broker(broker)
        cluster.remove_broker(1)
        
        assert len(cluster.brokers) == 0
        assert cluster.get_broker(1) is None
    
    def test_get_live_brokers(self):
        """Test getting live brokers."""
        cluster = ClusterMetadata(cluster_id="test-cluster")
        
        broker1 = BrokerMetadata(
            broker_id=1,
            host="localhost",
            port=9092,
            state=BrokerState.RUNNING,
        )
        broker2 = BrokerMetadata(
            broker_id=2,
            host="localhost",
            port=9093,
            state=BrokerState.FAILED,
        )
        
        cluster.add_broker(broker1)
        cluster.add_broker(broker2)
        
        live_brokers = cluster.get_live_brokers()
        
        assert len(live_brokers) == 1
        assert live_brokers[0].broker_id == 1
    
    def test_add_partition(self):
        """Test adding partition."""
        cluster = ClusterMetadata(cluster_id="test-cluster")
        
        partition = PartitionMetadata(
            topic="test-topic",
            partition=0,
            leader=1,
        )
        cluster.add_partition(partition)
        
        assert len(cluster.partitions) == 1
        assert cluster.get_partition("test-topic", 0) == partition
    
    def test_get_partitions_for_topic(self):
        """Test getting partitions for topic."""
        cluster = ClusterMetadata(cluster_id="test-cluster")
        
        for i in range(3):
            partition = PartitionMetadata(
                topic="test-topic",
                partition=i,
                leader=1,
            )
            cluster.add_partition(partition)
        
        partitions = cluster.get_partitions_for_topic("test-topic")
        
        assert len(partitions) == 3
    
    def test_get_partitions_for_broker(self):
        """Test getting partitions for broker."""
        cluster = ClusterMetadata(cluster_id="test-cluster")
        
        partition1 = PartitionMetadata(
            topic="test-topic",
            partition=0,
            leader=1,
            replicas=[1, 2],
        )
        partition2 = PartitionMetadata(
            topic="test-topic",
            partition=1,
            leader=2,
            replicas=[2, 3],
        )
        
        cluster.add_partition(partition1)
        cluster.add_partition(partition2)
        
        partitions = cluster.get_partitions_for_broker(2)
        
        assert len(partitions) == 2
    
    def test_get_leader_partitions(self):
        """Test getting leader partitions."""
        cluster = ClusterMetadata(cluster_id="test-cluster")
        
        partition1 = PartitionMetadata(
            topic="test-topic",
            partition=0,
            leader=1,
        )
        partition2 = PartitionMetadata(
            topic="test-topic",
            partition=1,
            leader=2,
        )
        
        cluster.add_partition(partition1)
        cluster.add_partition(partition2)
        
        leader_partitions = cluster.get_leader_partitions(1)
        
        assert len(leader_partitions) == 1
        assert leader_partitions[0].partition == 0


class TestHelperFunctions:
    """Test helper functions."""
    
    def test_get_local_hostname(self):
        """Test getting local hostname."""
        hostname = get_local_hostname()
        
        assert isinstance(hostname, str)
        assert len(hostname) > 0
    
    def test_get_local_ip(self):
        """Test getting local IP."""
        ip = get_local_ip()
        
        assert isinstance(ip, str)
        assert len(ip) > 0
