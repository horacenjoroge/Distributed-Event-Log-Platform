"""
Metadata cache for producer.

Caches topic-partition to broker mappings to avoid repeated lookups.
"""

import threading
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class BrokerMetadata:
    """
    Metadata about a broker.
    
    Attributes:
        broker_id: Unique broker identifier
        host: Broker hostname
        port: Broker port
    """
    broker_id: int
    host: str
    port: int
    
    def __str__(self) -> str:
        """String representation."""
        return f"{self.host}:{self.port}"


@dataclass
class PartitionMetadata:
    """
    Metadata about a partition.
    
    Attributes:
        topic: Topic name
        partition: Partition number
        leader: Leader broker ID
        replicas: List of replica broker IDs
        isr: In-sync replica broker IDs
    """
    topic: str
    partition: int
    leader: int
    replicas: List[int]
    isr: List[int]


@dataclass
class TopicMetadata:
    """
    Metadata about a topic.
    
    Attributes:
        topic: Topic name
        partitions: List of partition metadata
    """
    topic: str
    partitions: List[PartitionMetadata]
    
    def num_partitions(self) -> int:
        """Get number of partitions."""
        return len(self.partitions)
    
    def get_partition(self, partition: int) -> Optional[PartitionMetadata]:
        """Get metadata for specific partition."""
        for p in self.partitions:
            if p.partition == partition:
                return p
        return None


class MetadataCache:
    """
    Caches cluster metadata to reduce broker lookups.
    
    Maintains:
    - Broker list and connection info
    - Topic-partition to leader mappings
    - Partition counts per topic
    """
    
    def __init__(self, metadata_max_age_ms: int = 300000):
        """
        Initialize metadata cache.
        
        Args:
            metadata_max_age_ms: Max age before refresh (default: 5 minutes)
        """
        self.metadata_max_age_ms = metadata_max_age_ms
        
        self._brokers: Dict[int, BrokerMetadata] = {}
        self._topics: Dict[str, TopicMetadata] = {}
        self._last_update_ms: int = 0
        
        self._lock = threading.RLock()
        
        logger.info(
            "Initialized metadata cache",
            metadata_max_age_ms=metadata_max_age_ms,
        )
    
    def update_brokers(self, brokers: List[BrokerMetadata]) -> None:
        """
        Update broker list.
        
        Args:
            brokers: List of broker metadata
        """
        with self._lock:
            self._brokers.clear()
            for broker in brokers:
                self._brokers[broker.broker_id] = broker
            
            self._last_update_ms = int(time.time() * 1000)
            
            logger.info(
                "Updated broker metadata",
                broker_count=len(brokers),
            )
    
    def update_topic(self, topic_metadata: TopicMetadata) -> None:
        """
        Update topic metadata.
        
        Args:
            topic_metadata: Topic metadata
        """
        with self._lock:
            self._topics[topic_metadata.topic] = topic_metadata
            self._last_update_ms = int(time.time() * 1000)
            
            logger.info(
                "Updated topic metadata",
                topic=topic_metadata.topic,
                partitions=topic_metadata.num_partitions(),
            )
    
    def get_broker(self, broker_id: int) -> Optional[BrokerMetadata]:
        """
        Get broker by ID.
        
        Args:
            broker_id: Broker ID
        
        Returns:
            Broker metadata or None
        """
        with self._lock:
            return self._brokers.get(broker_id)
    
    def get_topic(self, topic: str) -> Optional[TopicMetadata]:
        """
        Get topic metadata.
        
        Args:
            topic: Topic name
        
        Returns:
            Topic metadata or None
        """
        with self._lock:
            return self._topics.get(topic)
    
    def get_partition_leader(self, topic: str, partition: int) -> Optional[int]:
        """
        Get leader broker ID for partition.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            Leader broker ID or None
        """
        with self._lock:
            topic_meta = self._topics.get(topic)
            if topic_meta is None:
                return None
            
            partition_meta = topic_meta.get_partition(partition)
            if partition_meta is None:
                return None
            
            return partition_meta.leader
    
    def get_partition_count(self, topic: str) -> int:
        """
        Get number of partitions for topic.
        
        Args:
            topic: Topic name
        
        Returns:
            Number of partitions, or 0 if topic not found
        """
        with self._lock:
            topic_meta = self._topics.get(topic)
            if topic_meta is None:
                return 0
            return topic_meta.num_partitions()
    
    def is_stale(self) -> bool:
        """
        Check if metadata is stale and needs refresh.
        
        Returns:
            True if metadata should be refreshed
        """
        with self._lock:
            if self._last_update_ms == 0:
                return True
            
            age_ms = int(time.time() * 1000) - self._last_update_ms
            return age_ms >= self.metadata_max_age_ms
    
    def needs_topic_metadata(self, topic: str) -> bool:
        """
        Check if topic metadata is missing or stale.
        
        Args:
            topic: Topic name
        
        Returns:
            True if metadata refresh needed
        """
        with self._lock:
            if topic not in self._topics:
                return True
            
            return self.is_stale()
    
    def get_all_brokers(self) -> List[BrokerMetadata]:
        """
        Get list of all brokers.
        
        Returns:
            List of broker metadata
        """
        with self._lock:
            return list(self._brokers.values())
    
    def get_random_broker(self) -> Optional[BrokerMetadata]:
        """
        Get random broker for initial connection.
        
        Returns:
            Random broker or None
        """
        with self._lock:
            brokers = list(self._brokers.values())
            if not brokers:
                return None
            
            import random
            return random.choice(brokers)
    
    def clear(self) -> None:
        """Clear all cached metadata."""
        with self._lock:
            self._brokers.clear()
            self._topics.clear()
            self._last_update_ms = 0
            
            logger.info("Cleared metadata cache")
    
    def get_stats(self) -> dict:
        """
        Get cache statistics.
        
        Returns:
            Dictionary with cache stats
        """
        with self._lock:
            age_ms = 0
            if self._last_update_ms > 0:
                age_ms = int(time.time() * 1000) - self._last_update_ms
            
            return {
                "broker_count": len(self._brokers),
                "topic_count": len(self._topics),
                "age_ms": age_ms,
                "is_stale": self.is_stale(),
            }


class BootstrapServerParser:
    """Parses bootstrap server strings."""
    
    @staticmethod
    def parse(bootstrap_servers: str | List[str]) -> List[BrokerMetadata]:
        """
        Parse bootstrap servers into broker metadata.
        
        Args:
            bootstrap_servers: Single string "host:port" or list of strings
        
        Returns:
            List of broker metadata
        
        Example:
            parse("localhost:9092")
            parse(["broker1:9092", "broker2:9092"])
        """
        if isinstance(bootstrap_servers, str):
            bootstrap_servers = [bootstrap_servers]
        
        brokers = []
        
        for i, server in enumerate(bootstrap_servers):
            parts = server.split(":")
            if len(parts) != 2:
                raise ValueError(f"Invalid bootstrap server format: {server}")
            
            host = parts[0]
            try:
                port = int(parts[1])
            except ValueError:
                raise ValueError(f"Invalid port in bootstrap server: {server}")
            
            brokers.append(BrokerMetadata(
                broker_id=i,
                host=host,
                port=port,
            ))
        
        logger.info(
            "Parsed bootstrap servers",
            count=len(brokers),
            servers=bootstrap_servers,
        )
        
        return brokers
