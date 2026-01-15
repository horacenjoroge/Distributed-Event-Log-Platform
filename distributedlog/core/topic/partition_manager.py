"""
Partition manager for handling partition assignment and storage.

Manages the mapping between logical partitions and physical storage.
"""

import threading
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from distributedlog.core.log.log import Log
from distributedlog.core.topic.metadata import PartitionInfo, TopicMetadata, TopicRegistry
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class PartitionManager:
    """
    Manages partitions and their underlying storage.
    
    Responsibilities:
    - Create partition directories
    - Manage partition-to-log mapping
    - Handle partition assignment
    - Coordinate with topic registry
    """
    
    def __init__(
        self,
        base_dir: Path,
        topic_registry: TopicRegistry,
        broker_id: int = 0,
    ):
        """
        Initialize partition manager.
        
        Args:
            base_dir: Base directory for partition data
            topic_registry: Topic registry instance
            broker_id: This broker's ID
        """
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        
        self.topic_registry = topic_registry
        self.broker_id = broker_id
        
        self._logs: Dict[Tuple[str, int], Log] = {}
        self._lock = threading.RLock()
        
        logger.info(
            "Initialized partition manager",
            base_dir=str(base_dir),
            broker_id=broker_id,
        )
    
    def create_partition(
        self,
        topic: str,
        partition: int,
        config: Optional[dict] = None,
    ) -> Log:
        """
        Create a partition and its underlying log.
        
        Args:
            topic: Topic name
            partition: Partition number
            config: Optional log configuration
        
        Returns:
            Log instance for partition
        """
        with self._lock:
            key = (topic, partition)
            
            if key in self._logs:
                logger.warning(
                    "Partition already exists",
                    topic=topic,
                    partition=partition,
                )
                return self._logs[key]
            
            partition_dir = self._get_partition_dir(topic, partition)
            partition_dir.mkdir(parents=True, exist_ok=True)
            
            topic_metadata = self.topic_registry.get_topic(topic)
            if topic_metadata:
                log_config = {
                    'max_segment_size': topic_metadata.config.segment_bytes,
                    'max_segment_age_ms': topic_metadata.config.segment_ms,
                    'retention_hours': topic_metadata.config.retention_hours,
                    'retention_bytes': topic_metadata.config.retention_bytes,
                }
            else:
                log_config = {}
            
            if config:
                log_config.update(config)
            
            log = Log(
                directory=partition_dir,
                **log_config,
            )
            
            self._logs[key] = log
            
            logger.info(
                "Created partition",
                topic=topic,
                partition=partition,
                directory=str(partition_dir),
            )
            
            return log
    
    def get_partition_log(
        self,
        topic: str,
        partition: int,
    ) -> Optional[Log]:
        """
        Get log for partition.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            Log instance or None
        """
        with self._lock:
            key = (topic, partition)
            return self._logs.get(key)
    
    def get_or_create_partition(
        self,
        topic: str,
        partition: int,
    ) -> Log:
        """
        Get existing partition or create new one.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            Log instance
        """
        log = self.get_partition_log(topic, partition)
        if log is None:
            log = self.create_partition(topic, partition)
        return log
    
    def delete_partition(
        self,
        topic: str,
        partition: int,
    ) -> bool:
        """
        Delete a partition.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            True if deleted, False if not found
        """
        with self._lock:
            key = (topic, partition)
            
            if key not in self._logs:
                return False
            
            log = self._logs[key]
            log.close()
            
            del self._logs[key]
            
            logger.info(
                "Deleted partition",
                topic=topic,
                partition=partition,
            )
            
            return True
    
    def list_partitions(
        self,
        topic: Optional[str] = None,
    ) -> List[Tuple[str, int]]:
        """
        List partitions.
        
        Args:
            topic: Topic name (None = all topics)
        
        Returns:
            List of (topic, partition) tuples
        """
        with self._lock:
            if topic is None:
                return list(self._logs.keys())
            
            return [
                key for key in self._logs.keys()
                if key[0] == topic
            ]
    
    def get_partition_assignments(
        self,
        topic: str,
    ) -> List[int]:
        """
        Get partitions assigned to this broker.
        
        Args:
            topic: Topic name
        
        Returns:
            List of partition numbers
        """
        topic_metadata = self.topic_registry.get_topic(topic)
        if not topic_metadata:
            return []
        
        return [
            p.partition
            for p in topic_metadata.partitions
            if self.broker_id in p.replicas
        ]
    
    def assign_partition(
        self,
        topic: str,
        partition: int,
        leader: int,
        replicas: List[int],
    ) -> None:
        """
        Assign partition to brokers.
        
        Args:
            topic: Topic name
            partition: Partition number
            leader: Leader broker ID
            replicas: Replica broker IDs
        """
        topic_metadata = self.topic_registry.get_topic(topic)
        if not topic_metadata:
            logger.error(
                "Cannot assign partition, topic not found",
                topic=topic,
            )
            return
        
        partition_info = topic_metadata.get_partition(partition)
        if not partition_info:
            logger.error(
                "Cannot assign partition, partition not found",
                topic=topic,
                partition=partition,
            )
            return
        
        partition_info.leader = leader
        partition_info.replicas = replicas
        partition_info.isr = replicas.copy()
        
        logger.info(
            "Assigned partition",
            topic=topic,
            partition=partition,
            leader=leader,
            replicas=replicas,
        )
    
    def _get_partition_dir(self, topic: str, partition: int) -> Path:
        """
        Get partition directory path.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            Path to partition directory
        """
        return self.base_dir / topic / f"partition-{partition}"
    
    def close_all(self) -> None:
        """Close all partition logs."""
        with self._lock:
            for log in self._logs.values():
                log.close()
            
            self._logs.clear()
            
            logger.info("Closed all partitions")
    
    def get_partition_stats(
        self,
        topic: str,
        partition: int,
    ) -> Optional[dict]:
        """
        Get statistics for partition.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            Dictionary with stats or None
        """
        log = self.get_partition_log(topic, partition)
        if not log:
            return None
        
        return {
            'topic': topic,
            'partition': partition,
            'segment_count': log.segment_count(),
            'earliest_offset': log.get_earliest_offset(),
            'latest_offset': log.get_latest_offset(),
        }


def calculate_partition(key: bytes, num_partitions: int) -> int:
    """
    Calculate partition for key using hash.
    
    Args:
        key: Message key
        num_partitions: Number of partitions
    
    Returns:
        Partition number (0 to num_partitions-1)
    """
    import hashlib
    
    hash_value = int(hashlib.md5(key).hexdigest(), 16)
    return hash_value % num_partitions


def assign_partitions_round_robin(
    num_partitions: int,
    num_brokers: int,
    replication_factor: int = 1,
) -> List[List[int]]:
    """
    Assign partitions to brokers using round-robin.
    
    Args:
        num_partitions: Number of partitions
        num_brokers: Number of brokers
        replication_factor: Number of replicas per partition
    
    Returns:
        List of replica lists per partition
    """
    assignments = []
    
    for partition in range(num_partitions):
        replicas = []
        for i in range(replication_factor):
            broker_id = (partition + i) % num_brokers
            if broker_id not in replicas:
                replicas.append(broker_id)
        
        assignments.append(replicas)
    
    return assignments
