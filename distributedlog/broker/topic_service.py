"""
Topic service for broker.

Provides unified interface for topic and partition management.
"""

from pathlib import Path
from typing import Dict, List, Optional

from distributedlog.core.topic.metadata import TopicConfig, TopicMetadata, TopicRegistry
from distributedlog.core.topic.partition_manager import PartitionManager
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class TopicService:
    """
    Service for managing topics and partitions on a broker.
    
    Coordinates between TopicRegistry and PartitionManager.
    """
    
    def __init__(
        self,
        data_dir: Path,
        broker_id: int = 0,
    ):
        """
        Initialize topic service.
        
        Args:
            data_dir: Base data directory
            broker_id: This broker's ID
        """
        self.data_dir = Path(data_dir)
        self.broker_id = broker_id
        
        metadata_dir = self.data_dir / "metadata"
        partitions_dir = self.data_dir / "partitions"
        
        self.topic_registry = TopicRegistry(metadata_dir)
        self.partition_manager = PartitionManager(
            partitions_dir,
            self.topic_registry,
            broker_id,
        )
        
        self._initialize_partitions()
        
        logger.info(
            "Initialized topic service",
            data_dir=str(data_dir),
            broker_id=broker_id,
        )
    
    def create_topic(
        self,
        name: str,
        num_partitions: int = 1,
        replication_factor: int = 1,
        **config,
    ) -> TopicMetadata:
        """
        Create a new topic.
        
        Args:
            name: Topic name
            num_partitions: Number of partitions
            replication_factor: Replication factor
            **config: Additional configuration
        
        Returns:
            Topic metadata
        """
        metadata = self.topic_registry.create_topic(
            name=name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            **config,
        )
        
        for partition_info in metadata.partitions:
            if self.broker_id in partition_info.replicas:
                self.partition_manager.create_partition(
                    name,
                    partition_info.partition,
                )
        
        logger.info(
            "Created topic",
            topic=name,
            num_partitions=num_partitions,
        )
        
        return metadata
    
    def get_topic(self, name: str) -> Optional[TopicMetadata]:
        """
        Get topic metadata.
        
        Args:
            name: Topic name
        
        Returns:
            Topic metadata or None
        """
        return self.topic_registry.get_topic(name)
    
    def list_topics(self) -> List[str]:
        """
        List all topics.
        
        Returns:
            List of topic names
        """
        return self.topic_registry.list_topics()
    
    def delete_topic(self, name: str) -> bool:
        """
        Delete a topic.
        
        Args:
            name: Topic name
        
        Returns:
            True if deleted, False if not found
        """
        metadata = self.topic_registry.get_topic(name)
        if not metadata:
            return False
        
        for partition_info in metadata.partitions:
            if self.broker_id in partition_info.replicas:
                self.partition_manager.delete_partition(
                    name,
                    partition_info.partition,
                )
        
        return self.topic_registry.delete_topic(name)
    
    def add_partitions(self, name: str, num_partitions: int) -> bool:
        """
        Add partitions to topic.
        
        Args:
            name: Topic name
            num_partitions: New total partition count
        
        Returns:
            True if successful, False otherwise
        """
        metadata = self.topic_registry.get_topic(name)
        if not metadata:
            return False
        
        old_count = len(metadata.partitions)
        
        success = self.topic_registry.add_partitions(name, num_partitions)
        
        if success:
            updated_metadata = self.topic_registry.get_topic(name)
            
            for partition_info in updated_metadata.partitions[old_count:]:
                if self.broker_id in partition_info.replicas:
                    self.partition_manager.create_partition(
                        name,
                        partition_info.partition,
                    )
            
            logger.info(
                "Added partitions",
                topic=name,
                old_count=old_count,
                new_count=num_partitions,
            )
        
        return success
    
    def get_partition_log(self, topic: str, partition: int):
        """
        Get log for partition.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            Log instance or None
        """
        return self.partition_manager.get_partition_log(topic, partition)
    
    def get_or_create_partition(self, topic: str, partition: int):
        """
        Get or create partition log.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            Log instance
        """
        metadata = self.topic_registry.get_topic(topic)
        if not metadata:
            metadata = self.create_topic(topic, num_partitions=max(partition + 1, 1))
        
        return self.partition_manager.get_or_create_partition(topic, partition)
    
    def get_partition_count(self, topic: str) -> int:
        """
        Get partition count for topic.
        
        Args:
            topic: Topic name
        
        Returns:
            Number of partitions (0 if topic not found)
        """
        metadata = self.topic_registry.get_topic(topic)
        if not metadata:
            return 0
        return len(metadata.partitions)
    
    def get_topic_stats(self, topic: str) -> Optional[dict]:
        """
        Get statistics for topic.
        
        Args:
            topic: Topic name
        
        Returns:
            Dictionary with stats or None
        """
        metadata = self.topic_registry.get_topic(topic)
        if not metadata:
            return None
        
        partition_stats = []
        for partition_info in metadata.partitions:
            if self.broker_id in partition_info.replicas:
                stats = self.partition_manager.get_partition_stats(
                    topic,
                    partition_info.partition,
                )
                if stats:
                    partition_stats.append(stats)
        
        return {
            'topic': topic,
            'num_partitions': len(metadata.partitions),
            'replication_factor': metadata.config.replication_factor,
            'partitions': partition_stats,
        }
    
    def _initialize_partitions(self) -> None:
        """Initialize partitions for existing topics."""
        for topic_name in self.topic_registry.list_topics():
            metadata = self.topic_registry.get_topic(topic_name)
            if metadata:
                for partition_info in metadata.partitions:
                    if self.broker_id in partition_info.replicas:
                        partition_dir = (
                            self.data_dir / "partitions" / topic_name /
                            f"partition-{partition_info.partition}"
                        )
                        
                        if partition_dir.exists():
                            self.partition_manager.get_or_create_partition(
                                topic_name,
                                partition_info.partition,
                            )
        
        logger.info("Initialized existing partitions")
    
    def close(self) -> None:
        """Close topic service and all partitions."""
        self.partition_manager.close_all()
        logger.info("Closed topic service")
