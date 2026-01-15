"""
Topic and partition metadata structures.

Defines the logical organization of data into topics and partitions.
"""

import json
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set

from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class PartitionInfo:
    """
    Information about a single partition.
    
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
    replicas: List[int] = field(default_factory=list)
    isr: List[int] = field(default_factory=list)
    
    def __hash__(self):
        return hash((self.topic, self.partition))
    
    def __eq__(self, other):
        if not isinstance(other, PartitionInfo):
            return False
        return self.topic == other.topic and self.partition == other.partition


@dataclass
class TopicConfig:
    """
    Configuration for a topic.
    
    Attributes:
        name: Topic name
        num_partitions: Number of partitions
        replication_factor: Replication factor
        retention_hours: Retention period in hours (-1 = unlimited)
        retention_bytes: Max size in bytes (-1 = unlimited)
        segment_bytes: Max segment size in bytes
        segment_ms: Max segment age in milliseconds
    """
    name: str
    num_partitions: int = 1
    replication_factor: int = 1
    retention_hours: int = -1
    retention_bytes: int = -1
    segment_bytes: int = 1073741824  # 1GB
    segment_ms: int = 604800000  # 7 days
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            'name': self.name,
            'num_partitions': self.num_partitions,
            'replication_factor': self.replication_factor,
            'retention_hours': self.retention_hours,
            'retention_bytes': self.retention_bytes,
            'segment_bytes': self.segment_bytes,
            'segment_ms': self.segment_ms,
        }
    
    @staticmethod
    def from_dict(data: dict) -> 'TopicConfig':
        """Create from dictionary."""
        return TopicConfig(
            name=data['name'],
            num_partitions=data.get('num_partitions', 1),
            replication_factor=data.get('replication_factor', 1),
            retention_hours=data.get('retention_hours', -1),
            retention_bytes=data.get('retention_bytes', -1),
            segment_bytes=data.get('segment_bytes', 1073741824),
            segment_ms=data.get('segment_ms', 604800000),
        )


@dataclass
class TopicMetadata:
    """
    Complete metadata for a topic.
    
    Attributes:
        config: Topic configuration
        partitions: List of partition information
    """
    config: TopicConfig
    partitions: List[PartitionInfo] = field(default_factory=list)
    
    def num_partitions(self) -> int:
        """Get number of partitions."""
        return len(self.partitions)
    
    def get_partition(self, partition: int) -> Optional[PartitionInfo]:
        """
        Get partition info by number.
        
        Args:
            partition: Partition number
        
        Returns:
            Partition info or None
        """
        for p in self.partitions:
            if p.partition == partition:
                return p
        return None
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            'config': self.config.to_dict(),
            'partitions': [
                {
                    'topic': p.topic,
                    'partition': p.partition,
                    'leader': p.leader,
                    'replicas': p.replicas,
                    'isr': p.isr,
                }
                for p in self.partitions
            ],
        }
    
    @staticmethod
    def from_dict(data: dict) -> 'TopicMetadata':
        """Create from dictionary."""
        config = TopicConfig.from_dict(data['config'])
        partitions = [
            PartitionInfo(
                topic=p['topic'],
                partition=p['partition'],
                leader=p['leader'],
                replicas=p.get('replicas', []),
                isr=p.get('isr', []),
            )
            for p in data.get('partitions', [])
        ]
        return TopicMetadata(config=config, partitions=partitions)


class TopicRegistry:
    """
    Registry for all topics and their metadata.
    
    Maintains in-memory state and persists to disk.
    """
    
    def __init__(self, data_dir: Path):
        """
        Initialize topic registry.
        
        Args:
            data_dir: Directory for storing metadata
        """
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        self._topics: Dict[str, TopicMetadata] = {}
        self._lock = threading.RLock()
        
        self._load_metadata()
        
        logger.info("Initialized topic registry", data_dir=str(data_dir))
    
    def create_topic(
        self,
        name: str,
        num_partitions: int = 1,
        replication_factor: int = 1,
        **kwargs,
    ) -> TopicMetadata:
        """
        Create a new topic.
        
        Args:
            name: Topic name
            num_partitions: Number of partitions
            replication_factor: Replication factor
            **kwargs: Additional config options
        
        Returns:
            Topic metadata
        
        Raises:
            ValueError: If topic already exists
        """
        with self._lock:
            if name in self._topics:
                raise ValueError(f"Topic already exists: {name}")
            
            config = TopicConfig(
                name=name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                **kwargs,
            )
            
            partitions = []
            for i in range(num_partitions):
                partition_info = PartitionInfo(
                    topic=name,
                    partition=i,
                    leader=0,
                    replicas=[0],
                    isr=[0],
                )
                partitions.append(partition_info)
            
            metadata = TopicMetadata(config=config, partitions=partitions)
            self._topics[name] = metadata
            
            self._save_metadata()
            
            logger.info(
                "Created topic",
                topic=name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
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
        with self._lock:
            return self._topics.get(name)
    
    def list_topics(self) -> List[str]:
        """
        List all topic names.
        
        Returns:
            List of topic names
        """
        with self._lock:
            return list(self._topics.keys())
    
    def delete_topic(self, name: str) -> bool:
        """
        Delete a topic.
        
        Args:
            name: Topic name
        
        Returns:
            True if deleted, False if not found
        """
        with self._lock:
            if name not in self._topics:
                return False
            
            del self._topics[name]
            self._save_metadata()
            
            logger.info("Deleted topic", topic=name)
            
            return True
    
    def add_partitions(self, name: str, num_partitions: int) -> bool:
        """
        Add partitions to existing topic.
        
        Args:
            name: Topic name
            num_partitions: Total partition count (must be > current)
        
        Returns:
            True if successful, False otherwise
        """
        with self._lock:
            metadata = self._topics.get(name)
            if not metadata:
                return False
            
            current_count = len(metadata.partitions)
            if num_partitions <= current_count:
                logger.warning(
                    "Cannot reduce partitions",
                    topic=name,
                    current=current_count,
                    requested=num_partitions,
                )
                return False
            
            for i in range(current_count, num_partitions):
                partition_info = PartitionInfo(
                    topic=name,
                    partition=i,
                    leader=0,
                    replicas=[0],
                    isr=[0],
                )
                metadata.partitions.append(partition_info)
            
            metadata.config.num_partitions = num_partitions
            
            self._save_metadata()
            
            logger.info(
                "Added partitions",
                topic=name,
                old_count=current_count,
                new_count=num_partitions,
            )
            
            return True
    
    def _save_metadata(self) -> None:
        """Save metadata to disk."""
        metadata_file = self.data_dir / "topics.json"
        
        data = {
            name: metadata.to_dict()
            for name, metadata in self._topics.items()
        }
        
        with open(metadata_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        logger.debug("Saved topic metadata", topics=len(self._topics))
    
    def _load_metadata(self) -> None:
        """Load metadata from disk."""
        metadata_file = self.data_dir / "topics.json"
        
        if not metadata_file.exists():
            logger.info("No existing topic metadata found")
            return
        
        try:
            with open(metadata_file, 'r') as f:
                data = json.load(f)
            
            for name, topic_data in data.items():
                metadata = TopicMetadata.from_dict(topic_data)
                self._topics[name] = metadata
            
            logger.info(
                "Loaded topic metadata",
                topics=len(self._topics),
            )
        
        except Exception as e:
            logger.error("Failed to load topic metadata", error=str(e))
