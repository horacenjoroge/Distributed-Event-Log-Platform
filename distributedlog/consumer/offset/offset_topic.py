"""
Internal offset topic for persisting consumer group offsets.

The __consumer_offsets topic is a special compacted topic that stores
committed offsets for all consumer groups.
"""

import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Optional, Set

from distributedlog.consumer.offset import TopicPartition
from distributedlog.core.log.log import Log
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)

INTERNAL_OFFSET_TOPIC = "__consumer_offsets"
OFFSET_TOPIC_NUM_PARTITIONS = 50  # Kafka uses 50 by default
OFFSET_RETENTION_MS = 7 * 24 * 60 * 60 * 1000  # 7 days


@dataclass
class OffsetMetadata:
    """
    Metadata about a committed offset.
    
    Attributes:
        offset: Committed offset
        metadata: Optional metadata string
        commit_timestamp: When offset was committed
        expire_timestamp: When offset expires
    """
    offset: int
    metadata: str = ""
    commit_timestamp: int = 0
    expire_timestamp: int = 0
    
    def __post_init__(self):
        if self.commit_timestamp == 0:
            self.commit_timestamp = int(time.time() * 1000)
        if self.expire_timestamp == 0:
            self.expire_timestamp = self.commit_timestamp + OFFSET_RETENTION_MS
    
    def is_expired(self, current_time_ms: int) -> bool:
        """
        Check if offset metadata has expired.
        
        Args:
            current_time_ms: Current time in milliseconds
        
        Returns:
            True if expired
        """
        return current_time_ms > self.expire_timestamp
    
    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> "OffsetMetadata":
        """Create from dictionary."""
        return cls(**data)


@dataclass
class OffsetKey:
    """
    Key for offset storage.
    
    Uniquely identifies a consumer group + topic-partition combination.
    """
    group_id: str
    topic: str
    partition: int
    
    def to_string(self) -> str:
        """Convert to string for use as log key."""
        return f"{self.group_id}:{self.topic}:{self.partition}"
    
    @classmethod
    def from_string(cls, key_str: str) -> "OffsetKey":
        """Parse from string."""
        parts = key_str.split(":")
        if len(parts) != 3:
            raise ValueError(f"Invalid offset key: {key_str}")
        return cls(
            group_id=parts[0],
            topic=parts[1],
            partition=int(parts[2]),
        )
    
    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return asdict(self)
    
    @classmethod
    def from_dict(cls, data: dict) -> "OffsetKey":
        """Create from dictionary."""
        return cls(**data)


class OffsetTopic:
    """
    Manages the internal __consumer_offsets topic.
    
    This is a compacted topic that stores committed offsets for all
    consumer groups. Each message has:
    - Key: (group_id, topic, partition)
    - Value: (offset, metadata, timestamp)
    
    The topic is compacted so only the latest offset per key is kept.
    """
    
    def __init__(
        self,
        data_dir: Path,
        num_partitions: int = OFFSET_TOPIC_NUM_PARTITIONS,
    ):
        """
        Initialize offset topic.
        
        Args:
            data_dir: Data directory for logs
            num_partitions: Number of partitions for offset topic
        """
        self._data_dir = data_dir
        self._num_partitions = num_partitions
        self._partition_logs: Dict[int, Log] = {}
        
        self._topic_dir = data_dir / INTERNAL_OFFSET_TOPIC
        self._topic_dir.mkdir(parents=True, exist_ok=True)
        
        self._initialize_partitions()
        
        logger.info(
            "OffsetTopic initialized",
            topic=INTERNAL_OFFSET_TOPIC,
            partitions=num_partitions,
        )
    
    def _initialize_partitions(self) -> None:
        """Initialize partition logs."""
        for partition in range(self._num_partitions):
            partition_dir = self._topic_dir / f"partition-{partition}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            
            self._partition_logs[partition] = Log(
                directory=partition_dir,
                max_segment_size=1024 * 1024 * 100,  # 100MB
                enable_compaction=True,  # Critical: enable compaction
            )
    
    def _partition_for_key(self, key: OffsetKey) -> int:
        """
        Calculate partition for offset key.
        
        Uses hash of group_id to ensure all offsets for a group
        go to the same partition.
        
        Args:
            key: Offset key
        
        Returns:
            Partition number
        """
        group_hash = abs(hash(key.group_id))
        return group_hash % self._num_partitions
    
    def commit_offset(
        self,
        group_id: str,
        topic_partition: TopicPartition,
        offset: int,
        metadata: str = "",
    ) -> None:
        """
        Commit offset to internal topic.
        
        Args:
            group_id: Consumer group ID
            topic_partition: Topic-partition
            offset: Offset to commit
            metadata: Optional metadata
        """
        key = OffsetKey(
            group_id=group_id,
            topic=topic_partition.topic,
            partition=topic_partition.partition,
        )
        
        offset_metadata = OffsetMetadata(
            offset=offset,
            metadata=metadata,
        )
        
        partition = self._partition_for_key(key)
        log = self._partition_logs[partition]
        
        key_bytes = json.dumps(key.to_dict()).encode()
        value_bytes = json.dumps(offset_metadata.to_dict()).encode()
        
        log.append(key=key_bytes, value=value_bytes)
        
        logger.debug(
            "Committed offset to internal topic",
            group_id=group_id,
            topic=topic_partition.topic,
            partition=topic_partition.partition,
            offset=offset,
        )
    
    def fetch_offset(
        self,
        group_id: str,
        topic_partition: TopicPartition,
    ) -> Optional[OffsetMetadata]:
        """
        Fetch committed offset from internal topic.
        
        Args:
            group_id: Consumer group ID
            topic_partition: Topic-partition
        
        Returns:
            Offset metadata or None
        """
        key = OffsetKey(
            group_id=group_id,
            topic=topic_partition.topic,
            partition=topic_partition.partition,
        )
        
        partition = self._partition_for_key(key)
        log = self._partition_logs[partition]
        
        key_str = key.to_string()
        
        latest_offset = None
        for message in log.read(start_offset=0):
            try:
                msg_key = json.loads(message.key.decode())
                msg_key_obj = OffsetKey.from_dict(msg_key)
                
                if msg_key_obj.to_string() == key_str:
                    value = json.loads(message.value.decode())
                    latest_offset = OffsetMetadata.from_dict(value)
            except Exception as e:
                logger.warning(
                    "Failed to parse offset message",
                    error=str(e),
                )
                continue
        
        if latest_offset:
            current_time = int(time.time() * 1000)
            if latest_offset.is_expired(current_time):
                logger.debug(
                    "Offset expired",
                    group_id=group_id,
                    topic=topic_partition.topic,
                    partition=topic_partition.partition,
                )
                return None
        
        return latest_offset
    
    def fetch_all_offsets(
        self,
        group_id: str,
    ) -> Dict[TopicPartition, OffsetMetadata]:
        """
        Fetch all committed offsets for a consumer group.
        
        Args:
            group_id: Consumer group ID
        
        Returns:
            Map of topic-partition to offset metadata
        """
        offsets: Dict[TopicPartition, OffsetMetadata] = {}
        
        for partition in range(self._num_partitions):
            log = self._partition_logs[partition]
            
            for message in log.read(start_offset=0):
                try:
                    msg_key = json.loads(message.key.decode())
                    key = OffsetKey.from_dict(msg_key)
                    
                    if key.group_id != group_id:
                        continue
                    
                    value = json.loads(message.value.decode())
                    offset_metadata = OffsetMetadata.from_dict(value)
                    
                    current_time = int(time.time() * 1000)
                    if offset_metadata.is_expired(current_time):
                        continue
                    
                    tp = TopicPartition(key.topic, key.partition)
                    offsets[tp] = offset_metadata
                    
                except Exception as e:
                    logger.warning(
                        "Failed to parse offset message",
                        error=str(e),
                    )
                    continue
        
        logger.debug(
            "Fetched all offsets for group",
            group_id=group_id,
            count=len(offsets),
        )
        
        return offsets
    
    def delete_group_offsets(self, group_id: str) -> None:
        """
        Delete all offsets for a consumer group.
        
        Writes tombstone records (null value) to mark offsets as deleted.
        
        Args:
            group_id: Consumer group ID
        """
        offsets = self.fetch_all_offsets(group_id)
        
        for tp in offsets.keys():
            key = OffsetKey(
                group_id=group_id,
                topic=tp.topic,
                partition=tp.partition,
            )
            
            partition = self._partition_for_key(key)
            log = self._partition_logs[partition]
            
            key_bytes = json.dumps(key.to_dict()).encode()
            log.append(key=key_bytes, value=b"")  # Tombstone
        
        logger.info(
            "Deleted group offsets",
            group_id=group_id,
            count=len(offsets),
        )
    
    def get_active_groups(self) -> Set[str]:
        """
        Get all active consumer groups.
        
        Returns:
            Set of group IDs with committed offsets
        """
        groups = set()
        
        for partition in range(self._num_partitions):
            log = self._partition_logs[partition]
            
            for message in log.read(start_offset=0):
                try:
                    msg_key = json.loads(message.key.decode())
                    key = OffsetKey.from_dict(msg_key)
                    
                    if message.value:  # Not a tombstone
                        value = json.loads(message.value.decode())
                        offset_metadata = OffsetMetadata.from_dict(value)
                        
                        current_time = int(time.time() * 1000)
                        if not offset_metadata.is_expired(current_time):
                            groups.add(key.group_id)
                except Exception:
                    continue
        
        return groups
    
    def compact_partition(self, partition: int) -> None:
        """
        Compact a partition of the offset topic.
        
        Args:
            partition: Partition number
        """
        if partition not in self._partition_logs:
            return
        
        log = self._partition_logs[partition]
        
        segments = log._segments
        for segment in segments[:-1]:  # Don't compact active segment
            log._compaction_manager.compact_segment(segment)
        
        logger.info(
            "Compacted offset topic partition",
            partition=partition,
        )
    
    def compact_all_partitions(self) -> None:
        """Compact all partitions of the offset topic."""
        for partition in range(self._num_partitions):
            self.compact_partition(partition)
        
        logger.info("Compacted all offset topic partitions")
    
    def close(self) -> None:
        """Close all partition logs."""
        for log in self._partition_logs.values():
            log.close()
        
        logger.info("OffsetTopic closed")
