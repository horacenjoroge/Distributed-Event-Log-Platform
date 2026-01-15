"""
Group coordinator with persistent offset storage.

Extends base GroupCoordinator to use the internal __consumer_offsets topic
for durable offset storage.
"""

from pathlib import Path
from typing import Dict, Optional

from distributedlog.consumer.group.coordinator import GroupCoordinator
from distributedlog.consumer.offset import TopicPartition
from distributedlog.consumer.offset.offset_topic import OffsetTopic
from distributedlog.consumer.offset.rebalance_handler import RebalanceOffsetHandler
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class PersistentGroupCoordinator(GroupCoordinator):
    """
    Group coordinator with persistent offset storage.
    
    Extends GroupCoordinator to persist offsets in the internal
    __consumer_offsets topic instead of just in memory.
    
    Features:
    - Durable offset storage
    - Offset recovery on restart
    - Offset expiration
    - Offset reset strategies
    - Rebalance offset handling
    """
    
    def __init__(
        self,
        partitions_per_topic: Dict[str, int],
        data_dir: Path,
        offset_reset_strategy: str = "latest",
    ):
        """
        Initialize persistent group coordinator.
        
        Args:
            partitions_per_topic: Number of partitions per topic
            data_dir: Data directory for offset topic
            offset_reset_strategy: Strategy for offset reset
        """
        super().__init__(partitions_per_topic)
        
        self._offset_topic = OffsetTopic(data_dir)
        self._rebalance_handler = RebalanceOffsetHandler(
            offset_topic=self._offset_topic,
            reset_strategy=offset_reset_strategy,
        )
        
        self._load_active_groups()
        
        logger.info(
            "PersistentGroupCoordinator initialized",
            reset_strategy=offset_reset_strategy,
        )
    
    def _load_active_groups(self) -> None:
        """Load active consumer groups from offset topic on startup."""
        active_groups = self._offset_topic.get_active_groups()
        
        logger.info(
            "Loaded active groups",
            count=len(active_groups),
            groups=list(active_groups),
        )
    
    async def commit_offset(
        self,
        group_id: str,
        topic_partition: TopicPartition,
        offset: int,
        metadata: str = "",
    ) -> None:
        """
        Commit offset with persistence to internal topic.
        
        Args:
            group_id: Consumer group ID
            topic_partition: Topic-partition
            offset: Offset to commit
            metadata: Optional metadata
        """
        async with self._lock:
            self._offset_topic.commit_offset(
                group_id=group_id,
                topic_partition=topic_partition,
                offset=offset,
                metadata=metadata,
            )
            
            if group_id not in self._offsets:
                from distributedlog.consumer.group.metadata import GroupOffsets
                self._offsets[group_id] = GroupOffsets(group_id)
            
            self._offsets[group_id].commit(topic_partition, offset)
            
            logger.debug(
                "Committed offset (persistent)",
                group_id=group_id,
                topic=topic_partition.topic,
                partition=topic_partition.partition,
                offset=offset,
            )
    
    async def fetch_offset(
        self,
        group_id: str,
        topic_partition: TopicPartition,
    ) -> Optional[int]:
        """
        Fetch committed offset from persistent storage.
        
        Args:
            group_id: Consumer group ID
            topic_partition: Topic-partition
        
        Returns:
            Committed offset or None
        """
        async with self._lock:
            offset_metadata = self._offset_topic.fetch_offset(
                group_id=group_id,
                topic_partition=topic_partition,
            )
            
            if offset_metadata:
                return offset_metadata.offset
            
            return None
    
    async def get_lag(
        self,
        group_id: str,
        topic_partition: TopicPartition,
        log_end_offset: int,
    ) -> Optional[int]:
        """
        Calculate consumer lag from persistent storage.
        
        Args:
            group_id: Consumer group ID
            topic_partition: Topic-partition
            log_end_offset: End offset of log
        
        Returns:
            Lag or None
        """
        async with self._lock:
            offset_metadata = self._offset_topic.fetch_offset(
                group_id=group_id,
                topic_partition=topic_partition,
            )
            
            if offset_metadata:
                return log_end_offset - offset_metadata.offset
            
            return None
    
    async def delete_group(self, group_id: str) -> None:
        """
        Delete consumer group and all its offsets.
        
        Args:
            group_id: Consumer group ID
        """
        async with self._lock:
            self._offset_topic.delete_group_offsets(group_id)
            
            if group_id in self._groups:
                del self._groups[group_id]
            
            if group_id in self._offsets:
                del self._offsets[group_id]
            
            logger.info(
                "Deleted consumer group",
                group_id=group_id,
            )
    
    async def compact_offsets(self) -> None:
        """Compact the internal offset topic."""
        self._offset_topic.compact_all_partitions()
        
        logger.info("Compacted offset topic")
    
    def get_offset_topic_stats(self) -> dict:
        """
        Get statistics about the offset topic.
        
        Returns:
            Dictionary with stats
        """
        return {
            "topic": "__consumer_offsets",
            "partitions": self._offset_topic._num_partitions,
            "active_groups": len(self._offset_topic.get_active_groups()),
        }
    
    def close(self) -> None:
        """Close coordinator and offset topic."""
        self._offset_topic.close()
        
        logger.info("PersistentGroupCoordinator closed")
