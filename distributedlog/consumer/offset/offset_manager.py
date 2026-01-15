"""
Enhanced offset manager with persistent storage.

Extends the in-memory offset manager with persistent storage
in the internal __consumer_offsets topic.
"""

import asyncio
from typing import Dict, Optional

from distributedlog.consumer.offset import (
    OffsetAndMetadata,
    OffsetManager as BaseOffsetManager,
    TopicPartition,
)
from distributedlog.consumer.offset.offset_topic import (
    OffsetMetadata,
    OffsetTopic,
)
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class PersistentOffsetManager(BaseOffsetManager):
    """
    Offset manager with persistent storage.
    
    Extends base offset manager to persist commits to the internal
    __consumer_offsets topic. Provides:
    - Durable offset storage
    - Offset recovery on restart
    - Offset expiration
    - Group offset deletion
    """
    
    def __init__(
        self,
        group_id: str,
        offset_topic: OffsetTopic,
    ):
        """
        Initialize persistent offset manager.
        
        Args:
            group_id: Consumer group ID
            offset_topic: Internal offset topic
        """
        super().__init__(group_id)
        self._offset_topic = offset_topic
        self._lock = asyncio.Lock()
        
        self._load_committed_offsets()
        
        logger.info(
            "PersistentOffsetManager initialized",
            group_id=group_id,
        )
    
    def _load_committed_offsets(self) -> None:
        """Load committed offsets from internal topic on startup."""
        offsets = self._offset_topic.fetch_all_offsets(self.group_id)
        
        for tp, offset_metadata in offsets.items():
            self._committed[tp] = OffsetAndMetadata(
                offset=offset_metadata.offset,
                metadata=offset_metadata.metadata,
            )
        
        logger.info(
            "Loaded committed offsets",
            group_id=self.group_id,
            count=len(offsets),
        )
    
    async def commit_async(
        self,
        offsets: Dict[TopicPartition, OffsetAndMetadata],
    ) -> None:
        """
        Commit offsets asynchronously with persistence.
        
        Args:
            offsets: Offsets to commit
        """
        async with self._lock:
            for tp, offset_metadata in offsets.items():
                self._offset_topic.commit_offset(
                    group_id=self.group_id,
                    topic_partition=tp,
                    offset=offset_metadata.offset,
                    metadata=offset_metadata.metadata,
                )
                
                self._committed[tp] = offset_metadata
            
            logger.info(
                "Committed offsets (async)",
                group_id=self.group_id,
                count=len(offsets),
            )
    
    def commit(
        self,
        offsets: Dict[TopicPartition, OffsetAndMetadata],
    ) -> None:
        """
        Commit offsets synchronously with persistence.
        
        Args:
            offsets: Offsets to commit
        """
        asyncio.run(self.commit_async(offsets))
    
    async def fetch_offset_async(
        self,
        topic_partition: TopicPartition,
    ) -> Optional[OffsetAndMetadata]:
        """
        Fetch committed offset asynchronously.
        
        Args:
            topic_partition: Topic-partition
        
        Returns:
            Committed offset or None
        """
        async with self._lock:
            if topic_partition in self._committed:
                return self._committed[topic_partition]
            
            offset_metadata = self._offset_topic.fetch_offset(
                group_id=self.group_id,
                topic_partition=topic_partition,
            )
            
            if offset_metadata:
                offset_and_metadata = OffsetAndMetadata(
                    offset=offset_metadata.offset,
                    metadata=offset_metadata.metadata,
                )
                self._committed[topic_partition] = offset_and_metadata
                return offset_and_metadata
            
            return None
    
    def fetch_offset(
        self,
        topic_partition: TopicPartition,
    ) -> Optional[OffsetAndMetadata]:
        """
        Fetch committed offset synchronously.
        
        Args:
            topic_partition: Topic-partition
        
        Returns:
            Committed offset or None
        """
        return asyncio.run(self.fetch_offset_async(topic_partition))
    
    def delete_group_offsets(self) -> None:
        """Delete all offsets for this group."""
        self._offset_topic.delete_group_offsets(self.group_id)
        self._committed.clear()
        
        logger.info(
            "Deleted group offsets",
            group_id=self.group_id,
        )
