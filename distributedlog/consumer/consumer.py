"""
Consumer client for reading messages from DistributedLog.

Provides high-level API for message consumption with:
- Topic subscription
- Polling with timeout
- Offset management (auto and manual commit)
- Seek operations
"""

import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Union

from distributedlog.consumer.fetcher import ConsumerRecord, MessageFetcher
from distributedlog.consumer.offset import (
    AutoCommitManager,
    OffsetAndMetadata,
    OffsetManager,
    OffsetResetStrategy,
    TopicPartition,
    get_beginning_offset,
    get_end_offset,
)
from distributedlog.producer.metadata import BootstrapServerParser, MetadataCache
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ConsumerConfig:
    """
    Configuration for consumer.
    
    Attributes:
        bootstrap_servers: Initial broker list
        group_id: Consumer group ID
        auto_commit: Enable auto-commit
        auto_commit_interval_ms: Auto-commit interval
        fetch_min_bytes: Minimum bytes to fetch
        fetch_max_wait_ms: Max wait for min bytes
        max_poll_records: Max records per poll
        auto_offset_reset: Offset reset strategy
        enable_auto_commit: Deprecated, use auto_commit
    """
    bootstrap_servers: Union[str, List[str]] = "localhost:9092"
    group_id: str = "default-group"
    auto_commit: bool = True
    auto_commit_interval_ms: int = 5000
    fetch_min_bytes: int = 1
    fetch_max_wait_ms: int = 500
    max_poll_records: int = 500
    auto_offset_reset: str = OffsetResetStrategy.LATEST


class Consumer:
    """
    High-level consumer client.
    
    Example:
        consumer = Consumer(
            bootstrap_servers=['localhost:9092'],
            group_id='my-group',
            auto_commit=True,
        )
        
        consumer.subscribe(['my-topic'])
        
        while True:
            messages = consumer.poll(timeout_ms=1000)
            for message in messages:
                print(f"Offset: {message.offset}, Value: {message.value}")
            
            # Manual commit (if auto_commit=False)
            # consumer.commit()
        
        consumer.close()
    """
    
    def __init__(
        self,
        bootstrap_servers: Union[str, List[str], None] = None,
        group_id: Optional[str] = None,
        auto_commit: bool = True,
        config: Optional[ConsumerConfig] = None,
        **kwargs,
    ):
        """
        Initialize consumer.
        
        Args:
            bootstrap_servers: Broker list (overrides config)
            group_id: Consumer group ID (overrides config)
            auto_commit: Enable auto-commit (overrides config)
            config: Consumer configuration
            **kwargs: Additional config overrides
        """
        self.config = config or ConsumerConfig()
        
        if bootstrap_servers is not None:
            self.config.bootstrap_servers = bootstrap_servers
        
        if group_id is not None:
            self.config.group_id = group_id
        
        self.config.auto_commit = auto_commit
        
        for key, value in kwargs.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)
        
        self._closed = False
        self._subscriptions: Set[str] = set()
        self._assignment: Set[TopicPartition] = set()
        
        self._offset_manager = OffsetManager(self.config.group_id)
        
        self._auto_commit_manager: Optional[AutoCommitManager] = None
        if self.config.auto_commit:
            self._auto_commit_manager = AutoCommitManager(
                self._offset_manager,
                self.config.auto_commit_interval_ms,
            )
            self._auto_commit_manager.start()
        
        self._fetcher = MessageFetcher(
            fetch_min_bytes=self.config.fetch_min_bytes,
            fetch_max_wait_ms=self.config.fetch_max_wait_ms,
            max_poll_records=self.config.max_poll_records,
        )
        
        self._metadata = MetadataCache()
        
        brokers = BootstrapServerParser.parse(self.config.bootstrap_servers)
        self._metadata.update_brokers(brokers)
        
        logger.info(
            "Consumer initialized",
            group_id=self.config.group_id,
            bootstrap_servers=self.config.bootstrap_servers,
            auto_commit=self.config.auto_commit,
        )
    
    def subscribe(self, topics: List[str]) -> None:
        """
        Subscribe to topics.
        
        Args:
            topics: List of topic names
        """
        self._subscriptions = set(topics)
        
        self._update_assignment()
        
        logger.info(
            "Subscribed to topics",
            topics=topics,
            group_id=self.config.group_id,
        )
    
    def _update_assignment(self) -> None:
        """Update partition assignment based on subscriptions."""
        new_assignment: Set[TopicPartition] = set()
        
        for topic in self._subscriptions:
            partition_count = self._metadata.get_partition_count(topic)
            
            if partition_count == 0:
                partition_count = 1
                logger.warning(
                    "No metadata for topic, using single partition",
                    topic=topic,
                )
            
            for partition in range(partition_count):
                new_assignment.add(TopicPartition(topic, partition))
        
        removed = self._assignment - new_assignment
        added = new_assignment - self._assignment
        
        if removed:
            self._fetcher.unassign_partitions(removed)
            self._offset_manager.reset_positions(removed)
        
        if added:
            offsets = {}
            for tp in added:
                committed = self._offset_manager.committed(tp)
                if committed:
                    offsets[tp] = committed.offset
                else:
                    if self.config.auto_offset_reset == OffsetResetStrategy.EARLIEST:
                        offsets[tp] = get_beginning_offset(tp)
                    elif self.config.auto_offset_reset == OffsetResetStrategy.LATEST:
                        offsets[tp] = get_end_offset(tp)
                    else:
                        offsets[tp] = 0
                
                self._offset_manager.update_position(tp, offsets[tp])
            
            self._fetcher.assign_partitions(added, offsets)
        
        self._assignment = new_assignment
        
        logger.info(
            "Updated assignment",
            added=len(added),
            removed=len(removed),
            total=len(self._assignment),
        )
    
    def poll(self, timeout_ms: int = 1000) -> List[ConsumerRecord]:
        """
        Poll for messages.
        
        Args:
            timeout_ms: Timeout in milliseconds
        
        Returns:
            List of consumer records
        """
        if self._closed:
            raise RuntimeError("Consumer is closed")
        
        start_time = time.time()
        timeout_sec = timeout_ms / 1000.0
        
        while True:
            if self._fetcher.has_buffered_data():
                records = self._fetcher.poll_buffer(self.config.max_poll_records)
                
                for record in records:
                    tp = TopicPartition(record.topic, record.partition)
                    self._offset_manager.update_position(tp, record.offset + 1)
                
                if records:
                    logger.debug(
                        "Polled records",
                        count=len(records),
                        group_id=self.config.group_id,
                    )
                
                return records
            
            assignments = {
                tp: self._offset_manager.position(tp) or 0
                for tp in self._assignment
            }
            
            self._fetcher.fetch_into_buffer(assignments)
            
            elapsed = time.time() - start_time
            if elapsed >= timeout_sec:
                return []
            
            time.sleep(0.01)
    
    def commit(
        self,
        offsets: Optional[Dict[TopicPartition, OffsetAndMetadata]] = None,
    ) -> None:
        """
        Commit offsets.
        
        Args:
            offsets: Offsets to commit (None = current positions)
        """
        if offsets is None:
            offsets = self._offset_manager.get_offsets_to_commit()
        
        self._offset_manager.commit(offsets)
        
        logger.info(
            "Committed offsets",
            count=len(offsets),
            group_id=self.config.group_id,
        )
    
    def seek(self, tp: TopicPartition, offset: int) -> None:
        """
        Seek to specific offset.
        
        Args:
            tp: Topic-partition
            offset: Target offset
        """
        self._offset_manager.seek(tp, offset)
        
        logger.info(
            "Seeked to offset",
            topic=tp.topic,
            partition=tp.partition,
            offset=offset,
        )
    
    def seek_to_beginning(self, partitions: Optional[List[TopicPartition]] = None) -> None:
        """
        Seek to beginning of partitions.
        
        Args:
            partitions: Partitions to seek (None = all assigned)
        """
        if partitions is None:
            partitions = list(self._assignment)
        
        for tp in partitions:
            offset = get_beginning_offset(tp)
            self._offset_manager.seek(tp, offset)
        
        logger.info(
            "Seeked to beginning",
            partitions=len(partitions),
        )
    
    def seek_to_end(self, partitions: Optional[List[TopicPartition]] = None) -> None:
        """
        Seek to end of partitions.
        
        Args:
            partitions: Partitions to seek (None = all assigned)
        """
        if partitions is None:
            partitions = list(self._assignment)
        
        for tp in partitions:
            offset = get_end_offset(tp)
            if offset >= 0:
                self._offset_manager.seek(tp, offset)
        
        logger.info(
            "Seeked to end",
            partitions=len(partitions),
        )
    
    def position(self, tp: TopicPartition) -> Optional[int]:
        """
        Get current position for partition.
        
        Args:
            tp: Topic-partition
        
        Returns:
            Current offset or None
        """
        return self._offset_manager.position(tp)
    
    def committed(self, tp: TopicPartition) -> Optional[OffsetAndMetadata]:
        """
        Get committed offset for partition.
        
        Args:
            tp: Topic-partition
        
        Returns:
            Committed offset or None
        """
        return self._offset_manager.committed(tp)
    
    def assignment(self) -> Set[TopicPartition]:
        """
        Get current partition assignment.
        
        Returns:
            Set of assigned partitions
        """
        return self._assignment.copy()
    
    def subscription(self) -> Set[str]:
        """
        Get current topic subscriptions.
        
        Returns:
            Set of subscribed topics
        """
        return self._subscriptions.copy()
    
    def close(self) -> None:
        """Close consumer and release resources."""
        if self._closed:
            return
        
        logger.info("Closing consumer", group_id=self.config.group_id)
        
        self._closed = True
        
        if self._auto_commit_manager:
            self._auto_commit_manager.stop()
        
        if self.config.auto_commit:
            self.commit()
        
        self._fetcher.close()
        
        logger.info("Consumer closed", group_id=self.config.group_id)
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    def metrics(self) -> dict:
        """
        Get consumer metrics.
        
        Returns:
            Dictionary with metrics
        """
        return {
            "group_id": self.config.group_id,
            "subscriptions": list(self._subscriptions),
            "assignment_count": len(self._assignment),
            "closed": self._closed,
        }
