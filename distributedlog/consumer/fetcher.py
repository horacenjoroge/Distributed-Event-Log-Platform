"""
Message fetcher for consumer.

Fetches messages from log and buffers them for consumer poll().
"""

import threading
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Deque, Dict, List, Optional, Set

from distributedlog.consumer.offset import TopicPartition
from distributedlog.core.log.log import Log
from distributedlog.core.log.format import Message
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ConsumerRecord:
    """
    A record consumed from a topic-partition.
    
    Attributes:
        topic: Topic name
        partition: Partition number
        offset: Message offset
        timestamp: Message timestamp
        key: Message key
        value: Message value
    """
    topic: str
    partition: int
    offset: int
    timestamp: int
    key: Optional[bytes]
    value: bytes


class FetchBuffer:
    """
    Buffers fetched messages per topic-partition.
    
    Provides efficient access to messages during poll().
    """
    
    def __init__(self, max_size: int = 1000):
        """
        Initialize fetch buffer.
        
        Args:
            max_size: Maximum messages to buffer per partition
        """
        self.max_size = max_size
        
        self._buffers: Dict[TopicPartition, Deque[ConsumerRecord]] = {}
        self._lock = threading.RLock()
        
        logger.debug("Initialized fetch buffer", max_size=max_size)
    
    def add(self, tp: TopicPartition, records: List[ConsumerRecord]) -> None:
        """
        Add records to buffer.
        
        Args:
            tp: Topic-partition
            records: Records to add
        """
        with self._lock:
            if tp not in self._buffers:
                self._buffers[tp] = deque(maxlen=self.max_size)
            
            buffer = self._buffers[tp]
            
            for record in records:
                if len(buffer) < self.max_size:
                    buffer.append(record)
                else:
                    logger.warning(
                        "Buffer full, dropping record",
                        topic=tp.topic,
                        partition=tp.partition,
                    )
                    break
            
            logger.debug(
                "Added records to buffer",
                topic=tp.topic,
                partition=tp.partition,
                count=len(records),
                buffer_size=len(buffer),
            )
    
    def poll(self, max_records: int = 100) -> List[ConsumerRecord]:
        """
        Poll records from buffer.
        
        Args:
            max_records: Maximum records to return
        
        Returns:
            List of records
        """
        with self._lock:
            records = []
            
            for tp, buffer in list(self._buffers.items()):
                while buffer and len(records) < max_records:
                    records.append(buffer.popleft())
                
                if not buffer:
                    del self._buffers[tp]
            
            return records
    
    def has_data(self) -> bool:
        """Check if buffer has data."""
        with self._lock:
            return any(len(buffer) > 0 for buffer in self._buffers.values())
    
    def clear(self, tp: Optional[TopicPartition] = None) -> None:
        """
        Clear buffer.
        
        Args:
            tp: Topic-partition to clear (None = all)
        """
        with self._lock:
            if tp is None:
                self._buffers.clear()
            elif tp in self._buffers:
                self._buffers[tp].clear()
                del self._buffers[tp]


class MessageFetcher:
    """
    Fetches messages from log files.
    
    Reads messages from assigned partitions and buffers them.
    """
    
    def __init__(
        self,
        fetch_min_bytes: int = 1,
        fetch_max_wait_ms: int = 500,
        max_poll_records: int = 500,
    ):
        """
        Initialize message fetcher.
        
        Args:
            fetch_min_bytes: Minimum bytes to fetch
            fetch_max_wait_ms: Max time to wait for min bytes
            max_poll_records: Max records per poll
        """
        self.fetch_min_bytes = fetch_min_bytes
        self.fetch_max_wait_ms = fetch_max_wait_ms
        self.max_poll_records = max_poll_records
        
        self._buffer = FetchBuffer(max_size=max_poll_records * 2)
        self._logs: Dict[TopicPartition, Log] = {}
        
        self._fetching = False
        self._fetch_thread: Optional[threading.Thread] = None
        
        logger.info(
            "Initialized message fetcher",
            fetch_min_bytes=fetch_min_bytes,
            fetch_max_wait_ms=fetch_max_wait_ms,
            max_poll_records=max_poll_records,
        )
    
    def assign_partitions(
        self,
        partitions: Set[TopicPartition],
        offsets: Dict[TopicPartition, int],
    ) -> None:
        """
        Assign partitions to fetch from.
        
        Args:
            partitions: Partitions to assign
            offsets: Starting offsets per partition
        """
        for tp in partitions:
            if tp not in self._logs:
                log_dir = Path(f"/tmp/distributedlog/{tp.topic}/{tp.partition}")
                
                if log_dir.exists():
                    self._logs[tp] = Log(
                        directory=log_dir,
                        fsync_on_append=False,
                    )
                    
                    logger.info(
                        "Assigned partition",
                        topic=tp.topic,
                        partition=tp.partition,
                        starting_offset=offsets.get(tp, 0),
                    )
                else:
                    logger.warning(
                        "Partition log not found",
                        topic=tp.topic,
                        partition=tp.partition,
                    )
    
    def unassign_partitions(self, partitions: Set[TopicPartition]) -> None:
        """
        Unassign partitions.
        
        Args:
            partitions: Partitions to unassign
        """
        for tp in partitions:
            if tp in self._logs:
                self._logs[tp].close()
                del self._logs[tp]
                
                logger.info(
                    "Unassigned partition",
                    topic=tp.topic,
                    partition=tp.partition,
                )
        
        for tp in partitions:
            self._buffer.clear(tp)
    
    def fetch(
        self,
        tp: TopicPartition,
        offset: int,
        max_records: int,
    ) -> List[ConsumerRecord]:
        """
        Fetch records from partition.
        
        Args:
            tp: Topic-partition
            offset: Starting offset
            max_records: Maximum records to fetch
        
        Returns:
            List of consumer records
        """
        if tp not in self._logs:
            return []
        
        log = self._logs[tp]
        
        try:
            records = []
            
            for message in log.read(start_offset=offset, max_messages=max_records):
                record = ConsumerRecord(
                    topic=tp.topic,
                    partition=tp.partition,
                    offset=message.offset,
                    timestamp=message.timestamp,
                    key=message.key,
                    value=message.value,
                )
                records.append(record)
            
            if records:
                logger.debug(
                    "Fetched records",
                    topic=tp.topic,
                    partition=tp.partition,
                    offset=offset,
                    count=len(records),
                )
            
            return records
        
        except Exception as e:
            logger.error(
                "Fetch failed",
                topic=tp.topic,
                partition=tp.partition,
                offset=offset,
                error=str(e),
            )
            return []
    
    def fetch_into_buffer(
        self,
        assignments: Dict[TopicPartition, int],
    ) -> None:
        """
        Fetch messages into buffer.
        
        Args:
            assignments: Topic-partitions with current offsets
        """
        for tp, offset in assignments.items():
            records = self.fetch(tp, offset, self.max_poll_records)
            
            if records:
                self._buffer.add(tp, records)
    
    def poll_buffer(self, max_records: int) -> List[ConsumerRecord]:
        """
        Poll records from buffer.
        
        Args:
            max_records: Maximum records to return
        
        Returns:
            List of records
        """
        return self._buffer.poll(max_records)
    
    def has_buffered_data(self) -> bool:
        """Check if buffer has data."""
        return self._buffer.has_data()
    
    def close(self) -> None:
        """Close fetcher and release resources."""
        for log in self._logs.values():
            log.close()
        
        self._logs.clear()
        self._buffer.clear()
        
        logger.info("Closed message fetcher")
