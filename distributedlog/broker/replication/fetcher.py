"""
Follower fetch protocol for replication.

Implements pull-based replication from leader to follower.
"""

import asyncio
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Set

from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class FetchRequest:
    """
    Request to fetch data from leader.
    
    Attributes:
        replica_id: Follower replica ID
        topic: Topic name
        partition: Partition number
        fetch_offset: Offset to start fetching from
        max_bytes: Maximum bytes to fetch
        min_bytes: Minimum bytes to wait for
        max_wait_ms: Maximum time to wait for min_bytes
    """
    replica_id: int
    topic: str
    partition: int
    fetch_offset: int
    max_bytes: int = 1048576  # 1 MB
    min_bytes: int = 1
    max_wait_ms: int = 500


@dataclass
class FetchResponse:
    """
    Response from leader with fetched data.
    
    Attributes:
        topic: Topic name
        partition: Partition number
        high_watermark: Leader's high-water mark
        log_end_offset: Leader's log end offset
        messages: Fetched messages
        error_code: Error code (0 = success)
    """
    topic: str
    partition: int
    high_watermark: int
    log_end_offset: int
    messages: List[bytes]
    error_code: int = 0


class ReplicationFetcher:
    """
    Manages fetching data from leader for replication.
    
    Used by follower replicas to pull data from partition leaders.
    """
    
    def __init__(
        self,
        broker_id: int,
        fetch_interval_ms: int = 500,
        max_fetch_bytes: int = 1048576,
    ):
        """
        Initialize replication fetcher.
        
        Args:
            broker_id: This broker's ID
            fetch_interval_ms: Interval between fetches
            max_fetch_bytes: Maximum bytes per fetch
        """
        self._broker_id = broker_id
        self._fetch_interval_ms = fetch_interval_ms
        self._max_fetch_bytes = max_fetch_bytes
        
        # Track partition offsets we're fetching
        self._partition_offsets: Dict[tuple, int] = {}
        
        # Track fetch tasks
        self._fetch_tasks: Set[asyncio.Task] = set()
        
        self._running = False
        self._lock = asyncio.Lock()
        
        logger.info(
            "ReplicationFetcher initialized",
            broker_id=broker_id,
            fetch_interval_ms=fetch_interval_ms,
        )
    
    async def start_fetching(
        self,
        topic: str,
        partition: int,
        leader_broker_id: int,
        start_offset: int,
    ) -> None:
        """
        Start fetching data for a partition.
        
        Args:
            topic: Topic name
            partition: Partition number
            leader_broker_id: Leader broker ID
            start_offset: Offset to start from
        """
        async with self._lock:
            key = (topic, partition)
            
            if key in self._partition_offsets:
                logger.warning(
                    "Already fetching partition",
                    topic=topic,
                    partition=partition,
                )
                return
            
            self._partition_offsets[key] = start_offset
            
            # Create fetch task
            task = asyncio.create_task(
                self._fetch_loop(
                    topic,
                    partition,
                    leader_broker_id,
                )
            )
            self._fetch_tasks.add(task)
            task.add_done_callback(self._fetch_tasks.discard)
            
            logger.info(
                "Started fetching partition",
                topic=topic,
                partition=partition,
                leader_id=leader_broker_id,
                start_offset=start_offset,
            )
    
    async def stop_fetching(
        self,
        topic: str,
        partition: int,
    ) -> None:
        """
        Stop fetching data for a partition.
        
        Args:
            topic: Topic name
            partition: Partition number
        """
        async with self._lock:
            key = (topic, partition)
            
            if key in self._partition_offsets:
                del self._partition_offsets[key]
                
                logger.info(
                    "Stopped fetching partition",
                    topic=topic,
                    partition=partition,
                )
    
    async def _fetch_loop(
        self,
        topic: str,
        partition: int,
        leader_broker_id: int,
    ) -> None:
        """
        Continuous fetch loop for a partition.
        
        Args:
            topic: Topic name
            partition: Partition number
            leader_broker_id: Leader broker ID
        """
        key = (topic, partition)
        
        while key in self._partition_offsets:
            try:
                fetch_offset = self._partition_offsets[key]
                
                # Create fetch request
                request = FetchRequest(
                    replica_id=self._broker_id,
                    topic=topic,
                    partition=partition,
                    fetch_offset=fetch_offset,
                    max_bytes=self._max_fetch_bytes,
                )
                
                # Fetch from leader
                response = await self._fetch_from_leader(
                    leader_broker_id,
                    request,
                )
                
                if response and response.error_code == 0:
                    # Process fetched messages
                    await self._process_fetch_response(response)
                    
                    # Update fetch offset
                    if response.messages:
                        # Move to next offset
                        self._partition_offsets[key] = (
                            fetch_offset + len(response.messages)
                        )
                
                # Wait before next fetch
                await asyncio.sleep(self._fetch_interval_ms / 1000)
                
            except asyncio.CancelledError:
                logger.info(
                    "Fetch loop cancelled",
                    topic=topic,
                    partition=partition,
                )
                break
            except Exception as e:
                logger.error(
                    "Error in fetch loop",
                    topic=topic,
                    partition=partition,
                    error=str(e),
                )
                
                # Back off on error
                await asyncio.sleep(1.0)
    
    async def _fetch_from_leader(
        self,
        leader_broker_id: int,
        request: FetchRequest,
    ) -> Optional[FetchResponse]:
        """
        Fetch data from leader broker.
        
        Args:
            leader_broker_id: Leader broker ID
            request: Fetch request
        
        Returns:
            Fetch response or None
        """
        # TODO: Implement actual gRPC call to leader
        # For now, return placeholder
        
        logger.debug(
            "Fetching from leader",
            leader_id=leader_broker_id,
            topic=request.topic,
            partition=request.partition,
            offset=request.fetch_offset,
        )
        
        # Placeholder response
        return FetchResponse(
            topic=request.topic,
            partition=request.partition,
            high_watermark=request.fetch_offset,
            log_end_offset=request.fetch_offset,
            messages=[],
        )
    
    async def _process_fetch_response(
        self,
        response: FetchResponse,
    ) -> None:
        """
        Process fetched messages.
        
        Args:
            response: Fetch response
        """
        if not response.messages:
            return
        
        logger.debug(
            "Received messages from leader",
            topic=response.topic,
            partition=response.partition,
            message_count=len(response.messages),
            hwm=response.high_watermark,
        )
        
        # TODO: Write messages to local log
        # TODO: Update local high-water mark
    
    async def shutdown(self) -> None:
        """Shutdown fetcher and cancel all tasks."""
        self._running = False
        
        # Cancel all fetch tasks
        for task in self._fetch_tasks:
            task.cancel()
        
        if self._fetch_tasks:
            await asyncio.gather(*self._fetch_tasks, return_exceptions=True)
        
        logger.info("ReplicationFetcher shut down")


class LeaderReplicationManager:
    """
    Manages replication on the leader side.
    
    Tracks follower fetch positions and serves fetch requests.
    """
    
    def __init__(self):
        """Initialize leader replication manager."""
        # Track follower positions: (topic, partition, replica_id) -> offset
        self._follower_positions: Dict[tuple, int] = {}
        self._lock = asyncio.Lock()
        
        logger.info("LeaderReplicationManager initialized")
    
    async def handle_fetch_request(
        self,
        request: FetchRequest,
        log_reader,
    ) -> FetchResponse:
        """
        Handle fetch request from follower.
        
        Args:
            request: Fetch request
            log_reader: Log reader for partition
        
        Returns:
            Fetch response
        """
        async with self._lock:
            key = (request.topic, request.partition, request.replica_id)
            
            # Update follower position
            self._follower_positions[key] = request.fetch_offset
            
            logger.debug(
                "Handling fetch request",
                replica_id=request.replica_id,
                topic=request.topic,
                partition=request.partition,
                offset=request.fetch_offset,
            )
        
        # Read messages from log
        messages = []
        bytes_read = 0
        current_offset = request.fetch_offset
        
        # TODO: Read from actual log
        # For now, return empty response
        
        response = FetchResponse(
            topic=request.topic,
            partition=request.partition,
            high_watermark=current_offset,
            log_end_offset=current_offset,
            messages=messages,
        )
        
        return response
    
    async def get_follower_lag(
        self,
        topic: str,
        partition: int,
        replica_id: int,
        leader_offset: int,
    ) -> Optional[int]:
        """
        Get lag for a follower replica.
        
        Args:
            topic: Topic name
            partition: Partition number
            replica_id: Follower replica ID
            leader_offset: Leader's log end offset
        
        Returns:
            Lag in messages or None
        """
        async with self._lock:
            key = (topic, partition, replica_id)
            
            if key not in self._follower_positions:
                return None
            
            follower_offset = self._follower_positions[key]
            
            return leader_offset - follower_offset
    
    async def get_all_follower_positions(
        self,
        topic: str,
        partition: int,
    ) -> Dict[int, int]:
        """
        Get positions for all followers of a partition.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            Map of replica_id -> offset
        """
        async with self._lock:
            positions = {}
            
            for (t, p, replica_id), offset in self._follower_positions.items():
                if t == topic and p == partition:
                    positions[replica_id] = offset
            
            return positions
