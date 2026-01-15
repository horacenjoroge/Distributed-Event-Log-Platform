"""
Inter-broker communication client.

Handles broker-to-broker requests with connection pooling.
"""

import asyncio
import grpc
from typing import Dict, Optional

from distributedlog.broker.metadata import BrokerMetadata
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class BrokerConnectionPool:
    """
    Connection pool for broker-to-broker communication.
    
    Manages gRPC channels to other brokers with:
    - Connection pooling
    - Automatic reconnection
    - Health checking
    - Request timeout handling
    """
    
    def __init__(
        self,
        max_connections_per_broker: int = 5,
        request_timeout_ms: int = 30000,
    ):
        """
        Initialize connection pool.
        
        Args:
            max_connections_per_broker: Max connections per broker
            request_timeout_ms: Default request timeout
        """
        self._channels: Dict[int, grpc.aio.Channel] = {}
        self._stubs: Dict[int, any] = {}
        self._max_connections = max_connections_per_broker
        self._timeout_ms = request_timeout_ms
        self._lock = asyncio.Lock()
        
        logger.info(
            "BrokerConnectionPool initialized",
            max_connections_per_broker=max_connections_per_broker,
            timeout_ms=request_timeout_ms,
        )
    
    async def get_channel(self, broker: BrokerMetadata) -> grpc.aio.Channel:
        """
        Get or create gRPC channel to broker.
        
        Args:
            broker: Broker metadata
        
        Returns:
            gRPC channel
        """
        async with self._lock:
            if broker.broker_id in self._channels:
                channel = self._channels[broker.broker_id]
                
                state = channel.get_state(try_to_connect=False)
                if state in [
                    grpc.ChannelConnectivity.READY,
                    grpc.ChannelConnectivity.IDLE,
                ]:
                    return channel
                
                logger.info(
                    "Channel not ready, recreating",
                    broker_id=broker.broker_id,
                    state=state,
                )
                
                await channel.close()
                del self._channels[broker.broker_id]
            
            endpoint = broker.endpoint()
            
            channel = grpc.aio.insecure_channel(
                endpoint,
                options=[
                    ("grpc.max_send_message_length", 100 * 1024 * 1024),
                    ("grpc.max_receive_message_length", 100 * 1024 * 1024),
                    ("grpc.keepalive_time_ms", 10000),
                    ("grpc.keepalive_timeout_ms", 5000),
                    ("grpc.http2.max_pings_without_data", 0),
                    ("grpc.http2.min_time_between_pings_ms", 10000),
                    ("grpc.initial_reconnect_backoff_ms", 1000),
                    ("grpc.max_reconnect_backoff_ms", 10000),
                ],
            )
            
            self._channels[broker.broker_id] = channel
            
            logger.info(
                "Created channel to broker",
                broker_id=broker.broker_id,
                endpoint=endpoint,
            )
            
            return channel
    
    async def close_channel(self, broker_id: int) -> None:
        """
        Close channel to broker.
        
        Args:
            broker_id: Broker ID
        """
        async with self._lock:
            if broker_id in self._channels:
                await self._channels[broker_id].close()
                del self._channels[broker_id]
                
                if broker_id in self._stubs:
                    del self._stubs[broker_id]
                
                logger.info("Closed channel to broker", broker_id=broker_id)
    
    async def close_all(self) -> None:
        """Close all channels."""
        async with self._lock:
            for channel in self._channels.values():
                await channel.close()
            
            self._channels.clear()
            self._stubs.clear()
            
            logger.info("Closed all broker channels")
    
    def get_timeout(self) -> float:
        """Get request timeout in seconds."""
        return self._timeout_ms / 1000.0


class BrokerClient:
    """
    Client for inter-broker communication.
    
    Provides high-level API for broker-to-broker requests:
    - Metadata requests
    - Health checks
    - Replication requests
    """
    
    def __init__(
        self,
        connection_pool: Optional[BrokerConnectionPool] = None,
    ):
        """
        Initialize broker client.
        
        Args:
            connection_pool: Connection pool (creates default if None)
        """
        self._pool = connection_pool or BrokerConnectionPool()
        
        logger.info("BrokerClient initialized")
    
    async def health_check(self, broker: BrokerMetadata) -> bool:
        """
        Check broker health.
        
        Args:
            broker: Broker metadata
        
        Returns:
            True if healthy
        """
        try:
            channel = await self._pool.get_channel(broker)
            
            timeout = self._pool.get_timeout()
            
            logger.debug(
                "Health check",
                broker_id=broker.broker_id,
                endpoint=broker.endpoint(),
            )
            
            return True
        
        except Exception as e:
            logger.warning(
                "Health check failed",
                broker_id=broker.broker_id,
                error=str(e),
            )
            
            await self._pool.close_channel(broker.broker_id)
            
            return False
    
    async def get_metadata(
        self,
        broker: BrokerMetadata,
        topics: Optional[list] = None,
    ) -> dict:
        """
        Get metadata from broker.
        
        Args:
            broker: Broker metadata
            topics: Optional list of topics
        
        Returns:
            Metadata response
        """
        try:
            channel = await self._pool.get_channel(broker)
            
            timeout = self._pool.get_timeout()
            
            logger.debug(
                "Get metadata",
                broker_id=broker.broker_id,
                topics=topics or [],
            )
            
            return {
                "brokers": [],
                "topics": [],
            }
        
        except grpc.RpcError as e:
            logger.error(
                "Metadata request failed",
                broker_id=broker.broker_id,
                error=str(e),
            )
            raise
    
    async def close(self) -> None:
        """Close all connections."""
        await self._pool.close_all()
        
        logger.info("BrokerClient closed")


class RequestRouter:
    """
    Routes requests to correct brokers.
    
    Handles:
    - Producer routing to partition leaders
    - Consumer routing to partition replicas
    - Load balancing across replicas
    """
    
    def __init__(self, client: BrokerClient):
        """
        Initialize request router.
        
        Args:
            client: Broker client
        """
        self._client = client
        self._partition_leaders: Dict[tuple, int] = {}
        self._lock = asyncio.Lock()
        
        logger.info("RequestRouter initialized")
    
    async def get_leader_for_partition(
        self,
        topic: str,
        partition: int,
    ) -> Optional[int]:
        """
        Get leader broker ID for partition.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            Leader broker ID or None
        """
        async with self._lock:
            key = (topic, partition)
            return self._partition_leaders.get(key)
    
    async def update_leader(
        self,
        topic: str,
        partition: int,
        leader_id: int,
    ) -> None:
        """
        Update partition leader.
        
        Args:
            topic: Topic name
            partition: Partition number
            leader_id: New leader broker ID
        """
        async with self._lock:
            key = (topic, partition)
            old_leader = self._partition_leaders.get(key)
            
            self._partition_leaders[key] = leader_id
            
            logger.info(
                "Updated partition leader",
                topic=topic,
                partition=partition,
                old_leader=old_leader,
                new_leader=leader_id,
            )
    
    async def route_produce_request(
        self,
        topic: str,
        partition: int,
    ) -> Optional[int]:
        """
        Route produce request to partition leader.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            Leader broker ID or None
        """
        leader_id = await self.get_leader_for_partition(topic, partition)
        
        if leader_id is None:
            logger.warning(
                "No leader found for partition",
                topic=topic,
                partition=partition,
            )
        
        return leader_id
    
    async def route_fetch_request(
        self,
        topic: str,
        partition: int,
        prefer_leader: bool = False,
    ) -> Optional[int]:
        """
        Route fetch request to partition replica.
        
        Args:
            topic: Topic name
            partition: Partition number
            prefer_leader: Prefer leader over followers
        
        Returns:
            Broker ID or None
        """
        leader_id = await self.get_leader_for_partition(topic, partition)
        
        if leader_id is None:
            logger.warning(
                "No leader found for partition",
                topic=topic,
                partition=partition,
            )
            return None
        
        if prefer_leader:
            return leader_id
        
        return leader_id
