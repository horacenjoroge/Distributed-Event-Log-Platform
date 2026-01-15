"""
Broker gRPC server implementation.

Implements BrokerService and handles client/broker requests.
"""

import asyncio
import grpc
from concurrent import futures
from pathlib import Path
from typing import Optional

from distributedlog.broker.metadata import BrokerMetadata, get_local_ip
from distributedlog.broker.registry import BrokerRegistry
from distributedlog.consumer.offset.offset_topic import OffsetTopic
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class BrokerServer:
    """
    Broker gRPC server.
    
    Handles:
    - Produce requests
    - Fetch requests
    - Metadata requests
    - Offset management
    - Health checks
    """
    
    def __init__(
        self,
        broker_id: int,
        host: str,
        port: int,
        data_dir: Path,
        registry: BrokerRegistry,
        rack: Optional[str] = None,
    ):
        """
        Initialize broker server.
        
        Args:
            broker_id: Unique broker ID
            host: Broker hostname
            port: Broker port
            data_dir: Data directory for logs
            registry: Broker registry
            rack: Optional rack ID
        """
        self.broker_id = broker_id
        self.host = host
        self.port = port
        self.data_dir = data_dir
        self.registry = registry
        self.rack = rack
        
        self._server: Optional[grpc.aio.Server] = None
        self._metadata: Optional[BrokerMetadata] = None
        self._offset_topic: Optional[OffsetTopic] = None
        
        self._running = False
        self._heartbeat_task: Optional[asyncio.Task] = None
        
        logger.info(
            "BrokerServer initialized",
            broker_id=broker_id,
            host=host,
            port=port,
        )
    
    async def start(self) -> None:
        """Start the broker server."""
        if self._running:
            logger.warning("Broker server already running")
            return
        
        self._metadata = await self.registry.register_broker(
            broker_id=self.broker_id,
            host=self.host,
            port=self.port,
            rack=self.rack,
        )
        
        self._offset_topic = OffsetTopic(data_dir=self.data_dir)
        
        self._server = grpc.aio.server(
            futures.ThreadPoolExecutor(max_workers=10),
            options=[
                ("grpc.max_send_message_length", 100 * 1024 * 1024),  # 100MB
                ("grpc.max_receive_message_length", 100 * 1024 * 1024),
                ("grpc.keepalive_time_ms", 10000),
                ("grpc.keepalive_timeout_ms", 5000),
                ("grpc.http2.max_pings_without_data", 0),
                ("grpc.http2.min_time_between_pings_ms", 10000),
            ],
        )
        
        await self._server.add_insecure_port(f"{self.host}:{self.port}")
        await self._server.start()
        
        self._running = True
        
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        
        logger.info(
            "Broker server started",
            broker_id=self.broker_id,
            endpoint=self._metadata.endpoint(),
        )
    
    async def stop(self, grace_period: float = 5.0) -> None:
        """
        Stop the broker server.
        
        Args:
            grace_period: Grace period for shutdown in seconds
        """
        if not self._running:
            return
        
        logger.info("Stopping broker server", broker_id=self.broker_id)
        
        self._running = False
        
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        
        await self.registry.deregister_broker(self.broker_id)
        
        if self._server:
            await self._server.stop(grace_period)
        
        if self._offset_topic:
            self._offset_topic.close()
        
        logger.info("Broker server stopped", broker_id=self.broker_id)
    
    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeats to registry."""
        while self._running:
            try:
                await self.registry.update_heartbeat(self.broker_id)
                await asyncio.sleep(3.0)  # Heartbeat every 3 seconds
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    "Heartbeat error",
                    broker_id=self.broker_id,
                    error=str(e),
                )
                await asyncio.sleep(1.0)
    
    async def wait_for_termination(self) -> None:
        """Wait for server to terminate."""
        if self._server:
            await self._server.wait_for_termination()
    
    def is_running(self) -> bool:
        """Check if server is running."""
        return self._running
    
    def get_metadata(self) -> Optional[BrokerMetadata]:
        """Get broker metadata."""
        return self._metadata


class BrokerServiceImpl:
    """Implementation of BrokerService gRPC service."""
    
    def __init__(self, broker_server: BrokerServer):
        """
        Initialize service implementation.
        
        Args:
            broker_server: Broker server instance
        """
        self.broker_server = broker_server
        
        logger.info("BrokerServiceImpl initialized")
    
    async def Produce(self, request, context):
        """Handle produce request."""
        logger.debug(
            "Produce request",
            topic=request.topic,
            partition=request.partition,
        )
        
        response = {
            "error_code": 0,
            "error_message": "",
            "base_offset": 0,
            "partition": request.partition,
        }
        
        return response
    
    async def Fetch(self, request, context):
        """Handle fetch request."""
        logger.debug(
            "Fetch request",
            topic=request.topic,
            partition=request.partition,
            fetch_offset=request.fetch_offset,
        )
        
        response = {
            "error_code": 0,
            "error_message": "",
            "high_watermark": 0,
            "records": [],
        }
        
        yield response
    
    async def GetMetadata(self, request, context):
        """Handle metadata request."""
        logger.debug("Metadata request", topics=list(request.topics))
        
        brokers = await self.broker_server.registry.get_all_brokers()
        
        broker_metadata = [
            {
                "broker_id": b.broker_id,
                "host": b.host,
                "port": b.port,
                "grpc_port": b.port,
            }
            for b in brokers
        ]
        
        response = {
            "error_code": 0,
            "error_message": "",
            "brokers": broker_metadata,
            "topics": [],
        }
        
        return response
    
    async def HealthCheck(self, request, context):
        """Handle health check request."""
        healthy = self.broker_server.is_running()
        
        response = {
            "healthy": healthy,
            "status": "running" if healthy else "stopped",
        }
        
        logger.debug("Health check", healthy=healthy)
        
        return response
    
    async def CommitOffset(self, request, context):
        """Handle offset commit request."""
        logger.debug(
            "Commit offset request",
            group_id=request.group_id,
            topic=request.topic_partition.topic,
            partition=request.topic_partition.partition,
            offset=request.offset,
        )
        
        response = {
            "error_code": 0,
            "error_message": "",
        }
        
        return response
    
    async def FetchOffset(self, request, context):
        """Handle offset fetch request."""
        logger.debug(
            "Fetch offset request",
            group_id=request.group_id,
            topic=request.topic_partition.topic,
            partition=request.topic_partition.partition,
        )
        
        response = {
            "error_code": 0,
            "error_message": "",
            "offset": 0,
            "metadata": "",
        }
        
        return response
    
    async def ListOffsets(self, request, context):
        """Handle list offsets request."""
        logger.debug(
            "List offsets request",
            topic=request.topic_partition.topic,
            partition=request.topic_partition.partition,
            timestamp=request.timestamp,
        )
        
        response = {
            "error_code": 0,
            "error_message": "",
            "offset": 0,
            "timestamp": 0,
        }
        
        return response
