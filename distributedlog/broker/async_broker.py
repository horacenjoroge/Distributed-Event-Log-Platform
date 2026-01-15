"""
Async I/O broker for non-blocking high-throughput operations.

Uses asyncio for event loop and non-blocking I/O.
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Optional, Set

from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class AsyncBrokerConfig:
    """
    Configuration for async broker.
    
    Attributes:
        max_connections: Maximum concurrent connections
        disk_io_threads: Thread pool size for disk I/O
        network_buffer_size: Network buffer size
        backpressure_threshold: Backpressure threshold (pending requests)
    """
    
    def __init__(
        self,
        max_connections: int = 10000,
        disk_io_threads: int = 8,
        network_buffer_size: int = 64 * 1024,
        backpressure_threshold: int = 1000,
    ):
        self.max_connections = max_connections
        self.disk_io_threads = disk_io_threads
        self.network_buffer_size = network_buffer_size
        self.backpressure_threshold = backpressure_threshold


class ConnectionState:
    """
    State for a single connection.
    
    Attributes:
        conn_id: Connection ID
        reader: asyncio StreamReader
        writer: asyncio StreamWriter
        pending_requests: Number of pending requests
        bytes_sent: Total bytes sent
        bytes_received: Total bytes received
    """
    
    def __init__(
        self,
        conn_id: str,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ):
        self.conn_id = conn_id
        self.reader = reader
        self.writer = writer
        self.pending_requests = 0
        self.bytes_sent = 0
        self.bytes_received = 0
        self.created_at = asyncio.get_event_loop().time()
    
    def is_backpressured(self, threshold: int) -> bool:
        """
        Check if connection is backpressured.
        
        Args:
            threshold: Backpressure threshold
        
        Returns:
            True if backpressured
        """
        return self.pending_requests >= threshold


class AsyncBroker:
    """
    Async I/O broker with non-blocking operations.
    
    Features:
    - Non-blocking network I/O (asyncio)
    - Async disk I/O (thread pool)
    - Connection multiplexing
    - Backpressure handling
    - Event loop per broker
    """
    
    def __init__(
        self,
        broker_id: str,
        host: str = "localhost",
        port: int = 9092,
        config: Optional[AsyncBrokerConfig] = None,
    ):
        """
        Initialize async broker.
        
        Args:
            broker_id: Broker identifier
            host: Host to bind to
            port: Port to bind to
            config: Broker configuration
        """
        self.broker_id = broker_id
        self.host = host
        self.port = port
        self.config = config or AsyncBrokerConfig()
        
        # Event loop
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.server: Optional[asyncio.Server] = None
        
        # Thread pool for disk I/O (disk is always blocking)
        self.disk_executor = ThreadPoolExecutor(
            max_workers=self.config.disk_io_threads,
            thread_name_prefix="disk-io",
        )
        
        # Active connections
        self.connections: Dict[str, ConnectionState] = {}
        self._next_conn_id = 0
        
        # Semaphore for max connections
        self.connection_semaphore: Optional[asyncio.Semaphore] = None
        
        # Statistics
        self._total_connections = 0
        self._active_connections = 0
        self._rejected_connections = 0
        self._backpressure_events = 0
        
        logger.info(
            "AsyncBroker initialized",
            broker_id=broker_id,
            host=host,
            port=port,
            max_connections=self.config.max_connections,
            disk_threads=self.config.disk_io_threads,
        )
    
    async def start(self) -> None:
        """Start async broker server."""
        self.loop = asyncio.get_running_loop()
        
        # Create connection semaphore
        self.connection_semaphore = asyncio.Semaphore(
            self.config.max_connections
        )
        
        # Start TCP server
        self.server = await asyncio.start_server(
            self._handle_connection,
            self.host,
            self.port,
        )
        
        addrs = ', '.join(str(sock.getsockname()) for sock in self.server.sockets)
        
        logger.info(
            "AsyncBroker started",
            broker_id=self.broker_id,
            addresses=addrs,
        )
    
    async def stop(self) -> None:
        """Stop async broker server."""
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        
        # Close all connections
        for conn in list(self.connections.values()):
            conn.writer.close()
            await conn.writer.wait_closed()
        
        self.connections.clear()
        
        # Shutdown disk executor
        self.disk_executor.shutdown(wait=True)
        
        logger.info("AsyncBroker stopped", broker_id=self.broker_id)
    
    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        """
        Handle incoming connection.
        
        Args:
            reader: Stream reader
            writer: Stream writer
        """
        # Acquire connection semaphore (or reject)
        if not await self._try_acquire_connection():
            logger.warning("Connection rejected (max connections reached)")
            writer.close()
            await writer.wait_closed()
            self._rejected_connections += 1
            return
        
        try:
            # Create connection state
            conn_id = self._generate_conn_id()
            conn = ConnectionState(conn_id, reader, writer)
            self.connections[conn_id] = conn
            
            self._total_connections += 1
            self._active_connections += 1
            
            addr = writer.get_extra_info('peername')
            logger.info(
                "Connection accepted",
                conn_id=conn_id,
                address=addr,
            )
            
            # Handle requests
            await self._handle_requests(conn)
        
        except Exception as e:
            logger.error(
                "Connection error",
                conn_id=conn_id,
                error=str(e),
            )
        
        finally:
            # Clean up connection
            if conn_id in self.connections:
                del self.connections[conn_id]
            
            writer.close()
            await writer.wait_closed()
            
            self._active_connections -= 1
            
            # Release semaphore
            if self.connection_semaphore:
                self.connection_semaphore.release()
            
            logger.info("Connection closed", conn_id=conn_id)
    
    async def _try_acquire_connection(self) -> bool:
        """
        Try to acquire connection slot.
        
        Returns:
            True if acquired
        """
        if not self.connection_semaphore:
            return False
        
        # Non-blocking acquire
        try:
            return self.connection_semaphore.locked() is False and \
                   await asyncio.wait_for(
                       self.connection_semaphore.acquire(),
                       timeout=0.001,
                   )
        except asyncio.TimeoutError:
            return False
    
    def _generate_conn_id(self) -> str:
        """
        Generate connection ID.
        
        Returns:
            Connection ID
        """
        conn_id = f"{self.broker_id}-{self._next_conn_id}"
        self._next_conn_id += 1
        return conn_id
    
    async def _handle_requests(self, conn: ConnectionState) -> None:
        """
        Handle requests from connection.
        
        Args:
            conn: Connection state
        """
        while True:
            try:
                # Check backpressure
                if conn.is_backpressured(self.config.backpressure_threshold):
                    self._backpressure_events += 1
                    
                    logger.warning(
                        "Backpressure detected, slowing down",
                        conn_id=conn.conn_id,
                        pending=conn.pending_requests,
                    )
                    
                    # Apply backpressure (slow down reader)
                    await asyncio.sleep(0.1)
                
                # Read request (non-blocking)
                data = await conn.reader.read(self.config.network_buffer_size)
                
                if not data:
                    # EOF, client closed connection
                    break
                
                conn.bytes_received += len(data)
                conn.pending_requests += 1
                
                # Process request (async)
                response = await self._process_request(conn, data)
                
                # Write response (non-blocking)
                conn.writer.write(response)
                await conn.writer.drain()
                
                conn.bytes_sent += len(response)
                conn.pending_requests -= 1
            
            except asyncio.CancelledError:
                logger.info("Request handling cancelled", conn_id=conn.conn_id)
                break
            
            except Exception as e:
                logger.error(
                    "Request handling error",
                    conn_id=conn.conn_id,
                    error=str(e),
                )
                break
    
    async def _process_request(
        self,
        conn: ConnectionState,
        data: bytes,
    ) -> bytes:
        """
        Process request (async).
        
        Args:
            conn: Connection state
            data: Request data
        
        Returns:
            Response data
        """
        # TODO: Implement actual request processing
        # For now, just echo
        return data
    
    async def read_from_disk_async(
        self,
        filepath: str,
        offset: int,
        size: int,
    ) -> bytes:
        """
        Read from disk asynchronously (using thread pool).
        
        Disk I/O is inherently blocking, so we use a thread pool.
        
        Args:
            filepath: File path
            offset: Offset in file
            size: Number of bytes to read
        
        Returns:
            Data bytes
        """
        loop = asyncio.get_running_loop()
        
        # Run blocking disk I/O in thread pool
        def _read():
            with open(filepath, 'rb') as f:
                f.seek(offset)
                return f.read(size)
        
        data = await loop.run_in_executor(self.disk_executor, _read)
        
        logger.debug(
            "Async disk read",
            filepath=filepath,
            offset=offset,
            bytes=len(data),
        )
        
        return data
    
    async def write_to_disk_async(
        self,
        filepath: str,
        data: bytes,
        offset: int,
    ) -> int:
        """
        Write to disk asynchronously (using thread pool).
        
        Args:
            filepath: File path
            data: Data to write
            offset: Offset in file
        
        Returns:
            Number of bytes written
        """
        loop = asyncio.get_running_loop()
        
        # Run blocking disk I/O in thread pool
        def _write():
            with open(filepath, 'r+b') as f:
                f.seek(offset)
                return f.write(data)
        
        bytes_written = await loop.run_in_executor(self.disk_executor, _write)
        
        logger.debug(
            "Async disk write",
            filepath=filepath,
            offset=offset,
            bytes=bytes_written,
        )
        
        return bytes_written
    
    def get_stats(self) -> dict:
        """
        Get broker statistics.
        
        Returns:
            Statistics dict
        """
        return {
            "broker_id": self.broker_id,
            "total_connections": self._total_connections,
            "active_connections": self._active_connections,
            "rejected_connections": self._rejected_connections,
            "backpressure_events": self._backpressure_events,
            "max_connections": self.config.max_connections,
            "disk_io_threads": self.config.disk_io_threads,
        }


class ConnectionMultiplexer:
    """
    Multiplexes multiple logical streams over a single connection.
    
    Allows concurrent requests on same TCP connection.
    """
    
    def __init__(self, max_streams: int = 100):
        """
        Initialize connection multiplexer.
        
        Args:
            max_streams: Maximum concurrent streams
        """
        self.max_streams = max_streams
        
        # stream_id -> Future
        self._active_streams: Dict[int, asyncio.Future] = {}
        self._next_stream_id = 0
        
        logger.info("ConnectionMultiplexer initialized", max_streams=max_streams)
    
    async def send_request(
        self,
        writer: asyncio.StreamWriter,
        data: bytes,
    ) -> int:
        """
        Send request on multiplexed connection.
        
        Args:
            writer: Stream writer
            data: Request data
        
        Returns:
            Stream ID
        """
        if len(self._active_streams) >= self.max_streams:
            raise RuntimeError("Too many active streams")
        
        stream_id = self._next_stream_id
        self._next_stream_id += 1
        
        # Create future for response
        future = asyncio.Future()
        self._active_streams[stream_id] = future
        
        # Send with stream ID header
        writer.write(stream_id.to_bytes(4, 'big') + data)
        await writer.drain()
        
        return stream_id
    
    async def wait_response(self, stream_id: int) -> bytes:
        """
        Wait for response on stream.
        
        Args:
            stream_id: Stream ID
        
        Returns:
            Response data
        """
        if stream_id not in self._active_streams:
            raise ValueError(f"Unknown stream ID: {stream_id}")
        
        # Wait for future
        response = await self._active_streams[stream_id]
        
        # Clean up
        del self._active_streams[stream_id]
        
        return response
    
    def complete_response(self, stream_id: int, data: bytes) -> None:
        """
        Complete response for stream.
        
        Args:
            stream_id: Stream ID
            data: Response data
        """
        if stream_id in self._active_streams:
            self._active_streams[stream_id].set_result(data)
