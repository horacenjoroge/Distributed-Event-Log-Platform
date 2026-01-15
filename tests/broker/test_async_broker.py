"""
Tests for async I/O broker.
"""

import asyncio
import tempfile

import pytest

from distributedlog.broker.async_broker import (
    AsyncBroker,
    AsyncBrokerConfig,
    ConnectionMultiplexer,
)


class TestAsyncBroker:
    """Test AsyncBroker."""
    
    @pytest.mark.asyncio
    async def test_start_and_stop(self):
        """Test starting and stopping broker."""
        config = AsyncBrokerConfig(max_connections=10)
        broker = AsyncBroker("test-broker", port=19092, config=config)
        
        # Start broker
        await broker.start()
        
        assert broker.server is not None
        
        # Stop broker
        await broker.stop()
        
        assert len(broker.connections) == 0
    
    @pytest.mark.asyncio
    async def test_connection_limit(self):
        """Test connection limit enforcement."""
        config = AsyncBrokerConfig(max_connections=2)
        broker = AsyncBroker("test-broker", port=19093, config=config)
        
        await broker.start()
        
        # Stats should show limit
        stats = broker.get_stats()
        assert stats["max_connections"] == 2
        
        await broker.stop()
    
    @pytest.mark.asyncio
    async def test_async_disk_read(self):
        """Test async disk read."""
        broker = AsyncBroker("test-broker")
        
        # Create temp file
        with tempfile.NamedTemporaryFile(delete=False, mode='wb') as f:
            test_data = b"Hello, async world!"
            f.write(test_data)
            filepath = f.name
        
        try:
            # Read async
            data = await broker.read_from_disk_async(filepath, 0, len(test_data))
            
            assert data == test_data
        
        finally:
            import os
            os.unlink(filepath)
    
    @pytest.mark.asyncio
    async def test_async_disk_write(self):
        """Test async disk write."""
        broker = AsyncBroker("test-broker")
        
        # Create temp file
        with tempfile.NamedTemporaryFile(delete=False, mode='wb') as f:
            f.write(b"0" * 100)
            filepath = f.name
        
        try:
            # Write async
            test_data = b"Test"
            bytes_written = await broker.write_to_disk_async(filepath, test_data, 10)
            
            assert bytes_written == len(test_data)
            
            # Verify
            with open(filepath, 'rb') as f:
                f.seek(10)
                read_data = f.read(len(test_data))
                assert read_data == test_data
        
        finally:
            import os
            os.unlink(filepath)
    
    def test_stats(self):
        """Test statistics tracking."""
        broker = AsyncBroker("test-broker")
        
        stats = broker.get_stats()
        
        assert "broker_id" in stats
        assert "total_connections" in stats
        assert "active_connections" in stats


class TestConnectionMultiplexer:
    """Test ConnectionMultiplexer."""
    
    def test_initialization(self):
        """Test multiplexer initialization."""
        mux = ConnectionMultiplexer(max_streams=50)
        
        assert mux.max_streams == 50
    
    @pytest.mark.asyncio
    async def test_send_and_wait(self):
        """Test sending request and waiting for response."""
        mux = ConnectionMultiplexer()
        
        # Mock writer
        class MockWriter:
            def __init__(self):
                self.data = bytearray()
            
            def write(self, data):
                self.data.extend(data)
            
            async def drain(self):
                pass
        
        writer = MockWriter()
        
        # Send request
        stream_id = await mux.send_request(writer, b"test request")
        
        assert stream_id == 0
        assert len(writer.data) > 0
        
        # Complete response
        mux.complete_response(stream_id, b"test response")
        
        # Wait for response
        response = await mux.wait_response(stream_id)
        
        assert response == b"test response"
    
    @pytest.mark.asyncio
    async def test_max_streams(self):
        """Test max streams limit."""
        mux = ConnectionMultiplexer(max_streams=2)
        
        class MockWriter:
            def write(self, data):
                pass
            
            async def drain(self):
                pass
        
        writer = MockWriter()
        
        # Send max streams
        await mux.send_request(writer, b"msg1")
        await mux.send_request(writer, b"msg2")
        
        # Next should fail
        with pytest.raises(RuntimeError, match="Too many"):
            await mux.send_request(writer, b"msg3")


class TestAsyncBrokerConfig:
    """Test AsyncBrokerConfig."""
    
    def test_default_config(self):
        """Test default configuration."""
        config = AsyncBrokerConfig()
        
        assert config.max_connections == 10000
        assert config.disk_io_threads == 8
        assert config.network_buffer_size == 64 * 1024
        assert config.backpressure_threshold == 1000
    
    def test_custom_config(self):
        """Test custom configuration."""
        config = AsyncBrokerConfig(
            max_connections=5000,
            disk_io_threads=4,
        )
        
        assert config.max_connections == 5000
        assert config.disk_io_threads == 4
