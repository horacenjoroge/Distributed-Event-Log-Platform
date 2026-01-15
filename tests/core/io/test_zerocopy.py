"""
Tests for zero-copy transfers.
"""

import os
import tempfile

import pytest

from distributedlog.core.io import (
    AdaptiveBatchFetcher,
    BatchFetcher,
    BufferPool,
    DirectBufferTransfer,
    MappedFileReader,
    SendfileTransfer,
    ZeroCopySupport,
    get_optimal_transfer_method,
    get_pooled_buffer,
)


class TestZeroCopySupport:
    """Test ZeroCopySupport."""
    
    def test_platform_detection(self):
        """Test platform detection."""
        # Should detect current platform
        assert ZeroCopySupport.is_linux() or \
               ZeroCopySupport.is_macos() or \
               ZeroCopySupport.is_windows()
    
    def test_sendfile_support(self):
        """Test sendfile() support detection."""
        # Linux and macOS have sendfile
        if ZeroCopySupport.is_linux() or ZeroCopySupport.is_macos():
            assert ZeroCopySupport.supports_sendfile()
    
    def test_get_platform_info(self):
        """Test getting platform info."""
        info = ZeroCopySupport.get_platform_info()
        
        assert "system" in info
        assert "sendfile_support" in info


class TestSendfileTransfer:
    """Test SendfileTransfer."""
    
    def test_initialization(self):
        """Test sendfile transfer initialization."""
        transfer = SendfileTransfer()
        
        assert transfer.platform is not None
    
    def test_can_use_sendfile(self):
        """Test checking if sendfile can be used."""
        transfer = SendfileTransfer()
        
        # Cannot use with TLS
        assert not transfer.can_use_sendfile(use_tls=True)
    
    @pytest.mark.skipif(
        not ZeroCopySupport.is_linux(),
        reason="sendfile() only tested on Linux"
    )
    def test_sendfile_to_socket(self):
        """Test sendfile to socket (Linux only)."""
        # This test would require actual socket fd
        # Skip for now as it needs integration testing
        pass


class TestMappedFileReader:
    """Test MappedFileReader."""
    
    def test_map_and_read(self):
        """Test mapping file and reading."""
        # Create temporary file
        with tempfile.NamedTemporaryFile(delete=False) as f:
            test_data = b"Hello, zero-copy world!"
            f.write(test_data)
            filepath = f.name
        
        try:
            # Map and read file
            with MappedFileReader(filepath) as reader:
                data = reader.read()
                
                assert data == test_data
        
        finally:
            os.unlink(filepath)
    
    def test_read_at_offset(self):
        """Test reading at specific offset."""
        # Create temporary file
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"0123456789")
            filepath = f.name
        
        try:
            # Map file starting at offset 5
            with MappedFileReader(filepath, offset=0) as reader:
                # Read from position 5
                data = reader.read_at(5, 3)
                
                assert data == b"567"
        
        finally:
            os.unlink(filepath)
    
    def test_context_manager(self):
        """Test context manager auto-close."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"test")
            filepath = f.name
        
        try:
            reader = MappedFileReader(filepath)
            
            with reader:
                data = reader.read()
                assert data == b"test"
            
            # Should be closed
            assert reader.mmap is None
            assert reader.fd is None
        
        finally:
            os.unlink(filepath)


class TestDirectBufferTransfer:
    """Test DirectBufferTransfer."""
    
    def test_create_view(self):
        """Test creating memory view."""
        data = b"0123456789"
        
        # Create view with offset
        view = DirectBufferTransfer.create_view(data, offset=2, size=3)
        
        assert bytes(view) == b"234"
    
    def test_transfer_to_buffer(self):
        """Test transferring to buffer."""
        source = b"Hello"
        dest = bytearray(10)
        
        # Transfer at offset 2
        transferred = DirectBufferTransfer.transfer_to_buffer(source, dest, offset=2)
        
        assert transferred == 5
        assert dest[2:7] == b"Hello"


class TestBufferPool:
    """Test BufferPool."""
    
    def test_get_and_return(self):
        """Test getting and returning buffers."""
        pool = BufferPool(buffer_size=1024, max_buffers=10, preallocate=2)
        
        # Get buffer
        buffer = pool.get_buffer()
        
        assert buffer is not None
        assert buffer.capacity == 1024
        
        # Return buffer
        buffer.release()
        
        # Pool should have it
        stats = pool.get_stats()
        assert stats["available"] >= 1
    
    def test_buffer_reuse(self):
        """Test buffer reuse (pooling)."""
        pool = BufferPool(buffer_size=1024, max_buffers=10, preallocate=1)
        
        # Get and return
        buffer1 = pool.get_buffer()
        buffer1_id = id(buffer1)
        buffer1.release()
        
        # Get again (should reuse)
        buffer2 = pool.get_buffer()
        buffer2_id = id(buffer2)
        
        assert buffer1_id == buffer2_id  # Same buffer object
    
    def test_pool_exhaustion(self):
        """Test pool exhaustion."""
        pool = BufferPool(buffer_size=1024, max_buffers=2, preallocate=0)
        
        # Get max buffers
        buffer1 = pool.get_buffer()
        buffer2 = pool.get_buffer()
        
        # Get one more (pool exhausted)
        buffer3 = pool.get_buffer()
        
        # Should allocate non-pooled buffer
        assert buffer3.pool is None
    
    def test_context_manager(self):
        """Test buffer context manager."""
        pool = BufferPool(buffer_size=1024)
        
        with pool.get_buffer() as buffer:
            buffer.write(b"test")
        
        # Should be returned to pool
        stats = pool.get_stats()
        assert stats["in_use"] == 0


class TestPooledBuffer:
    """Test PooledBuffer."""
    
    def test_write_and_read(self):
        """Test writing and reading."""
        buffer = get_pooled_buffer(1024)
        
        # Write data
        buffer.write(b"Hello, World!")
        
        # Read data
        data = buffer.read()
        
        assert data == b"Hello, World!"
    
    def test_view_zerocopy(self):
        """Test zero-copy view."""
        buffer = get_pooled_buffer(1024)
        buffer.write(b"0123456789")
        
        # Get view (zero-copy)
        view = buffer.view(2, 3)
        
        assert bytes(view) == b"234"
    
    def test_clear(self):
        """Test clearing buffer."""
        buffer = get_pooled_buffer(1024)
        buffer.write(b"test")
        
        assert buffer.size == 4
        
        buffer.clear()
        
        assert buffer.size == 0


class TestBatchFetcher:
    """Test BatchFetcher."""
    
    def test_initialization(self):
        """Test batch fetcher initialization."""
        fetcher = BatchFetcher(use_zero_copy=True, use_mmap=True)
        
        assert fetcher.use_zero_copy
        assert fetcher.use_mmap
    
    def test_fetch_mmap(self):
        """Test fetching with mmap."""
        # Create temporary file
        with tempfile.NamedTemporaryFile(delete=False) as f:
            test_data = b"Message 1\nMessage 2\nMessage 3\n"
            f.write(test_data)
            filepath = f.name
        
        try:
            fetcher = BatchFetcher(use_mmap=True)
            
            # Fetch data
            data = fetcher.fetch(filepath, offset=0, max_bytes=len(test_data))
            
            assert data == test_data
            
            # Check stats
            stats = fetcher.get_stats()
            assert stats["total_fetches"] == 1
            assert stats["total_bytes"] == len(test_data)
        
        finally:
            os.unlink(filepath)
    
    def test_fetch_standard(self):
        """Test fetching with standard read."""
        # Create temporary file
        with tempfile.NamedTemporaryFile(delete=False) as f:
            test_data = b"Test data"
            f.write(test_data)
            filepath = f.name
        
        try:
            fetcher = BatchFetcher(use_mmap=False)
            
            # Fetch data
            data = fetcher.fetch(filepath, offset=0, max_bytes=len(test_data))
            
            assert data == test_data
        
        finally:
            os.unlink(filepath)


class TestAdaptiveBatchFetcher:
    """Test AdaptiveBatchFetcher."""
    
    def test_batch_size_increase(self):
        """Test batch size increases on full fetches."""
        fetcher = AdaptiveBatchFetcher(
            initial_batch_size=1024,
            min_batch_size=512,
            max_batch_size=4096,
        )
        
        initial_size = fetcher.current_batch_size
        
        # Simulate 3 consecutive full fetches
        for _ in range(3):
            fetcher.adapt_batch_size(fetcher.current_batch_size)
        
        # Should increase
        assert fetcher.current_batch_size > initial_size
    
    def test_batch_size_decrease(self):
        """Test batch size decreases on partial fetches."""
        fetcher = AdaptiveBatchFetcher(
            initial_batch_size=4096,
            min_batch_size=512,
            max_batch_size=8192,
        )
        
        initial_size = fetcher.current_batch_size
        
        # Simulate 5 consecutive partial fetches (< 30%)
        for _ in range(5):
            fetcher.adapt_batch_size(int(fetcher.current_batch_size * 0.2))
        
        # Should decrease
        assert fetcher.current_batch_size < initial_size
    
    def test_batch_size_bounds(self):
        """Test batch size respects bounds."""
        fetcher = AdaptiveBatchFetcher(
            initial_batch_size=1024,
            min_batch_size=512,
            max_batch_size=2048,
        )
        
        # Try to increase beyond max
        for _ in range(10):
            fetcher.adapt_batch_size(fetcher.current_batch_size)
        
        assert fetcher.current_batch_size <= 2048
        
        # Try to decrease below min
        for _ in range(20):
            fetcher.adapt_batch_size(0)
        
        assert fetcher.current_batch_size >= 512


class TestOptimalTransferMethod:
    """Test optimal transfer method selection."""
    
    def test_sendfile_for_large_files_linux(self):
        """Test sendfile selected for large files on Linux."""
        method = get_optimal_transfer_method(
            file_size=100 * 1024,  # 100KB
            use_tls=False,
            platform_override="Linux",
        )
        
        # Should prefer sendfile on Linux for large files
        assert method == "sendfile"
    
    def test_mmap_for_medium_files(self):
        """Test mmap selected for medium files."""
        method = get_optimal_transfer_method(
            file_size=50 * 1024,  # 50KB
            use_tls=False,
        )
        
        # Should use mmap for medium files
        assert method == "mmap"
    
    def test_standard_for_small_files(self):
        """Test standard read for small files."""
        method = get_optimal_transfer_method(
            file_size=1024,  # 1KB
            use_tls=False,
        )
        
        # Should use standard for small files
        assert method == "standard"
    
    def test_standard_with_tls(self):
        """Test standard read with TLS."""
        method = get_optimal_transfer_method(
            file_size=1024 * 1024,  # 1MB
            use_tls=True,
        )
        
        # TLS requires user-space, use standard
        assert method == "standard"
