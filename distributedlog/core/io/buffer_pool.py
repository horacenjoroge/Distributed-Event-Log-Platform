"""
Network buffer pooling for zero-copy transfers.

Reuses buffers to avoid allocation overhead and GC pressure.
"""

import threading
from collections import deque
from typing import Deque, Optional

from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class PooledBuffer:
    """
    Pooled buffer that returns to pool when released.
    
    Attributes:
        data: Underlying bytearray
        capacity: Buffer capacity
        size: Current data size
        pool: Parent pool (for return)
    """
    
    def __init__(self, capacity: int, pool: Optional['BufferPool'] = None):
        """
        Initialize pooled buffer.
        
        Args:
            capacity: Buffer capacity
            pool: Parent buffer pool
        """
        self.data = bytearray(capacity)
        self.capacity = capacity
        self.size = 0
        self.pool = pool
    
    def write(self, data: bytes, offset: int = 0) -> int:
        """
        Write data to buffer.
        
        Args:
            data: Data to write
            offset: Offset in buffer
        
        Returns:
            Number of bytes written
        """
        length = len(data)
        
        if offset + length > self.capacity:
            raise ValueError(f"Buffer overflow: {offset + length} > {self.capacity}")
        
        # Write using memoryview (zero-copy)
        view = memoryview(self.data)
        view[offset:offset + length] = data
        
        self.size = max(self.size, offset + length)
        
        return length
    
    def read(self, size: Optional[int] = None) -> bytes:
        """
        Read data from buffer.
        
        Args:
            size: Number of bytes to read (None = all)
        
        Returns:
            Data bytes
        """
        if size is None:
            size = self.size
        
        return bytes(self.data[:size])
    
    def view(self, offset: int = 0, size: Optional[int] = None) -> memoryview:
        """
        Get zero-copy view of buffer.
        
        Args:
            offset: Offset in buffer
            size: Size of view (None = remaining)
        
        Returns:
            Memory view
        """
        if size is None:
            return memoryview(self.data)[offset:self.size]
        else:
            return memoryview(self.data)[offset:offset + size]
    
    def clear(self) -> None:
        """Clear buffer (reset size)."""
        self.size = 0
    
    def release(self) -> None:
        """Release buffer back to pool."""
        if self.pool:
            self.pool.return_buffer(self)
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit (auto-release)."""
        self.release()


class BufferPool:
    """
    Object pool for network buffers.
    
    Reduces allocation overhead and GC pressure by reusing buffers.
    
    Benefits:
    - No allocation per request
    - Reduced GC pressure
    - Predictable memory usage
    - Better cache locality
    """
    
    def __init__(
        self,
        buffer_size: int = 64 * 1024,  # 64KB default
        max_buffers: int = 1000,
        preallocate: int = 10,
    ):
        """
        Initialize buffer pool.
        
        Args:
            buffer_size: Size of each buffer
            max_buffers: Maximum buffers in pool
            preallocate: Number of buffers to pre-allocate
        """
        self.buffer_size = buffer_size
        self.max_buffers = max_buffers
        
        # Pool of available buffers
        self._pool: Deque[PooledBuffer] = deque()
        self._lock = threading.Lock()
        
        # Statistics
        self._allocated = 0
        self._in_use = 0
        self._total_gets = 0
        self._total_returns = 0
        self._pool_hits = 0
        self._pool_misses = 0
        
        # Pre-allocate buffers
        for _ in range(preallocate):
            self._pool.append(PooledBuffer(buffer_size, pool=self))
            self._allocated += 1
        
        logger.info(
            "BufferPool initialized",
            buffer_size=buffer_size,
            max_buffers=max_buffers,
            preallocated=preallocate,
        )
    
    def get_buffer(self) -> PooledBuffer:
        """
        Get buffer from pool.
        
        Returns:
            Pooled buffer
        """
        self._total_gets += 1
        
        with self._lock:
            # Try to get from pool
            if self._pool:
                buffer = self._pool.popleft()
                buffer.clear()
                self._in_use += 1
                self._pool_hits += 1
                
                logger.debug("Buffer acquired from pool")
                
                return buffer
            
            # Pool empty, allocate new buffer
            if self._allocated < self.max_buffers:
                buffer = PooledBuffer(self.buffer_size, pool=self)
                self._allocated += 1
                self._in_use += 1
                self._pool_misses += 1
                
                logger.debug(
                    "New buffer allocated",
                    allocated=self._allocated,
                )
                
                return buffer
            
            # Pool exhausted
            self._pool_misses += 1
            logger.warning(
                "Buffer pool exhausted, allocating non-pooled buffer",
                max_buffers=self.max_buffers,
            )
            
            # Return non-pooled buffer (won't be returned to pool)
            return PooledBuffer(self.buffer_size, pool=None)
    
    def return_buffer(self, buffer: PooledBuffer) -> None:
        """
        Return buffer to pool.
        
        Args:
            buffer: Buffer to return
        """
        self._total_returns += 1
        
        with self._lock:
            # Clear buffer
            buffer.clear()
            
            # Add back to pool
            self._pool.append(buffer)
            self._in_use -= 1
            
            logger.debug(
                "Buffer returned to pool",
                pool_size=len(self._pool),
            )
    
    def get_stats(self) -> dict:
        """
        Get pool statistics.
        
        Returns:
            Statistics dict
        """
        with self._lock:
            hit_rate = (
                self._pool_hits / self._total_gets * 100
                if self._total_gets > 0
                else 0
            )
            
            return {
                "buffer_size": self.buffer_size,
                "allocated": self._allocated,
                "in_use": self._in_use,
                "available": len(self._pool),
                "max_buffers": self.max_buffers,
                "total_gets": self._total_gets,
                "total_returns": self._total_returns,
                "pool_hits": self._pool_hits,
                "pool_misses": self._pool_misses,
                "hit_rate_percent": round(hit_rate, 2),
            }
    
    def clear(self) -> None:
        """Clear pool (release all buffers)."""
        with self._lock:
            self._pool.clear()
            self._allocated = 0
            self._in_use = 0
            
            logger.info("Buffer pool cleared")


class BufferPoolManager:
    """
    Manages multiple buffer pools of different sizes.
    
    Different use cases need different buffer sizes:
    - Small (4KB): Control messages
    - Medium (64KB): Standard messages
    - Large (1MB): Batch fetches
    """
    
    def __init__(self):
        """Initialize buffer pool manager."""
        self.pools = {
            "small": BufferPool(buffer_size=4 * 1024, max_buffers=1000, preallocate=50),
            "medium": BufferPool(buffer_size=64 * 1024, max_buffers=500, preallocate=20),
            "large": BufferPool(buffer_size=1024 * 1024, max_buffers=100, preallocate=5),
        }
        
        logger.info("BufferPoolManager initialized with 3 pools")
    
    def get_buffer(self, size: int) -> PooledBuffer:
        """
        Get appropriately-sized buffer.
        
        Args:
            size: Requested size
        
        Returns:
            Pooled buffer
        """
        if size <= 4 * 1024:
            return self.pools["small"].get_buffer()
        elif size <= 64 * 1024:
            return self.pools["medium"].get_buffer()
        elif size <= 1024 * 1024:
            return self.pools["large"].get_buffer()
        else:
            # Very large, allocate non-pooled
            logger.warning(
                "Requested size exceeds largest pool, allocating non-pooled buffer",
                size=size,
            )
            return PooledBuffer(size, pool=None)
    
    def get_stats(self) -> dict:
        """
        Get statistics for all pools.
        
        Returns:
            Statistics dict
        """
        return {
            name: pool.get_stats()
            for name, pool in self.pools.items()
        }


# Global buffer pool manager
_global_pool_manager: Optional[BufferPoolManager] = None
_pool_lock = threading.Lock()


def get_buffer_pool_manager() -> BufferPoolManager:
    """
    Get global buffer pool manager (singleton).
    
    Returns:
        BufferPoolManager instance
    """
    global _global_pool_manager
    
    if _global_pool_manager is None:
        with _pool_lock:
            if _global_pool_manager is None:
                _global_pool_manager = BufferPoolManager()
    
    return _global_pool_manager


def get_pooled_buffer(size: int) -> PooledBuffer:
    """
    Get pooled buffer from global pool manager.
    
    Args:
        size: Requested size
    
    Returns:
        Pooled buffer
    """
    return get_buffer_pool_manager().get_buffer(size)
