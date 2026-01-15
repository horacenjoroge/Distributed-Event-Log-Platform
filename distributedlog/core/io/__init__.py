"""
I/O optimizations for zero-copy transfers.
"""

from distributedlog.core.io.batch_fetch import (
    AdaptiveBatchFetcher,
    BatchFetcher,
    FetchRequest,
    FetchResponse,
)
from distributedlog.core.io.buffer_pool import (
    BufferPool,
    BufferPoolManager,
    PooledBuffer,
    get_buffer_pool_manager,
    get_pooled_buffer,
)
from distributedlog.core.io.zerocopy import (
    DirectBufferTransfer,
    MappedFileReader,
    SendfileTransfer,
    ZeroCopySupport,
    get_optimal_transfer_method,
)

__all__ = [
    # Zero-copy
    "ZeroCopySupport",
    "SendfileTransfer",
    "MappedFileReader",
    "DirectBufferTransfer",
    "get_optimal_transfer_method",
    # Buffer pooling
    "BufferPool",
    "BufferPoolManager",
    "PooledBuffer",
    "get_buffer_pool_manager",
    "get_pooled_buffer",
    # Batch fetching
    "BatchFetcher",
    "AdaptiveBatchFetcher",
    "FetchRequest",
    "FetchResponse",
]
