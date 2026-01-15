"""
Batch fetch optimization for amortizing I/O overhead.

Fetches multiple messages in a single I/O operation.
"""

from dataclasses import dataclass
from typing import List, Optional

from distributedlog.core.io.buffer_pool import get_pooled_buffer
from distributedlog.core.io.zerocopy import MappedFileReader, get_optimal_transfer_method
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class FetchRequest:
    """
    Request to fetch messages from log.
    
    Attributes:
        topic: Topic name
        partition: Partition number
        offset: Starting offset
        max_bytes: Maximum bytes to fetch
        min_bytes: Minimum bytes to fetch
    """
    topic: str
    partition: int
    offset: int
    max_bytes: int = 1024 * 1024  # 1MB default
    min_bytes: int = 1  # At least 1 byte


@dataclass
class FetchedMessage:
    """
    Fetched message.
    
    Attributes:
        offset: Message offset
        key: Message key
        value: Message value
        timestamp: Message timestamp
    """
    offset: int
    key: Optional[bytes]
    value: bytes
    timestamp: int


@dataclass
class FetchResponse:
    """
    Response for fetch request.
    
    Attributes:
        topic: Topic name
        partition: Partition number
        messages: Fetched messages
        high_water_mark: Current high water mark
        bytes_read: Total bytes read
    """
    topic: str
    partition: int
    messages: List[FetchedMessage]
    high_water_mark: int
    bytes_read: int


class BatchFetcher:
    """
    Batch message fetcher with zero-copy optimizations.
    
    Amortizes I/O overhead by fetching multiple messages at once.
    
    Benefits:
    - Single I/O operation for multiple messages
    - Reduced syscall overhead
    - Better sequential disk access
    - Improved throughput
    """
    
    def __init__(
        self,
        use_zero_copy: bool = True,
        use_mmap: bool = True,
    ):
        """
        Initialize batch fetcher.
        
        Args:
            use_zero_copy: Enable zero-copy optimizations
            use_mmap: Use memory-mapped files
        """
        self.use_zero_copy = use_zero_copy
        self.use_mmap = use_mmap
        
        # Statistics
        self._total_fetches = 0
        self._total_messages = 0
        self._total_bytes = 0
        
        logger.info(
            "BatchFetcher initialized",
            zero_copy=use_zero_copy,
            mmap=use_mmap,
        )
    
    def fetch(
        self,
        filepath: str,
        offset: int,
        max_bytes: int,
    ) -> bytes:
        """
        Fetch data from log file.
        
        Args:
            filepath: Path to log file
            offset: Starting offset in file
            max_bytes: Maximum bytes to fetch
        
        Returns:
            Fetched data
        """
        self._total_fetches += 1
        
        # Determine transfer method
        transfer_method = get_optimal_transfer_method(
            file_size=max_bytes,
            use_tls=False,  # TODO: Check actual TLS setting
        )
        
        # Use memory-mapped file if enabled and appropriate
        if self.use_mmap and transfer_method == "mmap":
            return self._fetch_mmap(filepath, offset, max_bytes)
        
        # Fall back to standard read
        return self._fetch_standard(filepath, offset, max_bytes)
    
    def _fetch_mmap(
        self,
        filepath: str,
        offset: int,
        max_bytes: int,
    ) -> bytes:
        """
        Fetch using memory-mapped file.
        
        Args:
            filepath: Path to log file
            offset: Starting offset
            max_bytes: Maximum bytes
        
        Returns:
            Fetched data
        """
        try:
            with MappedFileReader(filepath, offset, max_bytes) as reader:
                data = reader.read(max_bytes)
                
                self._total_bytes += len(data)
                
                logger.debug(
                    "Batch fetched via mmap",
                    filepath=filepath,
                    offset=offset,
                    bytes=len(data),
                )
                
                return data
        
        except Exception as e:
            logger.warning(
                "mmap fetch failed, falling back to standard read",
                filepath=filepath,
                error=str(e),
            )
            return self._fetch_standard(filepath, offset, max_bytes)
    
    def _fetch_standard(
        self,
        filepath: str,
        offset: int,
        max_bytes: int,
    ) -> bytes:
        """
        Fetch using standard read.
        
        Args:
            filepath: Path to log file
            offset: Starting offset
            max_bytes: Maximum bytes
        
        Returns:
            Fetched data
        """
        # Use pooled buffer
        with get_pooled_buffer(max_bytes) as buffer:
            # Read from file
            with open(filepath, 'rb') as f:
                f.seek(offset)
                data = f.read(max_bytes)
            
            self._total_bytes += len(data)
            
            logger.debug(
                "Batch fetched via standard read",
                filepath=filepath,
                offset=offset,
                bytes=len(data),
            )
            
            return data
    
    def fetch_batch(
        self,
        requests: List[FetchRequest],
    ) -> List[FetchResponse]:
        """
        Fetch multiple requests in batch.
        
        Args:
            requests: List of fetch requests
        
        Returns:
            List of fetch responses
        """
        responses = []
        
        for request in requests:
            # TODO: Fetch from actual log
            # For now, return empty response
            
            response = FetchResponse(
                topic=request.topic,
                partition=request.partition,
                messages=[],
                high_water_mark=0,
                bytes_read=0,
            )
            
            responses.append(response)
        
        return responses
    
    def get_stats(self) -> dict:
        """
        Get fetcher statistics.
        
        Returns:
            Statistics dict
        """
        avg_bytes = (
            self._total_bytes / self._total_fetches
            if self._total_fetches > 0
            else 0
        )
        
        return {
            "total_fetches": self._total_fetches,
            "total_messages": self._total_messages,
            "total_bytes": self._total_bytes,
            "avg_bytes_per_fetch": round(avg_bytes, 2),
            "use_zero_copy": self.use_zero_copy,
            "use_mmap": self.use_mmap,
        }


class AdaptiveBatchFetcher(BatchFetcher):
    """
    Adaptive batch fetcher that adjusts fetch size based on workload.
    
    Dynamically tunes batch size to balance latency and throughput.
    """
    
    def __init__(
        self,
        initial_batch_size: int = 64 * 1024,  # 64KB
        min_batch_size: int = 4 * 1024,  # 4KB
        max_batch_size: int = 1024 * 1024,  # 1MB
        **kwargs,
    ):
        """
        Initialize adaptive batch fetcher.
        
        Args:
            initial_batch_size: Initial batch size
            min_batch_size: Minimum batch size
            max_batch_size: Maximum batch size
            **kwargs: Additional arguments for BatchFetcher
        """
        super().__init__(**kwargs)
        
        self.current_batch_size = initial_batch_size
        self.min_batch_size = min_batch_size
        self.max_batch_size = max_batch_size
        
        # Adaptation parameters
        self._consecutive_full = 0
        self._consecutive_partial = 0
        
        logger.info(
            "AdaptiveBatchFetcher initialized",
            initial_batch_size=initial_batch_size,
        )
    
    def adapt_batch_size(self, bytes_fetched: int) -> None:
        """
        Adapt batch size based on fetched bytes.
        
        Args:
            bytes_fetched: Number of bytes fetched
        """
        # If we fetched full batch, increase size
        if bytes_fetched >= self.current_batch_size * 0.9:
            self._consecutive_full += 1
            self._consecutive_partial = 0
            
            # Increase after 3 consecutive full fetches
            if self._consecutive_full >= 3:
                new_size = min(
                    self.current_batch_size * 2,
                    self.max_batch_size,
                )
                
                if new_size > self.current_batch_size:
                    logger.info(
                        "Increasing batch size",
                        old=self.current_batch_size,
                        new=new_size,
                    )
                    self.current_batch_size = new_size
                
                self._consecutive_full = 0
        
        # If we fetched partial batch, decrease size
        elif bytes_fetched < self.current_batch_size * 0.3:
            self._consecutive_partial += 1
            self._consecutive_full = 0
            
            # Decrease after 5 consecutive partial fetches
            if self._consecutive_partial >= 5:
                new_size = max(
                    self.current_batch_size // 2,
                    self.min_batch_size,
                )
                
                if new_size < self.current_batch_size:
                    logger.info(
                        "Decreasing batch size",
                        old=self.current_batch_size,
                        new=new_size,
                    )
                    self.current_batch_size = new_size
                
                self._consecutive_partial = 0
        
        # Reset counters if in between
        else:
            self._consecutive_full = 0
            self._consecutive_partial = 0
    
    def fetch(
        self,
        filepath: str,
        offset: int,
        max_bytes: Optional[int] = None,
    ) -> bytes:
        """
        Fetch with adaptive batch size.
        
        Args:
            filepath: Path to log file
            offset: Starting offset
            max_bytes: Maximum bytes (None = use current_batch_size)
        
        Returns:
            Fetched data
        """
        if max_bytes is None:
            max_bytes = self.current_batch_size
        
        # Fetch data
        data = super().fetch(filepath, offset, max_bytes)
        
        # Adapt batch size
        self.adapt_batch_size(len(data))
        
        return data
