"""
Compression support for message batches.

Supports gzip, snappy, and lz4 compression to reduce network bandwidth.
"""

import gzip
import zlib
from enum import Enum
from typing import Optional

from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class CompressionType(Enum):
    """Supported compression types."""
    NONE = 0
    GZIP = 1
    SNAPPY = 2
    LZ4 = 3


class Compressor:
    """
    Handles compression and decompression of message batches.
    
    Different compression algorithms have different trade-offs:
    - GZIP: Best compression ratio, slowest
    - SNAPPY: Fast compression, moderate ratio
    - LZ4: Fastest compression, lower ratio
    """
    
    def __init__(self, compression_type: CompressionType = CompressionType.NONE):
        """
        Initialize compressor.
        
        Args:
            compression_type: Type of compression to use
        """
        self.compression_type = compression_type
        
        if compression_type == CompressionType.SNAPPY:
            try:
                import snappy
                self._snappy = snappy
            except ImportError:
                logger.warning("python-snappy not installed, falling back to GZIP")
                self.compression_type = CompressionType.GZIP
        
        if compression_type == CompressionType.LZ4:
            try:
                import lz4.frame
                self._lz4 = lz4.frame
            except ImportError:
                logger.warning("lz4 not installed, falling back to GZIP")
                self.compression_type = CompressionType.GZIP
        
        logger.info("Initialized compressor", compression_type=self.compression_type.name)
    
    def compress(self, data: bytes) -> bytes:
        """
        Compress data using configured algorithm.
        
        Args:
            data: Uncompressed data
        
        Returns:
            Compressed data
        """
        if self.compression_type == CompressionType.NONE:
            return data
        
        original_size = len(data)
        
        try:
            if self.compression_type == CompressionType.GZIP:
                compressed = gzip.compress(data, compresslevel=6)
            
            elif self.compression_type == CompressionType.SNAPPY:
                compressed = self._snappy.compress(data)
            
            elif self.compression_type == CompressionType.LZ4:
                compressed = self._lz4.compress(data)
            
            else:
                compressed = data
            
            compressed_size = len(compressed)
            compression_ratio = (1.0 - compressed_size / original_size) * 100
            
            logger.debug(
                "Compressed batch",
                compression_type=self.compression_type.name,
                original_size=original_size,
                compressed_size=compressed_size,
                compression_ratio=f"{compression_ratio:.1f}%",
            )
            
            return compressed
        
        except Exception as e:
            logger.error(
                "Compression failed, sending uncompressed",
                compression_type=self.compression_type.name,
                error=str(e),
            )
            return data
    
    def decompress(self, data: bytes, compression_type: Optional[CompressionType] = None) -> bytes:
        """
        Decompress data.
        
        Args:
            data: Compressed data
            compression_type: Override compression type (for reading)
        
        Returns:
            Decompressed data
        """
        comp_type = compression_type or self.compression_type
        
        if comp_type == CompressionType.NONE:
            return data
        
        try:
            if comp_type == CompressionType.GZIP:
                return gzip.decompress(data)
            
            elif comp_type == CompressionType.SNAPPY:
                return self._snappy.decompress(data)
            
            elif comp_type == CompressionType.LZ4:
                return self._lz4.decompress(data)
            
            else:
                return data
        
        except Exception as e:
            logger.error(
                "Decompression failed",
                compression_type=comp_type.name,
                error=str(e),
            )
            raise
    
    @staticmethod
    def estimate_compressed_size(data: bytes, compression_type: CompressionType) -> int:
        """
        Estimate compressed size without actually compressing.
        
        Uses heuristics based on typical compression ratios.
        
        Args:
            data: Uncompressed data
            compression_type: Compression algorithm
        
        Returns:
            Estimated compressed size
        """
        original_size = len(data)
        
        if compression_type == CompressionType.NONE:
            return original_size
        elif compression_type == CompressionType.GZIP:
            return int(original_size * 0.3)
        elif compression_type == CompressionType.SNAPPY:
            return int(original_size * 0.5)
        elif compression_type == CompressionType.LZ4:
            return int(original_size * 0.6)
        else:
            return original_size
    
    @staticmethod
    def should_compress(data: bytes, min_size: int = 1024) -> bool:
        """
        Determine if data should be compressed.
        
        Small payloads often grow after compression due to overhead.
        
        Args:
            data: Data to check
            min_size: Minimum size worth compressing
        
        Returns:
            True if compression is recommended
        """
        return len(data) >= min_size


def benchmark_compression(data: bytes) -> dict:
    """
    Benchmark all compression algorithms on data.
    
    Args:
        data: Data to compress
    
    Returns:
        Dictionary with compression stats
    """
    import time
    
    results = {}
    
    for comp_type in CompressionType:
        compressor = Compressor(comp_type)
        
        start = time.time()
        compressed = compressor.compress(data)
        compress_time = time.time() - start
        
        start = time.time()
        decompressed = compressor.decompress(compressed)
        decompress_time = time.time() - start
        
        assert decompressed == data or comp_type == CompressionType.NONE
        
        results[comp_type.name] = {
            "original_size": len(data),
            "compressed_size": len(compressed),
            "ratio": len(compressed) / len(data),
            "compress_time_ms": compress_time * 1000,
            "decompress_time_ms": decompress_time * 1000,
        }
    
    return results
