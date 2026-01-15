"""Tests for producer compression."""

import pytest

from distributedlog.producer.compression import Compressor, CompressionType


class TestCompression:
    """Test compression."""
    
    def test_no_compression(self):
        """Test no compression."""
        compressor = Compressor(CompressionType.NONE)
        
        data = b"hello world" * 100
        compressed = compressor.compress(data)
        
        assert compressed == data
    
    def test_gzip_compression(self):
        """Test GZIP compression."""
        compressor = Compressor(CompressionType.GZIP)
        
        data = b"hello world" * 100
        compressed = compressor.compress(data)
        
        assert len(compressed) < len(data)
        
        decompressed = compressor.decompress(compressed)
        assert decompressed == data
    
    def test_compression_ratio(self):
        """Test compression reduces size."""
        compressor = Compressor(CompressionType.GZIP)
        
        data = b"a" * 10000
        compressed = compressor.compress(data)
        
        ratio = len(compressed) / len(data)
        assert ratio < 0.1
    
    def test_should_compress(self):
        """Test compression recommendation."""
        small_data = b"x" * 100
        large_data = b"x" * 2000
        
        assert not Compressor.should_compress(small_data)
        assert Compressor.should_compress(large_data)
    
    def test_estimate_compressed_size(self):
        """Test compression size estimation."""
        data = b"x" * 1000
        
        estimate = Compressor.estimate_compressed_size(
            data,
            CompressionType.GZIP,
        )
        
        assert estimate < len(data)
        assert estimate == 300
