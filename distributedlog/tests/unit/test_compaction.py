"""Tests for log compaction."""

import tempfile
from pathlib import Path

import pytest

from distributedlog.core.log.compaction import LogCompactor
from distributedlog.core.log.log import Log


class TestLogCompaction:
    """Test log compaction."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for tests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    def test_compact_with_duplicates(self, temp_dir):
        """Test compacting segment with duplicate keys."""
        log = Log(directory=temp_dir)
        
        for i in range(10):
            log.append(key=b"key-1", value=f"value-{i}".encode())
            log.append(key=b"key-2", value=f"value-{i}".encode())
        
        log.close()
        
        compactor = LogCompactor(temp_dir)
        segment = log._segments[0]
        
        compacted = compactor.compact_segment(segment, min_cleanable_ratio=0.5)
        
        assert compacted is not None
        assert compacted.size() < segment.size()
    
    def test_compact_with_no_keys(self, temp_dir):
        """Test compacting segment with no keys."""
        log = Log(directory=temp_dir)
        
        for i in range(10):
            log.append(key=None, value=f"value-{i}".encode())
        
        log.close()
        
        compactor = LogCompactor(temp_dir)
        segment = log._segments[0]
        
        compacted = compactor.compact_segment(segment, min_cleanable_ratio=0.5)
        
        assert compacted is None
    
    def test_compact_insufficient_duplicates(self, temp_dir):
        """Test compaction skipped when insufficient duplicates."""
        log = Log(directory=temp_dir)
        
        for i in range(10):
            log.append(key=f"key-{i}".encode(), value=f"value-{i}".encode())
        
        log.close()
        
        compactor = LogCompactor(temp_dir)
        segment = log._segments[0]
        
        compacted = compactor.compact_segment(segment, min_cleanable_ratio=0.5)
        
        assert compacted is None
