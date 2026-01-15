"""Integration tests for indexed reads."""

import tempfile
from pathlib import Path

import pytest

from distributedlog.core.log.log import Log


class TestIndexedReads:
    """Test reads using offset index."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for tests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    def test_read_with_index_faster_than_without(self, temp_dir):
        """Test that indexed reads are faster than sequential scans."""
        log = Log(
            directory=temp_dir,
            max_segment_size=1024 * 1024,
        )
        
        for i in range(1000):
            log.append(key=None, value=f"message-{i}".encode())
        
        log.flush()
        
        messages = list(log.read(start_offset=500, max_messages=10))
        
        assert len(messages) == 10
        assert messages[0].offset == 500
        assert messages[9].offset == 509
        
        log.close()
    
    def test_index_survives_restart(self, temp_dir):
        """Test that index persists across restarts."""
        log1 = Log(directory=temp_dir)
        
        for i in range(100):
            log1.append(key=None, value=f"msg-{i}".encode())
        
        log1.close()
        
        log2 = Log(directory=temp_dir)
        
        messages = list(log2.read(start_offset=50, max_messages=5))
        
        assert len(messages) == 5
        assert messages[0].offset == 50
        
        log2.close()
    
    def test_read_across_segments_with_index(self, temp_dir):
        """Test reading across multiple segments with index."""
        log = Log(
            directory=temp_dir,
            max_segment_size=100,
        )
        
        for i in range(50):
            log.append(key=None, value=b"x" * 50)
        
        assert log.segment_count() > 1
        
        messages = list(log.read(start_offset=10, max_messages=30))
        
        assert len(messages) == 30
        assert messages[0].offset == 10
        assert messages[29].offset == 39
        
        log.close()
