"""Tests for log manager with rotation."""

import tempfile
import time
from pathlib import Path

import pytest

from distributedlog.core.log.log import Log


class TestLog:
    """Test Log class with multiple segments."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for tests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    def test_create_log(self, temp_dir):
        """Test creating a new log."""
        log = Log(
            directory=temp_dir,
            max_segment_size=1024,
        )
        
        assert log.segment_count() == 1
        assert log.get_latest_offset() == -1
        
        log.close()
    
    def test_append_single_message(self, temp_dir):
        """Test appending a single message."""
        log = Log(
            directory=temp_dir,
            max_segment_size=1024,
        )
        
        offset = log.append(key=b"key", value=b"value")
        
        assert offset == 0
        assert log.get_latest_offset() == 0
        
        log.close()
    
    def test_append_multiple_messages(self, temp_dir):
        """Test appending multiple messages."""
        log = Log(
            directory=temp_dir,
            max_segment_size=10240,
        )
        
        offsets = []
        for i in range(10):
            offset = log.append(
                key=f"key-{i}".encode(),
                value=f"value-{i}".encode(),
            )
            offsets.append(offset)
        
        assert offsets == list(range(10))
        assert log.get_latest_offset() == 9
        
        log.close()
    
    def test_log_rotation_by_size(self, temp_dir):
        """Test that log rotates when segment is full."""
        log = Log(
            directory=temp_dir,
            max_segment_size=100,
        )
        
        for i in range(5):
            log.append(key=None, value=b"x" * 50)
        
        assert log.segment_count() > 1
        
        log.close()
    
    def test_read_messages(self, temp_dir):
        """Test reading messages from log."""
        log = Log(
            directory=temp_dir,
            max_segment_size=1024,
        )
        
        for i in range(5):
            log.append(
                key=f"key-{i}".encode(),
                value=f"value-{i}".encode(),
            )
        
        messages = list(log.read(start_offset=0, max_messages=10))
        
        assert len(messages) == 5
        assert messages[0].offset == 0
        assert messages[0].key == b"key-0"
        assert messages[4].offset == 4
        
        log.close()
    
    def test_read_from_middle(self, temp_dir):
        """Test reading from middle of log."""
        log = Log(
            directory=temp_dir,
            max_segment_size=1024,
        )
        
        for i in range(10):
            log.append(key=None, value=f"msg-{i}".encode())
        
        messages = list(log.read(start_offset=5, max_messages=3))
        
        assert len(messages) == 3
        assert messages[0].offset == 5
        assert messages[1].offset == 6
        assert messages[2].offset == 7
        
        log.close()
    
    def test_read_across_segments(self, temp_dir):
        """Test reading across multiple segments."""
        log = Log(
            directory=temp_dir,
            max_segment_size=100,
        )
        
        for i in range(10):
            log.append(key=None, value=b"x" * 50)
        
        messages = list(log.read(start_offset=0, max_messages=100))
        
        assert len(messages) == 10
        assert messages[0].offset == 0
        assert messages[9].offset == 9
        
        log.close()
    
    def test_log_recovery(self, temp_dir):
        """Test recovering log from disk."""
        log1 = Log(
            directory=temp_dir,
            max_segment_size=1024,
        )
        
        for i in range(5):
            log1.append(key=None, value=f"msg-{i}".encode())
        
        log1.close()
        
        log2 = Log(
            directory=temp_dir,
            max_segment_size=1024,
        )
        
        assert log2.get_latest_offset() == 4
        
        messages = list(log2.read(start_offset=0, max_messages=10))
        assert len(messages) == 5
        
        log2.close()
    
    def test_flush_log(self, temp_dir):
        """Test flushing log to disk."""
        log = Log(
            directory=temp_dir,
            max_segment_size=1024,
        )
        
        log.append(key=None, value=b"test")
        log.flush()
        
        segment_files = list(temp_dir.glob("*.log"))
        assert len(segment_files) >= 1
        assert segment_files[0].stat().st_size > 0
        
        log.close()
    
    def test_context_manager(self, temp_dir):
        """Test using log as context manager."""
        with Log(
            directory=temp_dir,
            max_segment_size=1024,
        ) as log:
            log.append(key=None, value=b"test")
        
        segment_files = list(temp_dir.glob("*.log"))
        assert len(segment_files) >= 1
    
    def test_append_without_key(self, temp_dir):
        """Test appending messages without keys."""
        log = Log(
            directory=temp_dir,
            max_segment_size=1024,
        )
        
        offset = log.append(key=None, value=b"value-only")
        
        messages = list(log.read(start_offset=offset, max_messages=1))
        assert len(messages) == 1
        assert messages[0].key is None
        assert messages[0].value == b"value-only"
        
        log.close()
