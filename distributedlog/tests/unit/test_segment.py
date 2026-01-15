"""Tests for log segment operations."""

import tempfile
from pathlib import Path

import pytest

from distributedlog.core.log.format import Message
from distributedlog.core.log.segment import LogSegment


class TestLogSegment:
    """Test LogSegment class."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for tests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    def test_create_segment(self, temp_dir):
        """Test creating a new segment."""
        segment = LogSegment(
            base_offset=0,
            directory=temp_dir,
            max_size_bytes=1024,
        )
        
        assert segment.base_offset == 0
        assert segment.next_offset() == 0
        assert segment.size() == 0
        assert not segment.is_full()
        
        segment.close()
    
    def test_segment_file_naming(self, temp_dir):
        """Test that segment files are named correctly."""
        segment = LogSegment(
            base_offset=12345,
            directory=temp_dir,
            max_size_bytes=1024,
        )
        
        expected_name = "00000000000000012345.log"
        assert segment.path.name == expected_name
        assert segment.path.exists()
        
        segment.close()
    
    def test_append_single_message(self, temp_dir):
        """Test appending a single message."""
        segment = LogSegment(
            base_offset=0,
            directory=temp_dir,
            max_size_bytes=1024,
        )
        
        msg = Message(
            offset=0,
            timestamp=1234567890,
            key=b"key",
            value=b"value",
        )
        
        offset = segment.append(msg)
        
        assert offset == 0
        assert segment.next_offset() == 1
        assert segment.size() > 0
        
        segment.close()
    
    def test_append_multiple_messages(self, temp_dir):
        """Test appending multiple messages."""
        segment = LogSegment(
            base_offset=100,
            directory=temp_dir,
            max_size_bytes=10240,
        )
        
        offsets = []
        for i in range(10):
            msg = Message(
                offset=0,
                timestamp=1234567890 + i,
                key=f"key-{i}".encode(),
                value=f"value-{i}".encode(),
            )
            offset = segment.append(msg)
            offsets.append(offset)
        
        assert offsets == list(range(100, 110))
        assert segment.next_offset() == 110
        assert segment.size() > 0
        
        segment.close()
    
    def test_segment_full_detection(self, temp_dir):
        """Test that segment detects when it's full."""
        segment = LogSegment(
            base_offset=0,
            directory=temp_dir,
            max_size_bytes=100,
        )
        
        msg = Message(
            offset=0,
            timestamp=1234567890,
            key=None,
            value=b"x" * 50,
        )
        
        segment.append(msg)
        
        assert segment.is_full()
        
        with pytest.raises(ValueError, match="Segment is full"):
            segment.append(msg)
        
        segment.close()
    
    def test_flush_segment(self, temp_dir):
        """Test flushing segment to disk."""
        segment = LogSegment(
            base_offset=0,
            directory=temp_dir,
            max_size_bytes=1024,
        )
        
        msg = Message(
            offset=0,
            timestamp=1234567890,
            key=None,
            value=b"test",
        )
        
        segment.append(msg)
        segment.flush()
        
        assert segment.path.exists()
        assert segment.path.stat().st_size > 0
        
        segment.close()
    
    def test_fsync_on_append(self, temp_dir):
        """Test segment with fsync on every append."""
        segment = LogSegment(
            base_offset=0,
            directory=temp_dir,
            max_size_bytes=1024,
            fsync_on_append=True,
        )
        
        msg = Message(
            offset=0,
            timestamp=1234567890,
            key=None,
            value=b"test",
        )
        
        segment.append(msg)
        
        assert segment.path.stat().st_size > 0
        
        segment.close()
    
    def test_context_manager(self, temp_dir):
        """Test using segment as context manager."""
        with LogSegment(
            base_offset=0,
            directory=temp_dir,
            max_size_bytes=1024,
        ) as segment:
            msg = Message(
                offset=0,
                timestamp=1234567890,
                key=None,
                value=b"test",
            )
            segment.append(msg)
        
        assert segment.path.exists()
    
    def test_reopen_segment(self, temp_dir):
        """Test reopening an existing segment."""
        segment1 = LogSegment(
            base_offset=0,
            directory=temp_dir,
            max_size_bytes=1024,
        )
        
        msg = Message(
            offset=0,
            timestamp=1234567890,
            key=None,
            value=b"test",
        )
        
        segment1.append(msg)
        initial_size = segment1.size()
        segment1.close()
        
        segment2 = LogSegment(
            base_offset=0,
            directory=temp_dir,
            max_size_bytes=1024,
        )
        
        assert segment2.size() == initial_size
        
        segment2.close()
    
    def test_negative_base_offset_raises_error(self, temp_dir):
        """Test that negative base offset raises ValueError."""
        with pytest.raises(ValueError, match="Base offset must be non-negative"):
            LogSegment(
                base_offset=-1,
                directory=temp_dir,
                max_size_bytes=1024,
            )
