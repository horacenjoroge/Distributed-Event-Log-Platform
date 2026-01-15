"""Tests for log retention policies."""

import tempfile
import time
from pathlib import Path

import pytest

from distributedlog.core.log.log import Log
from distributedlog.core.log.retention import RetentionManager, RetentionPolicy
from distributedlog.core.log.segment import LogSegment


class TestRetentionManager:
    """Test RetentionManager."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for tests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    def test_no_retention_policy(self):
        """Test manager with no retention policy."""
        manager = RetentionManager()
        
        assert manager.policy is None
    
    def test_time_based_policy(self):
        """Test time-based retention policy."""
        manager = RetentionManager(retention_hours=24)
        
        assert manager.policy == RetentionPolicy.TIME_BASED
    
    def test_size_based_policy(self):
        """Test size-based retention policy."""
        manager = RetentionManager(retention_bytes=1000000)
        
        assert manager.policy == RetentionPolicy.SIZE_BASED
    
    def test_combined_policy(self):
        """Test combined retention policy."""
        manager = RetentionManager(
            retention_hours=24,
            retention_bytes=1000000,
        )
        
        assert manager.policy == RetentionPolicy.BOTH
    
    def test_time_based_deletion(self, temp_dir):
        """Test time-based segment deletion."""
        manager = RetentionManager(retention_hours=1)
        
        log = Log(directory=temp_dir)
        
        for i in range(5):
            log.append(key=None, value=f"msg-{i}".encode())
        
        segments = list(log._segments)
        active = log._active_segment
        
        old_segment = segments[0]
        old_segment.path.touch()
        old_time = time.time() - 7200
        import os
        os.utime(old_segment.path, (old_time, old_time))
        
        to_delete = manager.segments_to_delete(segments, active)
        
        assert len(to_delete) > 0
        assert old_segment in to_delete
        assert active not in to_delete
        
        log.close()
    
    def test_size_based_deletion(self, temp_dir):
        """Test size-based segment deletion."""
        log = Log(
            directory=temp_dir,
            max_segment_size=100,
        )
        
        for i in range(20):
            log.append(key=None, value=b"x" * 50)
        
        assert log.segment_count() > 1
        
        manager = RetentionManager(retention_bytes=100)
        
        to_delete = manager.segments_to_delete(
            log._segments,
            log._active_segment,
        )
        
        assert len(to_delete) > 0
        
        log.close()
    
    def test_delete_segments(self, temp_dir):
        """Test deleting segments."""
        log = Log(
            directory=temp_dir,
            max_segment_size=100,
        )
        
        for i in range(10):
            log.append(key=None, value=b"x" * 50)
        
        segments_to_delete = log._segments[:-1]
        
        manager = RetentionManager()
        deleted = manager.delete_segments(segments_to_delete)
        
        assert deleted == len(segments_to_delete)
        
        for segment in segments_to_delete:
            assert not segment.path.exists()
        
        log.close()


class TestLogWithRetention:
    """Test Log with retention enabled."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for tests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    def test_retention_during_write(self, temp_dir):
        """Test retention policy application."""
        log = Log(
            directory=temp_dir,
            max_segment_size=100,
            retention_bytes=500,
        )
        
        for i in range(50):
            log.append(key=None, value=b"x" * 50)
        
        initial_segments = log.segment_count()
        
        log._apply_retention()
        
        final_segments = log.segment_count()
        
        assert final_segments < initial_segments
        
        log.close()
