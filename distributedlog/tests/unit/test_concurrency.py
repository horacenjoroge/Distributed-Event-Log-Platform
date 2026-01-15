"""Tests for concurrent access to log."""

import tempfile
import threading
import time
from pathlib import Path

import pytest

from distributedlog.core.log.log import Log


class TestConcurrentAccess:
    """Test concurrent read/write operations."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for tests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    def test_concurrent_writes(self, temp_dir):
        """Test concurrent writes from multiple threads."""
        log = Log(directory=temp_dir)
        
        def writer(thread_id, count):
            for i in range(count):
                log.append(
                    key=f"thread-{thread_id}".encode(),
                    value=f"msg-{i}".encode(),
                )
        
        threads = []
        for i in range(5):
            t = threading.Thread(target=writer, args=(i, 20))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        assert log.get_latest_offset() == 99
        
        log.close()
    
    def test_concurrent_read_write(self, temp_dir):
        """Test concurrent reads and writes."""
        log = Log(directory=temp_dir)
        
        read_messages = []
        
        def writer():
            for i in range(50):
                log.append(key=None, value=f"msg-{i}".encode())
                time.sleep(0.001)
        
        def reader():
            offset = 0
            while offset < 50:
                for msg in log.read(start_offset=offset, max_messages=10):
                    read_messages.append(msg)
                    offset = msg.offset + 1
                time.sleep(0.002)
        
        write_thread = threading.Thread(target=writer)
        read_thread = threading.Thread(target=reader)
        
        write_thread.start()
        read_thread.start()
        
        write_thread.join()
        read_thread.join()
        
        assert len(read_messages) <= 50
        
        log.close()
    
    def test_rotation_during_write(self, temp_dir):
        """Test segment rotation during concurrent writes."""
        log = Log(
            directory=temp_dir,
            max_segment_size=100,
        )
        
        def writer(thread_id):
            for i in range(10):
                log.append(key=None, value=b"x" * 50)
        
        threads = []
        for i in range(3):
            t = threading.Thread(target=writer, args=(i,))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
        
        assert log.segment_count() > 1
        
        log.close()
    
    def test_read_during_deletion(self, temp_dir):
        """Test reading while segments are being deleted."""
        log = Log(
            directory=temp_dir,
            max_segment_size=100,
            retention_bytes=300,
        )
        
        for i in range(20):
            log.append(key=None, value=b"x" * 50)
        
        def reader():
            messages = list(log.read(start_offset=0, max_messages=100))
            return len(messages)
        
        def deleter():
            time.sleep(0.01)
            log._apply_retention()
        
        read_thread = threading.Thread(target=reader)
        delete_thread = threading.Thread(target=deleter)
        
        read_thread.start()
        delete_thread.start()
        
        read_thread.join()
        delete_thread.join()
        
        log.close()
