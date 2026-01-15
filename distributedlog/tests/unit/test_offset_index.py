"""Tests for offset index."""

import struct
import tempfile
from pathlib import Path

import pytest

from distributedlog.core.index.offset_index import IndexEntry, OffsetIndex


class TestIndexEntry:
    """Test IndexEntry structure."""
    
    def test_create_entry(self):
        """Test creating an index entry."""
        entry = IndexEntry(relative_offset=100, position=4096)
        
        assert entry.relative_offset == 100
        assert entry.position == 4096
    
    def test_serialize_entry(self):
        """Test serializing an entry."""
        entry = IndexEntry(relative_offset=100, position=4096)
        data = entry.serialize()
        
        assert len(data) == 8
        assert isinstance(data, bytes)
    
    def test_deserialize_entry(self):
        """Test deserializing an entry."""
        entry = IndexEntry(relative_offset=100, position=4096)
        data = entry.serialize()
        
        recovered = IndexEntry.deserialize(data)
        
        assert recovered.relative_offset == 100
        assert recovered.position == 4096
    
    def test_deserialize_invalid_size(self):
        """Test that invalid size raises error."""
        with pytest.raises(ValueError, match="Expected 8 bytes"):
            IndexEntry.deserialize(b"short")
    
    def test_negative_offset_raises_error(self):
        """Test that negative offset raises error."""
        with pytest.raises(ValueError, match="Relative offset must be non-negative"):
            IndexEntry(relative_offset=-1, position=0)
    
    def test_negative_position_raises_error(self):
        """Test that negative position raises error."""
        with pytest.raises(ValueError, match="Position must be non-negative"):
            IndexEntry(relative_offset=0, position=-1)


class TestOffsetIndex:
    """Test OffsetIndex class."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for tests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    def test_create_index(self, temp_dir):
        """Test creating a new index."""
        index = OffsetIndex(
            base_offset=0,
            directory=temp_dir,
        )
        
        assert index.base_offset == 0
        assert index.entries_count() == 0
        assert index.path.exists()
        
        index.close()
    
    def test_index_file_naming(self, temp_dir):
        """Test that index files are named correctly."""
        index = OffsetIndex(
            base_offset=12345,
            directory=temp_dir,
        )
        
        expected_name = "00000000000000012345.index"
        assert index.path.name == expected_name
        
        index.close()
    
    def test_append_single_entry(self, temp_dir):
        """Test appending a single entry."""
        index = OffsetIndex(
            base_offset=0,
            directory=temp_dir,
            interval_bytes=0,
        )
        
        assert index.append(offset=0, position=0)
        assert index.entries_count() == 1
        
        index.close()
    
    def test_append_multiple_entries(self, temp_dir):
        """Test appending multiple entries."""
        index = OffsetIndex(
            base_offset=100,
            directory=temp_dir,
            interval_bytes=0,
        )
        
        for i in range(10):
            assert index.append(offset=100 + i, position=i * 1000)
        
        assert index.entries_count() == 10
        
        index.close()
    
    def test_append_respects_interval(self, temp_dir):
        """Test that append respects interval_bytes."""
        index = OffsetIndex(
            base_offset=0,
            directory=temp_dir,
            interval_bytes=4096,
        )
        
        assert index.append(offset=0, position=0)
        assert not index.append(offset=1, position=100)
        assert index.append(offset=2, position=5000)
        
        assert index.entries_count() == 2
        
        index.close()
    
    def test_lookup_exact_match(self, temp_dir):
        """Test looking up an exact match."""
        index = OffsetIndex(
            base_offset=0,
            directory=temp_dir,
            interval_bytes=0,
        )
        
        index.append(offset=0, position=0)
        index.append(offset=10, position=1000)
        index.append(offset=20, position=2000)
        
        result = index.lookup(offset=10)
        
        assert result is not None
        assert result[0] == 10
        assert result[1] == 1000
        
        index.close()
    
    def test_lookup_between_entries(self, temp_dir):
        """Test looking up offset between entries."""
        index = OffsetIndex(
            base_offset=0,
            directory=temp_dir,
            interval_bytes=0,
        )
        
        index.append(offset=0, position=0)
        index.append(offset=10, position=1000)
        index.append(offset=20, position=2000)
        
        result = index.lookup(offset=15)
        
        assert result is not None
        assert result[0] == 10
        assert result[1] == 1000
        
        index.close()
    
    def test_lookup_before_first_entry(self, temp_dir):
        """Test looking up before first entry."""
        index = OffsetIndex(
            base_offset=100,
            directory=temp_dir,
            interval_bytes=0,
        )
        
        index.append(offset=100, position=0)
        
        result = index.lookup(offset=50)
        
        assert result is None
        
        index.close()
    
    def test_lookup_empty_index(self, temp_dir):
        """Test looking up in empty index."""
        index = OffsetIndex(
            base_offset=0,
            directory=temp_dir,
        )
        
        result = index.lookup(offset=0)
        
        assert result is None
        
        index.close()
    
    def test_binary_search_correctness(self, temp_dir):
        """Test that binary search finds correct entries."""
        index = OffsetIndex(
            base_offset=0,
            directory=temp_dir,
            interval_bytes=0,
        )
        
        for i in range(100):
            index.append(offset=i * 10, position=i * 1000)
        
        for i in range(100):
            target = i * 10 + 5
            result = index.lookup(offset=target)
            
            assert result is not None
            assert result[0] == i * 10
            assert result[1] == i * 1000
        
        index.close()
    
    def test_flush_persists_data(self, temp_dir):
        """Test that flush persists data."""
        index = OffsetIndex(
            base_offset=0,
            directory=temp_dir,
            interval_bytes=0,
        )
        
        index.append(offset=0, position=0)
        index.flush()
        index.close()
        
        index2 = OffsetIndex(
            base_offset=0,
            directory=temp_dir,
        )
        
        assert index2.entries_count() == 1
        
        index2.close()
    
    def test_reopen_index(self, temp_dir):
        """Test reopening an existing index."""
        index1 = OffsetIndex(
            base_offset=0,
            directory=temp_dir,
            interval_bytes=0,
        )
        
        for i in range(5):
            index1.append(offset=i, position=i * 1000)
        
        index1.close()
        
        index2 = OffsetIndex(
            base_offset=0,
            directory=temp_dir,
        )
        
        assert index2.entries_count() == 5
        
        result = index2.lookup(offset=3)
        assert result is not None
        assert result[0] == 3
        assert result[1] == 3000
        
        index2.close()
    
    def test_truncate_index(self, temp_dir):
        """Test truncating index to fewer entries."""
        index = OffsetIndex(
            base_offset=0,
            directory=temp_dir,
            interval_bytes=0,
        )
        
        for i in range(10):
            index.append(offset=i, position=i * 1000)
        
        index.truncate(5)
        
        assert index.entries_count() == 5
        
        result = index.lookup(offset=8)
        assert result is not None
        assert result[0] == 4
        
        index.close()
    
    def test_context_manager(self, temp_dir):
        """Test using index as context manager."""
        with OffsetIndex(
            base_offset=0,
            directory=temp_dir,
            interval_bytes=0,
        ) as index:
            index.append(offset=0, position=0)
        
        assert index._mmap is None
        assert index._fd is None
    
    def test_max_entries_limit(self, temp_dir):
        """Test that index respects max_entries limit."""
        index = OffsetIndex(
            base_offset=0,
            directory=temp_dir,
            max_entries=5,
            interval_bytes=0,
        )
        
        for i in range(10):
            result = index.append(offset=i, position=i * 1000)
            if i < 5:
                assert result
            else:
                assert not result
        
        assert index.entries_count() == 5
        
        index.close()
