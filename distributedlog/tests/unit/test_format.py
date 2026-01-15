"""Tests for message format serialization and deserialization."""

import pytest

from distributedlog.core.log.format import (
    CompressionType,
    MagicByte,
    Message,
    MessageSet,
)


class TestMessage:
    """Test Message dataclass."""
    
    def test_create_message_with_key(self):
        """Test creating a message with a key."""
        msg = Message(
            offset=100,
            timestamp=1234567890,
            key=b"test-key",
            value=b"test-value",
        )
        
        assert msg.offset == 100
        assert msg.timestamp == 1234567890
        assert msg.key == b"test-key"
        assert msg.value == b"test-value"
        assert msg.attributes == 0
    
    def test_create_message_without_key(self):
        """Test creating a message without a key."""
        msg = Message(
            offset=200,
            timestamp=9876543210,
            key=None,
            value=b"value-only",
        )
        
        assert msg.offset == 200
        assert msg.key is None
        assert msg.value == b"value-only"
    
    def test_negative_offset_raises_error(self):
        """Test that negative offset raises ValueError."""
        with pytest.raises(ValueError, match="Offset must be non-negative"):
            Message(
                offset=-1,
                timestamp=1234567890,
                key=None,
                value=b"test",
            )
    
    def test_negative_timestamp_raises_error(self):
        """Test that negative timestamp raises ValueError."""
        with pytest.raises(ValueError, match="Timestamp must be non-negative"):
            Message(
                offset=0,
                timestamp=-1,
                key=None,
                value=b"test",
            )


class TestMessageSet:
    """Test MessageSet serialization and deserialization."""
    
    def test_serialize_message_with_key(self):
        """Test serializing a message with a key."""
        msg = Message(
            offset=0,
            timestamp=1234567890,
            key=b"key",
            value=b"value",
            attributes=CompressionType.NONE,
        )
        
        msg_set = MessageSet(message=msg)
        data = msg_set.serialize()
        
        assert isinstance(data, bytes)
        assert len(data) > 0
    
    def test_serialize_message_without_key(self):
        """Test serializing a message without a key."""
        msg = Message(
            offset=0,
            timestamp=1234567890,
            key=None,
            value=b"value",
        )
        
        msg_set = MessageSet(message=msg)
        data = msg_set.serialize()
        
        assert isinstance(data, bytes)
        assert len(data) > 0
    
    def test_deserialize_message_with_key(self):
        """Test deserializing a message with a key."""
        original_msg = Message(
            offset=42,
            timestamp=1234567890,
            key=b"test-key",
            value=b"test-value",
            attributes=CompressionType.GZIP,
        )
        
        msg_set = MessageSet(message=original_msg)
        data = msg_set.serialize()
        
        recovered_set = MessageSet.deserialize(data, offset=42)
        recovered_msg = recovered_set.message
        
        assert recovered_msg.offset == 42
        assert recovered_msg.timestamp == 1234567890
        assert recovered_msg.key == b"test-key"
        assert recovered_msg.value == b"test-value"
        assert recovered_msg.attributes == CompressionType.GZIP
    
    def test_deserialize_message_without_key(self):
        """Test deserializing a message without a key."""
        original_msg = Message(
            offset=100,
            timestamp=9876543210,
            key=None,
            value=b"no-key-value",
        )
        
        msg_set = MessageSet(message=original_msg)
        data = msg_set.serialize()
        
        recovered_set = MessageSet.deserialize(data, offset=100)
        recovered_msg = recovered_set.message
        
        assert recovered_msg.offset == 100
        assert recovered_msg.key is None
        assert recovered_msg.value == b"no-key-value"
    
    def test_deserialize_corrupted_crc(self):
        """Test that corrupted CRC is detected."""
        msg = Message(
            offset=0,
            timestamp=1234567890,
            key=None,
            value=b"test",
        )
        
        msg_set = MessageSet(message=msg)
        data = bytearray(msg_set.serialize())
        
        data[4] ^= 0xFF
        
        with pytest.raises(ValueError, match="CRC mismatch"):
            MessageSet.deserialize(bytes(data), offset=0)
    
    def test_deserialize_too_short_data(self):
        """Test that too short data raises ValueError."""
        with pytest.raises(ValueError, match="Data too short"):
            MessageSet.deserialize(b"short", offset=0)
    
    def test_deserialize_incomplete_message(self):
        """Test that incomplete message raises ValueError."""
        msg = Message(
            offset=0,
            timestamp=1234567890,
            key=None,
            value=b"test",
        )
        
        msg_set = MessageSet(message=msg)
        data = msg_set.serialize()
        
        truncated = data[:-5]
        
        with pytest.raises(ValueError, match="Incomplete message"):
            MessageSet.deserialize(truncated, offset=0)
    
    def test_message_set_size(self):
        """Test calculating message set size."""
        msg = Message(
            offset=0,
            timestamp=1234567890,
            key=b"key",
            value=b"value",
        )
        
        msg_set = MessageSet(message=msg)
        calculated_size = msg_set.size()
        actual_size = len(msg_set.serialize())
        
        assert calculated_size == actual_size
    
    def test_large_message(self):
        """Test serializing and deserializing a large message."""
        large_value = b"x" * 1024 * 1024
        
        msg = Message(
            offset=0,
            timestamp=1234567890,
            key=None,
            value=large_value,
        )
        
        msg_set = MessageSet(message=msg)
        data = msg_set.serialize()
        
        recovered_set = MessageSet.deserialize(data, offset=0)
        
        assert recovered_set.message.value == large_value
