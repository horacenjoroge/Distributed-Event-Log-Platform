"""
Message format structures for log segments.

This module defines the binary format for messages stored in log segments,
including serialization and deserialization.
"""

import struct
from dataclasses import dataclass
from enum import IntEnum
from typing import Optional

import crc32c


class MagicByte(IntEnum):
    """Message format version."""
    
    V0 = 0
    V1 = 1
    CURRENT = V1


class CompressionType(IntEnum):
    """Compression types for message attributes."""
    
    NONE = 0
    GZIP = 1
    SNAPPY = 2
    LZ4 = 3
    ZSTD = 4


@dataclass
class Message:
    """
    A single message in the log.
    
    Attributes:
        offset: Logical offset in the log
        timestamp: Unix timestamp in milliseconds
        key: Optional message key
        value: Message payload
        attributes: Compression and other flags
    """
    
    offset: int
    timestamp: int
    key: Optional[bytes]
    value: bytes
    attributes: int = 0
    
    def __post_init__(self) -> None:
        """Validate message fields."""
        if self.offset < 0:
            raise ValueError(f"Offset must be non-negative, got {self.offset}")
        if self.timestamp < 0:
            raise ValueError(f"Timestamp must be non-negative, got {self.timestamp}")
        if self.key is not None and not isinstance(self.key, bytes):
            raise TypeError(f"Key must be bytes or None, got {type(self.key)}")
        if not isinstance(self.value, bytes):
            raise TypeError(f"Value must be bytes, got {type(self.value)}")


@dataclass
class MessageSet:
    """
    A message set contains one or more messages with metadata.
    
    Wire format:
        Length (4 bytes) - Total length excluding this field
        CRC32 (4 bytes) - Checksum of remaining data
        Magic byte (1 byte) - Format version
        Attributes (1 byte) - Compression and flags
        Timestamp (8 bytes) - Message timestamp
        Key length (4 bytes) - Length of key (-1 if null)
        Key (variable) - Message key
        Value length (4 bytes) - Length of value
        Value (variable) - Message payload
    """
    
    message: Message
    magic_byte: int = MagicByte.CURRENT
    
    HEADER_SIZE = 4 + 4 + 1 + 1 + 8
    LENGTH_FIELD_SIZE = 4
    
    def serialize(self) -> bytes:
        """
        Serialize message set to bytes.
        
        Returns:
            Serialized message set
        """
        key_bytes = self.message.key if self.message.key is not None else b""
        key_length = len(key_bytes) if self.message.key is not None else -1
        value_length = len(self.message.value)
        
        payload = struct.pack(
            f">BBQi{len(key_bytes)}si{len(self.message.value)}s",
            self.magic_byte,
            self.message.attributes,
            self.message.timestamp,
            key_length,
            key_bytes,
            value_length,
            self.message.value,
        )
        
        crc = crc32c.crc32c(payload)
        
        total_length = 4 + len(payload)
        
        return struct.pack(">II", total_length, crc) + payload
    
    @classmethod
    def deserialize(cls, data: bytes, offset: int) -> "MessageSet":
        """
        Deserialize message set from bytes.
        
        Args:
            data: Serialized message set
            offset: Logical offset for the message
        
        Returns:
            Deserialized MessageSet
        
        Raises:
            ValueError: If data is corrupted or invalid
        """
        if len(data) < cls.LENGTH_FIELD_SIZE + 4:
            raise ValueError(f"Data too short: {len(data)} bytes")
        
        length, crc = struct.unpack(">II", data[:8])
        
        if len(data) < cls.LENGTH_FIELD_SIZE + length:
            raise ValueError(
                f"Incomplete message: expected {cls.LENGTH_FIELD_SIZE + length} bytes, "
                f"got {len(data)} bytes"
            )
        
        payload = data[8 : cls.LENGTH_FIELD_SIZE + length]
        
        computed_crc = crc32c.crc32c(payload)
        if computed_crc != crc:
            raise ValueError(
                f"CRC mismatch: expected {crc}, computed {computed_crc}"
            )
        
        magic_byte, attributes, timestamp = struct.unpack(">BBQ", payload[:10])
        
        if magic_byte not in [MagicByte.V0, MagicByte.V1]:
            raise ValueError(f"Unsupported magic byte: {magic_byte}")
        
        key_length = struct.unpack(">i", payload[10:14])[0]
        
        if key_length == -1:
            key = None
            value_offset = 14
        else:
            if key_length < 0:
                raise ValueError(f"Invalid key length: {key_length}")
            key = payload[14 : 14 + key_length]
            value_offset = 14 + key_length
        
        value_length = struct.unpack(">i", payload[value_offset : value_offset + 4])[0]
        
        if value_length < 0:
            raise ValueError(f"Invalid value length: {value_length}")
        
        value = payload[value_offset + 4 : value_offset + 4 + value_length]
        
        if len(value) != value_length:
            raise ValueError(
                f"Value length mismatch: expected {value_length}, got {len(value)}"
            )
        
        message = Message(
            offset=offset,
            timestamp=timestamp,
            key=key,
            value=value,
            attributes=attributes,
        )
        
        return cls(message=message, magic_byte=magic_byte)
    
    def size(self) -> int:
        """
        Calculate the serialized size of this message set.
        
        Returns:
            Size in bytes
        """
        key_size = len(self.message.key) if self.message.key is not None else 0
        value_size = len(self.message.value)
        return self.LENGTH_FIELD_SIZE + self.HEADER_SIZE + 8 + key_size + value_size
