"""
Core log storage implementation.

This package provides append-only log segments with:
- Binary message format with CRC validation
- Automatic segment rotation
- Sequential read operations
- Crash recovery
"""

from distributedlog.core.log.format import (
    CompressionType,
    MagicByte,
    Message,
    MessageSet,
)
from distributedlog.core.log.log import Log
from distributedlog.core.log.reader import LogSegmentReader
from distributedlog.core.log.segment import LogSegment

__all__ = [
    "CompressionType",
    "MagicByte",
    "Message",
    "MessageSet",
    "Log",
    "LogSegment",
    "LogSegmentReader",
]
