"""
Offset indexing for fast log lookups.

This package provides sparse indexing with memory-mapped files for O(log n)
offset lookups instead of O(n) sequential scans.
"""

from distributedlog.core.index.offset_index import IndexEntry, OffsetIndex
from distributedlog.core.index.recovery import IndexRecovery

__all__ = ["IndexEntry", "OffsetIndex", "IndexRecovery"]
