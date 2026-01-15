"""
Performance benchmarks for log operations.

Run with: python -m distributedlog.tests.benchmarks.benchmark_log
"""

import tempfile
import time
from pathlib import Path

from distributedlog.core.log.log import Log


def benchmark_sequential_writes(num_messages: int = 10000) -> dict:
    """
    Benchmark sequential write performance.
    
    Args:
        num_messages: Number of messages to write
    
    Returns:
        Performance metrics
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        log = Log(directory=Path(tmpdir))
        
        message_value = b"x" * 1000
        
        start_time = time.time()
        
        for i in range(num_messages):
            log.append(key=None, value=message_value)
        
        log.flush()
        
        end_time = time.time()
        
        duration = end_time - start_time
        throughput = num_messages / duration
        total_bytes = num_messages * len(message_value)
        bandwidth_mbps = (total_bytes / duration) / (1024 * 1024)
        
        log.close()
        
        return {
            "test": "sequential_writes",
            "messages": num_messages,
            "duration_sec": duration,
            "throughput_msg_sec": throughput,
            "bandwidth_mbps": bandwidth_mbps,
        }


def benchmark_sequential_reads(num_messages: int = 10000) -> dict:
    """
    Benchmark sequential read performance.
    
    Args:
        num_messages: Number of messages to read
    
    Returns:
        Performance metrics
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        log = Log(directory=Path(tmpdir))
        
        message_value = b"x" * 1000
        
        for i in range(num_messages):
            log.append(key=None, value=message_value)
        
        log.flush()
        
        start_time = time.time()
        
        messages_read = 0
        for msg in log.read(start_offset=0, max_messages=num_messages):
            messages_read += 1
        
        end_time = time.time()
        
        duration = end_time - start_time
        throughput = messages_read / duration
        total_bytes = messages_read * len(message_value)
        bandwidth_mbps = (total_bytes / duration) / (1024 * 1024)
        
        log.close()
        
        return {
            "test": "sequential_reads",
            "messages": messages_read,
            "duration_sec": duration,
            "throughput_msg_sec": throughput,
            "bandwidth_mbps": bandwidth_mbps,
        }


def benchmark_indexed_reads(num_messages: int = 10000) -> dict:
    """
    Benchmark random read performance with index.
    
    Args:
        num_messages: Number of messages in log
    
    Returns:
        Performance metrics
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        log = Log(directory=Path(tmpdir))
        
        message_value = b"x" * 1000
        
        for i in range(num_messages):
            log.append(key=None, value=message_value)
        
        log.flush()
        
        import random
        offsets = random.sample(range(num_messages), min(100, num_messages))
        
        start_time = time.time()
        
        for offset in offsets:
            messages = list(log.read(start_offset=offset, max_messages=1))
            assert len(messages) == 1
        
        end_time = time.time()
        
        duration = end_time - start_time
        avg_latency_ms = (duration / len(offsets)) * 1000
        
        log.close()
        
        return {
            "test": "indexed_reads",
            "random_reads": len(offsets),
            "duration_sec": duration,
            "avg_latency_ms": avg_latency_ms,
        }


def benchmark_compaction(num_messages: int = 1000) -> dict:
    """
    Benchmark compaction performance.
    
    Args:
        num_messages: Number of messages to compact
    
    Returns:
        Performance metrics
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        log = Log(directory=Path(tmpdir), enable_compaction=True)
        
        message_value = b"x" * 1000
        
        for i in range(num_messages):
            log.append(key=b"key-1", value=message_value)
            log.append(key=b"key-2", value=message_value)
        
        log.flush()
        
        original_size = sum(seg.size() for seg in log._segments)
        
        start_time = time.time()
        
        compacted_count = log.compact(min_cleanable_ratio=0.5)
        
        end_time = time.time()
        
        compacted_size = sum(seg.size() for seg in log._segments)
        
        duration = end_time - start_time
        space_saved = original_size - compacted_size
        space_saved_percent = (space_saved / original_size) * 100
        
        log.close()
        
        return {
            "test": "compaction",
            "messages": num_messages * 2,
            "duration_sec": duration,
            "segments_compacted": compacted_count,
            "original_size_mb": original_size / (1024 * 1024),
            "compacted_size_mb": compacted_size / (1024 * 1024),
            "space_saved_percent": space_saved_percent,
        }


def benchmark_retention(num_messages: int = 1000) -> dict:
    """
    Benchmark retention policy performance.
    
    Args:
        num_messages: Number of messages to write
    
    Returns:
        Performance metrics
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        log = Log(
            directory=Path(tmpdir),
            max_segment_size=10000,
            retention_bytes=50000,
        )
        
        message_value = b"x" * 1000
        
        for i in range(num_messages):
            log.append(key=None, value=message_value)
        
        log.flush()
        
        segments_before = log.segment_count()
        
        start_time = time.time()
        
        deleted = log._apply_retention()
        
        end_time = time.time()
        
        segments_after = log.segment_count()
        
        duration = end_time - start_time
        
        log.close()
        
        return {
            "test": "retention",
            "segments_before": segments_before,
            "segments_deleted": deleted,
            "segments_after": segments_after,
            "duration_sec": duration,
        }


def run_all_benchmarks():
    """Run all benchmarks and print results."""
    print("=== DistributedLog Performance Benchmarks ===\n")
    
    benchmarks = [
        (benchmark_sequential_writes, 10000),
        (benchmark_sequential_reads, 10000),
        (benchmark_indexed_reads, 10000),
        (benchmark_compaction, 1000),
        (benchmark_retention, 1000),
    ]
    
    results = []
    
    for benchmark_func, num_messages in benchmarks:
        print(f"Running {benchmark_func.__name__}...")
        result = benchmark_func(num_messages)
        results.append(result)
        
        print(f"  Test: {result['test']}")
        for key, value in result.items():
            if key != 'test':
                if isinstance(value, float):
                    print(f"    {key}: {value:.2f}")
                else:
                    print(f"    {key}: {value}")
        print()
    
    return results


if __name__ == "__main__":
    run_all_benchmarks()
