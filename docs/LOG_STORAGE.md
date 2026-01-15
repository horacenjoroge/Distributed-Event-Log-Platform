# Log Storage Implementation

## Overview

The log storage layer provides an append-only commit log built on disk with automatic segment rotation, crash recovery, and efficient sequential reads.

## Architecture

### Components

1. **Message Format** (`format.py`)
   - Binary serialization with CRC32C checksums
   - Versioned wire format (magic byte)
   - Support for nullable keys and compression attributes

2. **Log Segment** (`segment.py`)
   - Single file representing a range of offsets
   - Append-only writes with atomic operations
   - Configurable fsync behavior

3. **Segment Reader** (`reader.py`)
   - Sequential reads from segments
   - Partial write detection
   - Crash recovery support

4. **Log Manager** (`log.py`)
   - Manages multiple segments
   - Automatic rotation based on size/time
   - Cross-segment reads

## File Format

### Message Wire Format

```
[Length (4 bytes)] - Total length excluding this field
[CRC32 (4 bytes)]  - Checksum of remaining data
[Magic (1 byte)]   - Format version
[Attrs (1 byte)]   - Compression and flags
[Time (8 bytes)]   - Unix timestamp in milliseconds
[KeyLen (4 bytes)] - Length of key (-1 if null)
[Key (variable)]   - Message key
[ValLen (4 bytes)] - Length of value
[Value (variable)] - Message payload
```

### Segment Naming

Segments are named by their base offset, zero-padded to 20 digits:

```
00000000000000000000.log  # Base offset: 0
00000000000000001000.log  # Base offset: 1000
00000000000000012345.log  # Base offset: 12345
```

## Usage

### Basic Append and Read

```python
from pathlib import Path
from distributedlog.core.log import Log

# Create a log
log = Log(
    directory=Path("/var/lib/distributedlog/data"),
    max_segment_size=1073741824,  # 1GB
    fsync_on_append=False,
)

# Append messages
offset1 = log.append(key=b"user-123", value=b"event-data")
offset2 = log.append(key=None, value=b"another-event")

# Read messages
for message in log.read(start_offset=0, max_messages=10):
    print(f"Offset: {message.offset}")
    print(f"Key: {message.key}")
    print(f"Value: {message.value}")

# Close when done
log.close()
```

### Context Manager

```python
with Log(directory=Path("/data")) as log:
    log.append(key=b"key", value=b"value")
    # Automatically flushed and closed
```

### Configurable Fsync

```python
# Fsync after every append (slower, more durable)
log = Log(directory=Path("/data"), fsync_on_append=True)

# Or fsync manually
log.append(key=None, value=b"data")
log.flush()  # Force write to disk
```

## Durability Guarantees

### Write Durability

- **fsync_on_append=False**: Data buffered in OS page cache (default)
  - Fast writes
  - Risk of data loss on system crash
  - Suitable for high-throughput scenarios

- **fsync_on_append=True**: Data synced to disk immediately
  - Slower writes
  - Guaranteed durability
  - Suitable for critical data

### Partial Write Detection

The reader detects partial writes at segment end:

```python
reader = LogSegmentReader(path, base_offset=0)
messages, bytes_read = reader.recover_valid_messages()
# Returns all valid messages before corruption
```

## Segment Rotation

Segments rotate automatically based on:

1. **Size threshold**: When segment reaches max_segment_size
2. **Time threshold**: When segment age exceeds max_segment_age_ms

```python
log = Log(
    directory=Path("/data"),
    max_segment_size=1073741824,    # 1GB
    max_segment_age_ms=604800000,   # 7 days
)
```

## Crash Recovery

On startup, the log recovers existing segments:

1. Scans directory for `.log` files
2. Reads each segment sequentially
3. Validates CRC checksums
4. Stops at first corruption
5. Truncates corrupted tail
6. Resumes from last valid offset

Example:

```python
# After crash, reopening log recovers valid data
log = Log(directory=Path("/data"))
latest_offset = log.get_latest_offset()
print(f"Recovered up to offset: {latest_offset}")
```

## Performance Considerations

### Page Cache vs Direct I/O

Current implementation uses page cache (buffered I/O):

**Advantages:**
- Fast writes (batched by OS)
- Read caching for sequential access
- OS handles write scheduling

**Trade-offs:**
- Requires fsync for durability
- Memory pressure from cache
- Potential double-buffering

### Zero-Copy Reads (Future)

Plan to implement using `sendfile()` for zero-copy data transfer:

```python
# Future implementation
def zero_copy_read(self, offset, length, socket_fd):
    os.sendfile(socket_fd, self._fd, offset, length)
```

## Error Handling

### Corruption Detection

CRC mismatches trigger ValueError:

```python
try:
    message_set = MessageSet.deserialize(data, offset=0)
except ValueError as e:
    # CRC mismatch or invalid format
    logger.error(f"Corruption detected: {e}")
```

### Full Segment Handling

Attempting to append to full segment raises ValueError:

```python
try:
    segment.append(message)
except ValueError:
    # Rotate to new segment
    new_segment = create_next_segment()
```

## Testing

Run tests with:

```bash
pytest distributedlog/tests/unit/test_format.py
pytest distributedlog/tests/unit/test_segment.py
pytest distributedlog/tests/unit/test_log.py
```

Coverage includes:
- Serialization/deserialization
- CRC validation
- Segment rotation
- Crash recovery
- Edge cases

## Future Enhancements

1. **Log Compaction**: Remove old data while preserving latest values per key
2. **Index Files**: Offset-to-position mapping for faster random access
3. **Compression**: GZIP, Snappy, LZ4, ZSTD support
4. **Memory Mapping**: mmap() for faster reads
5. **Batch Writes**: Group multiple appends for efficiency
