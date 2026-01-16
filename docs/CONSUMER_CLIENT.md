# Consumer Client Documentation

## Overview

The Consumer client provides a high-level API for reading messages from DistributedLog with offset management, automatic commits, and seek operations.

## Quick Start

```python
from distributedlog.consumer import Consumer

consumer = Consumer(
    bootstrap_servers=['localhost:9092'],
    group_id='my-group',
    auto_commit=True,
)

consumer.subscribe(['my-topic'])

while True:
    messages = consumer.poll(timeout_ms=1000)
    for message in messages:
        print(f"Offset: {message.offset}, Value: {message.value}")
    
    # Process messages

consumer.close()
```

## Features

### 1. Subscribe to Topics

```python
consumer = Consumer(
    bootstrap_servers='localhost:9092',
    group_id='my-group',
)

consumer.subscribe(['topic1', 'topic2'])
```

**How it works:**
- Subscribe to one or more topics
- Consumer automatically discovers partitions
- Partitions are assigned to consumer

### 2. Poll for Messages

```python
messages = consumer.poll(timeout_ms=1000)

for message in messages:
    print(f"Topic: {message.topic}")
    print(f"Partition: {message.partition}")
    print(f"Offset: {message.offset}")
    print(f"Key: {message.key}")
    print(f"Value: {message.value}")
    print(f"Timestamp: {message.timestamp}")
```

**Polling behavior:**
- Blocks for up to `timeout_ms`
- Returns available messages
- Returns empty list if no messages

### 3. Offset Management

**Auto-commit (default):**
```python
consumer = Consumer(
    bootstrap_servers='localhost:9092',
    group_id='my-group',
    auto_commit=True,
    auto_commit_interval_ms=5000,  # Commit every 5s
)

consumer.subscribe(['my-topic'])

while True:
    messages = consumer.poll(timeout_ms=1000)
    for message in messages:
        process(message)
    # Offsets committed automatically
```

**Manual commit:**
```python
consumer = Consumer(
    bootstrap_servers='localhost:9092',
    group_id='my-group',
    auto_commit=False,
)

consumer.subscribe(['my-topic'])

while True:
    messages = consumer.poll(timeout_ms=1000)
    for message in messages:
        process(message)
    
    # Commit after processing
    consumer.commit()
```

**Commit specific offsets:**
```python
from distributedlog.consumer import TopicPartition, OffsetAndMetadata

offsets = {
    TopicPartition('my-topic', 0): OffsetAndMetadata(offset=100),
    TopicPartition('my-topic', 1): OffsetAndMetadata(offset=200),
}

consumer.commit(offsets)
```

### 4. Seek Operations

**Seek to specific offset:**
```python
from distributedlog.consumer import TopicPartition

tp = TopicPartition('my-topic', 0)
consumer.seek(tp, 100)

messages = consumer.poll(timeout_ms=1000)
# Returns messages starting from offset 100
```

**Seek to beginning:**
```python
consumer.seek_to_beginning()

messages = consumer.poll(timeout_ms=1000)
# Returns messages from offset 0
```

**Seek to end:**
```python
consumer.seek_to_end()

messages = consumer.poll(timeout_ms=1000)
# Returns only new messages
```

### 5. Position and Committed Queries

**Get current position:**
```python
tp = TopicPartition('my-topic', 0)
position = consumer.position(tp)
print(f"Current position: {position}")
```

**Get committed offset:**
```python
tp = TopicPartition('my-topic', 0)
committed = consumer.committed(tp)
if committed:
    print(f"Committed offset: {committed.offset}")
```

### 6. Assignment and Subscription

**Get current assignment:**
```python
assignment = consumer.assignment()
for tp in assignment:
    print(f"Assigned: {tp.topic} partition {tp.partition}")
```

**Get current subscription:**
```python
subscription = consumer.subscription()
print(f"Subscribed to: {subscription}")
```

## Configuration Reference

```python
from distributedlog.consumer import Consumer, ConsumerConfig

config = ConsumerConfig(
    bootstrap_servers='localhost:9092',
    
    # Consumer Group
    group_id='my-group',
    
    # Offset Management
    auto_commit=True,
    auto_commit_interval_ms=5000,  # 5 seconds
    auto_offset_reset='latest',     # 'earliest', 'latest', 'none'
    
    # Fetch Configuration
    fetch_min_bytes=1,
    fetch_max_wait_ms=500,
    max_poll_records=500,
)

consumer = Consumer(config=config)
```

## Best Practices

### 1. Always Close Consumer

```python
# Using context manager (recommended)
with Consumer(
    bootstrap_servers='localhost:9092',
    group_id='my-group',
) as consumer:
    consumer.subscribe(['my-topic'])
    messages = consumer.poll(timeout_ms=1000)
    process(messages)
# Automatically closed

# Manual close
consumer = Consumer(
    bootstrap_servers='localhost:9092',
    group_id='my-group',
)
try:
    consumer.subscribe(['my-topic'])
    # ... process messages ...
finally:
    consumer.close()  # Commits offsets
```

### 2. Handle Slow Processing

```python
consumer = Consumer(
    bootstrap_servers='localhost:9092',
    group_id='my-group',
    auto_commit=False,
    max_poll_records=10,  # Process smaller batches
)

while True:
    messages = consumer.poll(timeout_ms=1000)
    
    for message in messages:
        process_slowly(message)
    
    consumer.commit()  # Commit after batch
```

### 3. Error Handling

```python
consumer = Consumer(
    bootstrap_servers='localhost:9092',
    group_id='my-group',
    auto_commit=False,
)

while True:
    messages = consumer.poll(timeout_ms=1000)
    
    for message in messages:
        try:
            process(message)
        except Exception as e:
            logger.error("Processing failed", error=e)
            # Don't commit this offset
            continue
    
    consumer.commit()  # Only commit successfully processed
```

### 4. At-Least-Once Semantics

```python
# Commit AFTER processing (at-least-once)
messages = consumer.poll(timeout_ms=1000)
for message in messages:
    process(message)  # Process first
consumer.commit()     # Then commit

# Commit BEFORE processing (at-most-once, not recommended)
messages = consumer.poll(timeout_ms=1000)
consumer.commit()     # Commit first
for message in messages:
    process(message)  # Then process (may fail)
```

### 5. Seek for Reprocessing

```python
# Reprocess from specific point
consumer.seek(TopicPartition('my-topic', 0), 100)

# Reprocess everything
consumer.seek_to_beginning()

# Skip to latest
consumer.seek_to_end()
```

## Commit Strategies

### Strategy 1: Auto-Commit

```python
consumer = Consumer(
    bootstrap_servers='localhost:9092',
    group_id='my-group',
    auto_commit=True,
    auto_commit_interval_ms=5000,
)
```

**Pros:**
- Simple, no manual commits
- Automatic offset management

**Cons:**
- May commit before processing
- Can cause duplicates on failure

### Strategy 2: Commit After Each Message

```python
consumer = Consumer(
    bootstrap_servers='localhost:9092',
    group_id='my-group',
    auto_commit=False,
)

while True:
    messages = consumer.poll(timeout_ms=1000)
    for message in messages:
        process(message)
        consumer.commit()  # Commit each message
```

**Pros:**
- Precise control
- Minimal reprocessing on failure

**Cons:**
- High commit overhead
- Slow throughput

### Strategy 3: Batch Commit

```python
consumer = Consumer(
    bootstrap_servers='localhost:9092',
    group_id='my-group',
    auto_commit=False,
)

while True:
    messages = consumer.poll(timeout_ms=1000)
    for message in messages:
        process(message)
    consumer.commit()  # Commit batch
```

**Pros:**
- Good throughput
- Balance between safety and performance

**Cons:**
- May reprocess batch on failure

**Recommended:** Use batch commit for most applications.

## Performance Tuning

### High Throughput

```python
consumer = Consumer(
    bootstrap_servers='localhost:9092',
    group_id='my-group',
    max_poll_records=5000,         # Larger batches
    fetch_min_bytes=65536,         # 64KB min fetch
    auto_commit_interval_ms=10000, # Less frequent commits
)
```

### Low Latency

```python
consumer = Consumer(
    bootstrap_servers='localhost:9092',
    group_id='my-group',
    max_poll_records=100,          # Small batches
    fetch_min_bytes=1,             # Don't wait
    fetch_max_wait_ms=100,         # Quick timeout
)
```

## Troubleshooting

### Problem: Consumer Lag

**Symptoms:**
- Messages pile up
- Consumer can't keep up

**Solutions:**
1. Increase `max_poll_records`
2. Optimize message processing
3. Add more consumers to group
4. Increase parallelism in processing

### Problem: Duplicate Processing

**Symptoms:**
- Messages processed multiple times

**Causes:**
- Auto-commit before processing
- Consumer crash before commit

**Solutions:**
1. Use manual commit after processing
2. Implement idempotent processing
3. Store offsets with processed results

### Problem: Slow Fetches

**Symptoms:**
- Long poll times
- Low throughput

**Solutions:**
1. Reduce `fetch_min_bytes`
2. Reduce `fetch_max_wait_ms`
3. Check network latency
4. Verify broker performance

## Examples

### Basic Consumer

```python
from distributedlog.consumer import Consumer

with Consumer(
    bootstrap_servers='localhost:9092',
    group_id='my-group',
) as consumer:
    consumer.subscribe(['my-topic'])
    
    while True:
        messages = consumer.poll(timeout_ms=1000)
        for message in messages:
            print(f"Value: {message.value}")
```

### Manual Commit Consumer

```python
from distributedlog.consumer import Consumer

consumer = Consumer(
    bootstrap_servers='localhost:9092',
    group_id='my-group',
    auto_commit=False,
)

consumer.subscribe(['my-topic'])

try:
    while True:
        messages = consumer.poll(timeout_ms=1000)
        
        for message in messages:
            process(message)
        
        consumer.commit()

finally:
    consumer.close()
```

### Seek and Reprocess

```python
from distributedlog.consumer import Consumer, TopicPartition

consumer = Consumer(
    bootstrap_servers='localhost:9092',
    group_id='my-group',
    auto_commit=False,
)

consumer.subscribe(['my-topic'])

# Reprocess from specific offset
tp = TopicPartition('my-topic', 0)
consumer.seek(tp, 100)

messages = consumer.poll(timeout_ms=1000)
print(f"Reprocessing from offset 100: {len(messages)} messages")

consumer.close()
```

## Comparison with Kafka

| Feature | DistributedLog Consumer | Kafka Consumer |
|---------|------------------------|----------------|
| Subscribe | Yes | Yes |
| Poll API | Yes | Yes |
| Auto-commit | Yes | Yes |
| Manual commit | Yes | Yes |
| Seek operations | Yes | Yes |
| Consumer groups | Partial | Yes |
| Rebalancing | Not yet | Yes |
| Pause/resume | Not yet | Yes |

## References

- Kafka Consumer API
- "Designing Data-Intensive Applications" - Martin Kleppmann
- Consumer group coordination
