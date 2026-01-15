## Producer Client Documentation

## Overview

The Producer client provides a high-level API for sending messages to DistributedLog with batching, compression, partitioning, and retry logic.

## Quick Start

```python
from distributedlog.producer import Producer

producer = Producer(bootstrap_servers=['localhost:9092'])

metadata = producer.send('my-topic', key=b'user123', value=b'data').result()

print(f"Sent to partition {metadata.partition} offset {metadata.offset}")

producer.close()
```

## Features

### 1. Synchronous Send (Blocking)

```python
producer = Producer(bootstrap_servers='localhost:9092')

future = producer.send('my-topic', value=b'data')

metadata = future.result(timeout=5.0)

print(f"Offset: {metadata.offset}")
```

**How it works:**
- Returns a `Future` immediately
- Blocks on `.result()` until send completes
- Raises exception if send fails

### 2. Asynchronous Send (Non-blocking)

```python
def on_success(metadata):
    print(f"Sent to partition {metadata.partition}")

def on_error(exception):
    print(f"Failed: {exception}")

producer.send(
    'my-topic',
    value=b'data',
    callback=on_success,
    error_callback=on_error,
)

producer.flush()
```

**How it works:**
- Returns `Future` immediately
- Callback invoked on success/error
- Non-blocking - continues immediately

### 3. Message Batching

Messages are accumulated into batches before sending to reduce network overhead.

```python
producer = Producer(
    bootstrap_servers='localhost:9092',
    batch_size=16384,      # 16KB
    linger_ms=10,          # Wait 10ms
)
```

**Batching triggers:**
1. **Batch full**: When batch reaches `batch_size` bytes
2. **Linger time**: When batch age exceeds `linger_ms`
3. **Flush**: When `producer.flush()` is called

**Trade-offs:**
- **Large batch_size**: Better throughput, higher latency
- **Small batch_size**: Lower latency, more network calls
- **High linger_ms**: Better batching, higher latency
- **Low linger_ms**: Lower latency, smaller batches

### 4. Compression

Compress batches to reduce network bandwidth.

```python
from distributedlog.producer import CompressionType

producer = Producer(
    bootstrap_servers='localhost:9092',
    compression_type=CompressionType.GZIP,
)
```

**Supported algorithms:**
- **NONE**: No compression (default)
- **GZIP**: Best compression ratio (~70%), slowest
- **SNAPPY**: Fast compression (~50%), moderate ratio
- **LZ4**: Fastest compression (~40%), lower ratio

**When to use:**
- **GZIP**: Slow network, CPU to spare
- **SNAPPY**: Balanced performance
- **LZ4**: Low CPU, fast network
- **NONE**: Already compressed data (images, video)

### 5. Partitioning Strategies

Choose partition for each message.

**Default Partitioner (Hybrid):**
```python
producer = Producer(partitioner_type='default')

producer.send('topic', key=b'key1', value=b'data')

producer.send('topic', value=b'data')
```

- **With key**: Hash key to partition (consistent)
- **Without key**: Round-robin across partitions

**Key Hash Partitioner:**
```python
producer = Producer(partitioner_type='key_hash')

producer.send('topic', key=b'user123', value=b'data')
```

- **Always uses key hash**
- **Same key → same partition** (ordering guarantee)
- **Requires key** (fails if None)

**Round-Robin Partitioner:**
```python
producer = Producer(partitioner_type='round_robin')

for i in range(100):
    producer.send('topic', value=f'msg-{i}'.encode())
```

- **Evenly distributes** across partitions
- **Ignores key** (no ordering guarantee)
- **Best for load balancing**

**Sticky Partitioner:**
```python
producer = Producer(partitioner_type='sticky')
```

- **Sticks to partition** until batch full
- **Better batching efficiency** (fewer small batches)
- **Kafka 2.4+ default** for keyless messages

**Custom Partitioner:**
```python
from distributedlog.producer.partitioner import Partitioner

class CustomPartitioner(Partitioner):
    def partition(self, topic, key, value, num_partitions):
        if b'premium' in value:
            return 0  # Premium partition
        return 1

producer = Producer(bootstrap_servers='localhost:9092')
producer._partitioner = CustomPartitioner()
```

### 6. Retry Logic

Automatic retry with exponential backoff for transient failures.

```python
producer = Producer(
    bootstrap_servers='localhost:9092',
    max_retries=3,
    retry_backoff_ms=100,
)
```

**How it works:**
1. **Initial attempt** fails
2. **Wait 100ms** (with jitter)
3. **Retry** - wait 200ms
4. **Retry** - wait 400ms
5. **Retry** - wait 800ms (capped at max)
6. **Give up** after max_retries

**Formula:** `backoff = min(base * 2^attempt + jitter, max)`

**Retryable errors:**
- Network timeout
- Connection refused
- Broker unavailable
- Temporary failures

**Non-retryable errors:**
- Invalid topic
- Message too large
- Authorization failed

### 7. Error Handling

**Handling send failures:**
```python
future = producer.send('topic', value=b'data')

try:
    metadata = future.result(timeout=5.0)
except Exception as e:
    print(f"Send failed: {e}")
    # Handle error (log, retry, alert)
```

**With error callback:**
```python
def on_error(exception):
    if 'timeout' in str(exception):
        # Retry or log
        pass
    else:
        # Alert ops team
        pass

producer.send('topic', value=b'data', error_callback=on_error)
```

**Circuit breaker (advanced):**
```python
from distributedlog.producer.retry import CircuitBreaker

breaker = CircuitBreaker(
    failure_threshold=5,
    recovery_timeout_ms=60000,
)

def send_with_breaker():
    breaker.call(lambda: producer.send('topic', value=b'data'))
```

### 8. In-Flight Request Management

Limit concurrent requests to prevent overwhelming broker.

```python
producer = Producer(
    bootstrap_servers='localhost:9092',
    max_in_flight=5,
)
```

**How it works:**
- Tracks requests per topic-partition
- Blocks new requests when limit reached
- Ensures message ordering

**Trade-offs:**
- **max_in_flight=1**: Strict ordering, low throughput
- **max_in_flight=5**: Better throughput, possible reordering on retry
- **max_in_flight>5**: High throughput, ordering not guaranteed

## Configuration Reference

```python
from distributedlog.producer import Producer, ProducerConfig, CompressionType

config = ProducerConfig(
    bootstrap_servers='localhost:9092',
    
    # Batching
    batch_size=16384,              # 16KB batches
    linger_ms=0,                   # Send immediately
    
    # Performance
    max_in_flight=5,               # Concurrent requests
    buffer_memory=33554432,        # 32MB buffer
    
    # Compression
    compression_type=CompressionType.GZIP,
    
    # Reliability
    max_retries=3,
    retry_backoff_ms=100,
    request_timeout_ms=30000,
    
    # Metadata
    metadata_max_age_ms=300000,    # 5 min cache
    
    # Partitioning
    partitioner_type='default',
    
    # Blocking
    max_block_ms=60000,            # 1 min max block
)

producer = Producer(config=config)
```

## Best Practices

### 1. Always Close Producer

```python
# Using context manager (recommended)
with Producer(bootstrap_servers='localhost:9092') as producer:
    producer.send('topic', value=b'data')
# Automatically flushed and closed

# Manual close
producer = Producer(bootstrap_servers='localhost:9092')
try:
    producer.send('topic', value=b'data')
finally:
    producer.close()  # Ensures flush
```

### 2. Batch for Throughput

```python
# High throughput config
producer = Producer(
    bootstrap_servers='localhost:9092',
    batch_size=65536,      # 64KB (larger batches)
    linger_ms=100,         # Wait for more messages
    compression_type=CompressionType.LZ4,
)
```

### 3. Low Latency Config

```python
# Low latency config
producer = Producer(
    bootstrap_servers='localhost:9092',
    batch_size=1024,       # 1KB (small batches)
    linger_ms=0,           # Send immediately
    compression_type=CompressionType.NONE,
)
```

### 4. Handle Errors Gracefully

```python
def safe_send(producer, topic, value):
    try:
        future = producer.send(topic, value=value)
        metadata = future.result(timeout=5.0)
        return metadata
    except TimeoutError:
        logger.error("Send timeout", topic=topic)
        # Retry or queue for later
    except Exception as e:
        logger.error("Send failed", topic=topic, error=e)
        # Alert ops team
```

### 5. Use Keys for Ordering

```python
# Messages with same key go to same partition
# Maintaining order within key
for event in user_events:
    producer.send(
        'user-events',
        key=event.user_id.encode(),
        value=event.to_json().encode(),
    )
```

### 6. Monitor Metrics

```python
metrics = producer.metrics()

if metrics['pending_batches'] > 100:
    logger.warning("High pending batches", count=metrics['pending_batches'])

if metrics['in_flight_requests'] == max_in_flight:
    logger.warning("Max in-flight reached")
```

## Performance Tuning

### Throughput Optimization

```python
producer = Producer(
    bootstrap_servers='localhost:9092',
    batch_size=131072,             # 128KB
    linger_ms=100,                 # Accumulate messages
    compression_type=CompressionType.LZ4,
    max_in_flight=10,              # More parallelism
)
```

**Expected:** 50K+ msg/sec on local machine

### Latency Optimization

```python
producer = Producer(
    bootstrap_servers='localhost:9092',
    batch_size=1024,               # 1KB
    linger_ms=0,                   # No wait
    compression_type=CompressionType.NONE,
    max_in_flight=1,               # Strict ordering
)
```

**Expected:** <5ms p99 latency

### Memory Optimization

```python
producer = Producer(
    bootstrap_servers='localhost:9092',
    batch_size=4096,               # 4KB
    buffer_memory=10485760,        # 10MB
    max_in_flight=3,
)
```

## Troubleshooting

### Problem: Slow Sends

**Symptoms:**
- High latency
- Timeouts
- Backpressure

**Solutions:**
1. Increase `batch_size` and `linger_ms`
2. Enable compression
3. Increase `max_in_flight`
4. Check network bandwidth
5. Add more partitions

### Problem: Out of Memory

**Symptoms:**
- MemoryError exceptions
- Producer blocks on send()
- High memory usage

**Solutions:**
1. Reduce `buffer_memory`
2. Reduce `batch_size`
3. Call `flush()` more frequently
4. Reduce `linger_ms`

### Problem: Message Ordering Issues

**Symptoms:**
- Messages arrive out of order
- Retries cause reordering

**Solutions:**
1. Set `max_in_flight=1` (strict ordering)
2. Use key-based partitioning
3. Disable retries (not recommended)
4. Implement idempotent processing

### Problem: High CPU Usage

**Symptoms:**
- CPU at 100%
- Slow compression

**Solutions:**
1. Use LZ4 instead of GZIP
2. Disable compression
3. Reduce batch size
4. Increase `linger_ms` (less frequent compression)

## Examples

### Basic Producer

```python
from distributedlog.producer import Producer

with Producer(bootstrap_servers='localhost:9092') as producer:
    metadata = producer.send('my-topic', value=b'hello').result()
    print(f"Sent to partition {metadata.partition}")
```

### Async Producer with Callbacks

```python
from distributedlog.producer import Producer

results = []

def on_success(metadata):
    results.append(metadata)

with Producer(bootstrap_servers='localhost:9092') as producer:
    for i in range(100):
        producer.send(
            'my-topic',
            value=f'message-{i}'.encode(),
            callback=on_success,
        )
    
    producer.flush()
    print(f"Sent {len(results)} messages")
```

### High-Throughput Producer

```python
from distributedlog.producer import Producer, CompressionType

config = {
    'bootstrap_servers': 'localhost:9092',
    'batch_size': 131072,  # 128KB
    'linger_ms': 100,
    'compression_type': CompressionType.LZ4,
    'max_in_flight': 10,
}

with Producer(**config) as producer:
    for i in range(100000):
        producer.send('my-topic', value=f'message-{i}'.encode())
    
    producer.flush()
```

### Partitioning by User

```python
from distributedlog.producer import Producer

with Producer(bootstrap_servers='localhost:9092') as producer:
    user_events = [
        {'user_id': 'user1', 'action': 'login'},
        {'user_id': 'user1', 'action': 'purchase'},
        {'user_id': 'user2', 'action': 'login'},
    ]
    
    for event in user_events:
        producer.send(
            'user-events',
            key=event['user_id'].encode(),
            value=json.dumps(event).encode(),
        )
    
    producer.flush()
```

## Comparison with Kafka

| Feature | DistributedLog Producer | Kafka Producer |
|---------|------------------------|----------------|
| Batching | ✅ Yes | ✅ Yes |
| Compression | ✅ GZIP, SNAPPY, LZ4 | ✅ GZIP, SNAPPY, LZ4, ZSTD |
| Partitioning | ✅ 5 strategies | ✅ Default + custom |
| Retry logic | ✅ Exponential backoff | ✅ Exponential backoff |
| Idempotence | ❌ Not yet | ✅ Yes |
| Transactions | ❌ Not yet | ✅ Yes |
| Async send | ✅ Yes | ✅ Yes |
| Circuit breaker | ✅ Yes | ❌ No (manual) |

## References

- Kafka Producer API
- "Designing Data-Intensive Applications" - Martin Kleppmann
- Exponential backoff: https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
