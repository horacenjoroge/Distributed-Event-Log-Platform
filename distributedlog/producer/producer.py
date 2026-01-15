"""
Producer client for sending messages to DistributedLog.

Provides high-level API for message production with:
- Sync and async send
- Automatic batching
- Compression
- Retry logic
- Error handling
"""

import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Callable, List, Optional, Union

from distributedlog.core.log.log import Log
from distributedlog.producer.batch import (
    ProducerBatch,
    ProducerRecord,
    RecordAccumulator,
    RecordMetadata,
)
from distributedlog.producer.compression import Compressor, CompressionType
from distributedlog.producer.metadata import (
    BootstrapServerParser,
    MetadataCache,
    TopicMetadata,
    PartitionMetadata,
)
from distributedlog.producer.partitioner import Partitioner, create_partitioner
from distributedlog.producer.retry import RetryConfig, RetryManager, RetryableError
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ProducerConfig:
    """
    Configuration for producer.
    
    Attributes:
        bootstrap_servers: Initial broker list
        batch_size: Max batch size in bytes
        linger_ms: Max wait time before sending batch
        max_in_flight: Max in-flight requests per connection
        compression_type: Compression algorithm
        max_retries: Maximum retry attempts
        retry_backoff_ms: Initial retry backoff
        request_timeout_ms: Request timeout
        metadata_max_age_ms: Max metadata cache age
        partitioner_type: Partitioner strategy
        max_block_ms: Max time to block on send()
        buffer_memory: Total memory for buffering
    """
    bootstrap_servers: Union[str, List[str]] = "localhost:9092"
    batch_size: int = 16384  # 16KB
    linger_ms: int = 0  # Send immediately
    max_in_flight: int = 5
    compression_type: CompressionType = CompressionType.NONE
    max_retries: int = 3
    retry_backoff_ms: int = 100
    request_timeout_ms: int = 30000
    metadata_max_age_ms: int = 300000  # 5 minutes
    partitioner_type: str = "default"
    max_block_ms: int = 60000
    buffer_memory: int = 33554432  # 32MB


class Producer:
    """
    High-level producer client.
    
    Example:
        producer = Producer(bootstrap_servers=['localhost:9092'])
        
        # Sync send
        metadata = producer.send('my-topic', key=b'key', value=b'value')
        
        # Async send
        def on_success(metadata):
            print(f"Sent to partition {metadata.partition}")
        
        producer.send(
            'my-topic',
            value=b'value',
            callback=on_success
        )
        
        producer.flush()
        producer.close()
    """
    
    def __init__(
        self,
        bootstrap_servers: Union[str, List[str], None] = None,
        config: Optional[ProducerConfig] = None,
        **kwargs,
    ):
        """
        Initialize producer.
        
        Args:
            bootstrap_servers: Broker list (overrides config)
            config: Producer configuration
            **kwargs: Additional config overrides
        """
        self.config = config or ProducerConfig()
        
        if bootstrap_servers is not None:
            self.config.bootstrap_servers = bootstrap_servers
        
        for key, value in kwargs.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)
        
        self._closed = False
        self._sender_thread: Optional[threading.Thread] = None
        self._executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="producer")
        
        self._accumulator = RecordAccumulator(
            batch_size=self.config.batch_size,
            linger_ms=self.config.linger_ms,
            max_in_flight=self.config.max_in_flight,
        )
        
        self._compressor = Compressor(self.config.compression_type)
        
        self._partitioner = create_partitioner(self.config.partitioner_type)
        
        self._metadata = MetadataCache(self.config.metadata_max_age_ms)
        
        self._retry_manager = RetryManager(
            RetryConfig(
                max_retries=self.config.max_retries,
                retry_backoff_ms=self.config.retry_backoff_ms,
            )
        )
        
        brokers = BootstrapServerParser.parse(self.config.bootstrap_servers)
        self._metadata.update_brokers(brokers)
        
        self._logs: dict[str, dict[int, Log]] = {}
        
        self._start_sender_thread()
        
        logger.info(
            "Producer initialized",
            bootstrap_servers=self.config.bootstrap_servers,
            compression=self.config.compression_type.name,
            batch_size=self.config.batch_size,
        )
    
    def send(
        self,
        topic: str,
        value: bytes,
        key: Optional[bytes] = None,
        partition: Optional[int] = None,
        timestamp: Optional[int] = None,
        callback: Optional[Callable[[RecordMetadata], None]] = None,
        error_callback: Optional[Callable[[Exception], None]] = None,
    ) -> Future[RecordMetadata]:
        """
        Send message to topic.
        
        Args:
            topic: Topic name
            value: Message value
            key: Optional message key
            partition: Optional partition (auto-assigned if None)
            timestamp: Optional timestamp (current time if None)
            callback: Success callback
            error_callback: Error callback
        
        Returns:
            Future that resolves to RecordMetadata
        
        Raises:
            Exception: If producer is closed or send fails
        """
        if self._closed:
            raise RuntimeError("Producer is closed")
        
        future: Future[RecordMetadata] = Future()
        
        def on_complete(metadata: Optional[RecordMetadata], error: Optional[Exception]):
            if error:
                future.set_exception(error)
                if error_callback:
                    error_callback(error)
            else:
                future.set_result(metadata)
                if callback:
                    callback(metadata)
        
        try:
            record = ProducerRecord(
                topic=topic,
                partition=partition,
                key=key,
                value=value,
                timestamp=timestamp,
            )
            
            self._do_send(record, on_complete)
        
        except Exception as e:
            on_complete(None, e)
        
        return future
    
    def _do_send(
        self,
        record: ProducerRecord,
        callback: Callable[[Optional[RecordMetadata], Optional[Exception]], None],
    ) -> None:
        """
        Internal send implementation.
        
        Args:
            record: Record to send
            callback: Completion callback
        """
        try:
            if self._metadata.needs_topic_metadata(record.topic):
                self._refresh_metadata(record.topic)
            
            partition_count = self._metadata.get_partition_count(record.topic)
            if partition_count == 0:
                partition_count = 1
                logger.warning(
                    "No metadata for topic, using single partition",
                    topic=record.topic,
                )
            
            if record.partition is None:
                record.partition = self._partitioner.partition(
                    record.topic,
                    record.key,
                    record.value,
                    partition_count,
                )
            
            batch = self._accumulator.append(record, record.partition)
            
            if batch is not None:
                self._executor.submit(self._send_batch, batch)
            
            callback(
                RecordMetadata(
                    topic=record.topic,
                    partition=record.partition,
                    offset=0,
                    timestamp=record.timestamp,
                ),
                None,
            )
        
        except Exception as e:
            logger.error("Send failed", topic=record.topic, error=str(e))
            callback(None, e)
    
    def _send_batch(self, batch: ProducerBatch) -> None:
        """
        Send batch to broker.
        
        Args:
            batch: Batch to send
        """
        def send_operation():
            log = self._get_or_create_log(batch.topic, batch.partition)
            
            for record in batch.records:
                offset = log.append(key=record.key, value=record.value)
                logger.debug(
                    "Wrote message",
                    topic=batch.topic,
                    partition=batch.partition,
                    offset=offset,
                )
            
            log.flush()
        
        try:
            self._retry_manager.execute_with_retry(
                send_operation,
                operation_name=f"send_batch_{batch.topic}_{batch.partition}",
            )
            
            logger.info(
                "Batch sent successfully",
                topic=batch.topic,
                partition=batch.partition,
                records=len(batch.records),
            )
        
        except Exception as e:
            logger.error(
                "Batch send failed after retries",
                topic=batch.topic,
                partition=batch.partition,
                error=str(e),
            )
    
    def _get_or_create_log(self, topic: str, partition: int) -> Log:
        """
        Get or create log for topic-partition.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            Log instance
        """
        if topic not in self._logs:
            self._logs[topic] = {}
        
        if partition not in self._logs[topic]:
            from pathlib import Path
            log_dir = Path(f"/tmp/distributedlog/{topic}/{partition}")
            log_dir.mkdir(parents=True, exist_ok=True)
            
            self._logs[topic][partition] = Log(
                directory=log_dir,
                max_segment_size=self.config.batch_size * 100,
                fsync_on_append=False,
            )
        
        return self._logs[topic][partition]
    
    def _refresh_metadata(self, topic: str) -> None:
        """
        Refresh metadata for topic.
        
        Args:
            topic: Topic name
        """
        logger.info("Refreshing metadata", topic=topic)
        
        topic_metadata = TopicMetadata(
            topic=topic,
            partitions=[
                PartitionMetadata(
                    topic=topic,
                    partition=0,
                    leader=0,
                    replicas=[0],
                    isr=[0],
                )
            ],
        )
        
        self._metadata.update_topic(topic_metadata)
    
    def _start_sender_thread(self) -> None:
        """Start background thread for sending expired batches."""
        def sender_loop():
            while not self._closed:
                try:
                    expired_batches = self._accumulator.drain_expired_batches()
                    
                    for batch in expired_batches:
                        self._executor.submit(self._send_batch, batch)
                    
                    time.sleep(0.01)
                
                except Exception as e:
                    logger.error("Sender thread error", error=str(e))
        
        self._sender_thread = threading.Thread(
            target=sender_loop,
            name="producer-sender",
            daemon=True,
        )
        self._sender_thread.start()
    
    def flush(self, timeout_ms: Optional[int] = None) -> None:
        """
        Flush all pending messages.
        
        Args:
            timeout_ms: Max time to wait (None = wait forever)
        """
        logger.info("Flushing producer")
        
        batches = self._accumulator.drain_all()
        
        futures = []
        for batch in batches:
            future = self._executor.submit(self._send_batch, batch)
            futures.append(future)
        
        start_time = time.time()
        for future in futures:
            if timeout_ms is not None:
                elapsed_ms = (time.time() - start_time) * 1000
                remaining_ms = max(0, timeout_ms - elapsed_ms)
                future.result(timeout=remaining_ms / 1000.0)
            else:
                future.result()
        
        for topic_logs in self._logs.values():
            for log in topic_logs.values():
                log.flush()
        
        logger.info("Producer flushed")
    
    def close(self, timeout_ms: int = 30000) -> None:
        """
        Close producer and release resources.
        
        Args:
            timeout_ms: Max time to wait for pending sends
        """
        if self._closed:
            return
        
        logger.info("Closing producer")
        
        self._closed = True
        
        try:
            self.flush(timeout_ms)
        except Exception as e:
            logger.error("Error during flush on close", error=str(e))
        
        self._executor.shutdown(wait=True, timeout=timeout_ms / 1000.0)
        
        for topic_logs in self._logs.values():
            for log in topic_logs.values():
                log.close()
        
        logger.info("Producer closed")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    def metrics(self) -> dict:
        """
        Get producer metrics.
        
        Returns:
            Dictionary with metrics
        """
        return {
            "pending_batches": self._accumulator.pending_count(),
            "in_flight_requests": self._accumulator.in_flight_count(),
            "metadata": self._metadata.get_stats(),
            "closed": self._closed,
        }
