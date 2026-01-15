"""
DistributedLog - A distributed commit log system built from scratch.

This package implements a Kafka/Pulsar-like distributed event streaming platform
with features including:
- Append-only commit log on disk
- Partitioning across multiple brokers
- Leader-follower replication with automatic failover
- Raft consensus for leader election
- Exactly-once message delivery semantics
- Consumer groups with automatic rebalancing
- Transactional writes
"""

__version__ = "0.1.0"
__author__ = "Horace Njoroge"

from distributedlog.core import log, index, storage
from distributedlog.broker import server, replication, coordinator

__all__ = [
    "log",
    "index",
    "storage",
    "server",
    "replication",
    "coordinator",
]
