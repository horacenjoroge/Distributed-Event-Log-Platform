"""
Broker metadata and cluster registration.

Manages broker identity, cluster membership, and broker metadata.
"""

import socket
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set

from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class BrokerState(str, Enum):
    """Broker operational states."""
    
    STARTING = "starting"      # Broker starting up
    RUNNING = "running"        # Fully operational
    STOPPING = "stopping"      # Graceful shutdown
    STOPPED = "stopped"        # Stopped
    FAILED = "failed"          # Failure state


@dataclass
class BrokerMetadata:
    """
    Metadata about a broker in the cluster.
    
    Attributes:
        broker_id: Unique broker identifier
        host: Broker hostname/IP
        port: Broker port
        rack: Optional rack ID for topology awareness
        state: Current broker state
        registered_at: Registration timestamp
        last_heartbeat: Last heartbeat timestamp
    """
    broker_id: int
    host: str
    port: int
    rack: Optional[str] = None
    state: BrokerState = BrokerState.STARTING
    registered_at: int = 0
    last_heartbeat: int = 0
    
    def __post_init__(self):
        if self.registered_at == 0:
            self.registered_at = int(time.time() * 1000)
        if self.last_heartbeat == 0:
            self.last_heartbeat = int(time.time() * 1000)
    
    def endpoint(self) -> str:
        """
        Get broker endpoint.
        
        Returns:
            Endpoint string (host:port)
        """
        return f"{self.host}:{self.port}"
    
    def update_heartbeat(self) -> None:
        """Update last heartbeat timestamp."""
        self.last_heartbeat = int(time.time() * 1000)
    
    def is_healthy(self, timeout_ms: int = 30000) -> bool:
        """
        Check if broker is healthy.
        
        Args:
            timeout_ms: Heartbeat timeout in milliseconds
        
        Returns:
            True if broker is healthy
        """
        if self.state not in [BrokerState.RUNNING, BrokerState.STARTING]:
            return False
        
        current_time = int(time.time() * 1000)
        return (current_time - self.last_heartbeat) < timeout_ms
    
    def to_dict(self) -> dict:
        """Convert to dictionary for serialization."""
        return {
            "broker_id": self.broker_id,
            "host": self.host,
            "port": self.port,
            "rack": self.rack,
            "state": self.state.value,
            "registered_at": self.registered_at,
            "last_heartbeat": self.last_heartbeat,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "BrokerMetadata":
        """Create from dictionary."""
        return cls(
            broker_id=data["broker_id"],
            host=data["host"],
            port=data["port"],
            rack=data.get("rack"),
            state=BrokerState(data.get("state", "starting")),
            registered_at=data.get("registered_at", 0),
            last_heartbeat=data.get("last_heartbeat", 0),
        )


@dataclass
class PartitionMetadata:
    """
    Metadata about a topic partition.
    
    Attributes:
        topic: Topic name
        partition: Partition number
        leader: Leader broker ID
        replicas: List of replica broker IDs
        isr: In-sync replica broker IDs
    """
    topic: str
    partition: int
    leader: int
    replicas: List[int] = field(default_factory=list)
    isr: List[int] = field(default_factory=list)
    
    def is_under_replicated(self) -> bool:
        """Check if partition is under-replicated."""
        return len(self.isr) < len(self.replicas)
    
    def is_leader(self, broker_id: int) -> bool:
        """Check if broker is leader for this partition."""
        return self.leader == broker_id
    
    def is_replica(self, broker_id: int) -> bool:
        """Check if broker is a replica for this partition."""
        return broker_id in self.replicas
    
    def is_in_sync(self, broker_id: int) -> bool:
        """Check if broker replica is in-sync."""
        return broker_id in self.isr


@dataclass
class ClusterMetadata:
    """
    Metadata about the entire cluster.
    
    Attributes:
        cluster_id: Unique cluster identifier
        brokers: Map of broker_id to BrokerMetadata
        partitions: Map of (topic, partition) to PartitionMetadata
        controller_id: Current controller broker ID
    """
    cluster_id: str
    brokers: Dict[int, BrokerMetadata] = field(default_factory=dict)
    partitions: Dict[tuple, PartitionMetadata] = field(default_factory=dict)
    controller_id: Optional[int] = None
    
    def add_broker(self, metadata: BrokerMetadata) -> None:
        """
        Add or update broker in cluster.
        
        Args:
            metadata: Broker metadata
        """
        self.brokers[metadata.broker_id] = metadata
        
        logger.info(
            "Broker added to cluster",
            broker_id=metadata.broker_id,
            endpoint=metadata.endpoint(),
        )
    
    def remove_broker(self, broker_id: int) -> None:
        """
        Remove broker from cluster.
        
        Args:
            broker_id: Broker ID to remove
        """
        if broker_id in self.brokers:
            del self.brokers[broker_id]
            
            logger.info(
                "Broker removed from cluster",
                broker_id=broker_id,
            )
    
    def get_broker(self, broker_id: int) -> Optional[BrokerMetadata]:
        """
        Get broker metadata.
        
        Args:
            broker_id: Broker ID
        
        Returns:
            Broker metadata or None
        """
        return self.brokers.get(broker_id)
    
    def get_live_brokers(self) -> List[BrokerMetadata]:
        """
        Get all live brokers.
        
        Returns:
            List of live broker metadata
        """
        return [
            broker for broker in self.brokers.values()
            if broker.is_healthy()
        ]
    
    def add_partition(self, metadata: PartitionMetadata) -> None:
        """
        Add partition metadata.
        
        Args:
            metadata: Partition metadata
        """
        key = (metadata.topic, metadata.partition)
        self.partitions[key] = metadata
    
    def get_partition(
        self,
        topic: str,
        partition: int,
    ) -> Optional[PartitionMetadata]:
        """
        Get partition metadata.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            Partition metadata or None
        """
        return self.partitions.get((topic, partition))
    
    def get_partitions_for_topic(self, topic: str) -> List[PartitionMetadata]:
        """
        Get all partitions for a topic.
        
        Args:
            topic: Topic name
        
        Returns:
            List of partition metadata
        """
        return [
            p for (t, _), p in self.partitions.items()
            if t == topic
        ]
    
    def get_partitions_for_broker(
        self,
        broker_id: int,
    ) -> List[PartitionMetadata]:
        """
        Get all partitions where broker is a replica.
        
        Args:
            broker_id: Broker ID
        
        Returns:
            List of partition metadata
        """
        return [
            p for p in self.partitions.values()
            if p.is_replica(broker_id)
        ]
    
    def get_leader_partitions(
        self,
        broker_id: int,
    ) -> List[PartitionMetadata]:
        """
        Get all partitions where broker is the leader.
        
        Args:
            broker_id: Broker ID
        
        Returns:
            List of partition metadata
        """
        return [
            p for p in self.partitions.values()
            if p.is_leader(broker_id)
        ]


def get_local_hostname() -> str:
    """
    Get local hostname.
    
    Returns:
        Hostname string
    """
    try:
        return socket.gethostname()
    except Exception:
        return "localhost"


def get_local_ip() -> str:
    """
    Get local IP address.
    
    Returns:
        IP address string
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"
