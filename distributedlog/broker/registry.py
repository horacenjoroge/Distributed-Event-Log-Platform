"""
Broker registry for cluster membership.

Manages broker registration, health checks, and cluster view.
"""

import asyncio
import time
from typing import Dict, List, Optional

from distributedlog.broker.metadata import (
    BrokerMetadata,
    BrokerState,
    ClusterMetadata,
)
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class BrokerRegistry:
    """
    Registry for broker cluster membership.
    
    Manages:
    - Broker registration and deregistration
    - Broker health monitoring
    - Cluster metadata
    - Leader election (simple, will be replaced with Raft)
    """
    
    def __init__(
        self,
        cluster_id: str,
        heartbeat_timeout_ms: int = 30000,
    ):
        """
        Initialize broker registry.
        
        Args:
            cluster_id: Cluster identifier
            heartbeat_timeout_ms: Heartbeat timeout in milliseconds
        """
        self._cluster = ClusterMetadata(cluster_id=cluster_id)
        self._heartbeat_timeout_ms = heartbeat_timeout_ms
        self._lock = asyncio.Lock()
        
        logger.info(
            "BrokerRegistry initialized",
            cluster_id=cluster_id,
        )
    
    async def register_broker(
        self,
        broker_id: int,
        host: str,
        port: int,
        rack: Optional[str] = None,
    ) -> BrokerMetadata:
        """
        Register a broker in the cluster.
        
        Args:
            broker_id: Unique broker ID
            host: Broker hostname
            port: Broker port
            rack: Optional rack ID
        
        Returns:
            Broker metadata
        """
        async with self._lock:
            metadata = BrokerMetadata(
                broker_id=broker_id,
                host=host,
                port=port,
                rack=rack,
                state=BrokerState.STARTING,
            )
            
            self._cluster.add_broker(metadata)
            
            if self._cluster.controller_id is None:
                self._cluster.controller_id = broker_id
                
                logger.info(
                    "Elected controller",
                    controller_id=broker_id,
                )
            
            logger.info(
                "Broker registered",
                broker_id=broker_id,
                endpoint=metadata.endpoint(),
            )
            
            return metadata
    
    async def deregister_broker(self, broker_id: int) -> None:
        """
        Deregister a broker from the cluster.
        
        Args:
            broker_id: Broker ID
        """
        async with self._lock:
            broker = self._cluster.get_broker(broker_id)
            if broker:
                broker.state = BrokerState.STOPPED
                self._cluster.remove_broker(broker_id)
                
                if self._cluster.controller_id == broker_id:
                    self._elect_new_controller()
                
                logger.info(
                    "Broker deregistered",
                    broker_id=broker_id,
                )
    
    async def update_heartbeat(self, broker_id: int) -> bool:
        """
        Update broker heartbeat.
        
        Args:
            broker_id: Broker ID
        
        Returns:
            True if successful
        """
        async with self._lock:
            broker = self._cluster.get_broker(broker_id)
            if broker:
                broker.update_heartbeat()
                
                if broker.state == BrokerState.STARTING:
                    broker.state = BrokerState.RUNNING
                
                return True
            
            return False
    
    async def mark_broker_failed(self, broker_id: int) -> None:
        """
        Mark broker as failed.
        
        Args:
            broker_id: Broker ID
        """
        async with self._lock:
            broker = self._cluster.get_broker(broker_id)
            if broker:
                broker.state = BrokerState.FAILED
                
                logger.warning(
                    "Broker marked as failed",
                    broker_id=broker_id,
                )
                
                if self._cluster.controller_id == broker_id:
                    self._elect_new_controller()
    
    async def get_broker(self, broker_id: int) -> Optional[BrokerMetadata]:
        """
        Get broker metadata.
        
        Args:
            broker_id: Broker ID
        
        Returns:
            Broker metadata or None
        """
        async with self._lock:
            return self._cluster.get_broker(broker_id)
    
    async def get_all_brokers(self) -> List[BrokerMetadata]:
        """
        Get all registered brokers.
        
        Returns:
            List of broker metadata
        """
        async with self._lock:
            return list(self._cluster.brokers.values())
    
    async def get_live_brokers(self) -> List[BrokerMetadata]:
        """
        Get all live brokers.
        
        Returns:
            List of live broker metadata
        """
        async with self._lock:
            return self._cluster.get_live_brokers()
    
    async def get_controller(self) -> Optional[BrokerMetadata]:
        """
        Get current controller broker.
        
        Returns:
            Controller broker metadata or None
        """
        async with self._lock:
            if self._cluster.controller_id is not None:
                return self._cluster.get_broker(self._cluster.controller_id)
            return None
    
    async def check_broker_health(self) -> None:
        """Check health of all brokers and mark unhealthy ones as failed."""
        async with self._lock:
            failed_brokers = []
            
            for broker in self._cluster.brokers.values():
                if not broker.is_healthy(self._heartbeat_timeout_ms):
                    broker.state = BrokerState.FAILED
                    failed_brokers.append(broker.broker_id)
            
            if failed_brokers:
                logger.warning(
                    "Brokers marked as failed due to heartbeat timeout",
                    failed_brokers=failed_brokers,
                )
                
                if self._cluster.controller_id in failed_brokers:
                    self._elect_new_controller()
    
    def _elect_new_controller(self) -> None:
        """
        Elect new controller (simple: pick first live broker).
        
        In Phase 2, this will be replaced with Raft-based election.
        """
        live_brokers = self._cluster.get_live_brokers()
        
        if live_brokers:
            new_controller = sorted(live_brokers, key=lambda b: b.broker_id)[0]
            self._cluster.controller_id = new_controller.broker_id
            
            logger.info(
                "New controller elected",
                controller_id=new_controller.broker_id,
            )
        else:
            self._cluster.controller_id = None
            
            logger.warning("No live brokers available for controller election")
    
    def get_cluster_metadata(self) -> ClusterMetadata:
        """Get cluster metadata."""
        return self._cluster
