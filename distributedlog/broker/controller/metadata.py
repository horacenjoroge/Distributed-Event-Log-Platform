"""
Metadata propagation for cluster state.

Handles UpdateMetadata RPC for propagating cluster state to brokers.
"""

from dataclasses import dataclass
from typing import Dict, List, Set

from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class PartitionMetadataUpdate:
    """
    Partition metadata update.
    
    Attributes:
        topic: Topic name
        partition: Partition number
        leader: Leader broker ID
        isr: In-Sync Replica broker IDs
        replicas: All replica broker IDs
        leader_epoch: Leader epoch number
    """
    topic: str
    partition: int
    leader: int
    isr: List[int]
    replicas: List[int]
    leader_epoch: int


@dataclass
class BrokerMetadataUpdate:
    """
    Broker metadata update.
    
    Attributes:
        broker_id: Broker ID
        host: Broker host
        port: Broker port
        rack: Broker rack (optional)
    """
    broker_id: int
    host: str
    port: int
    rack: str = ""


@dataclass
class UpdateMetadataRequest:
    """
    UpdateMetadata RPC request.
    
    Sent from controller to all brokers to propagate cluster state.
    
    Attributes:
        controller_id: Controller broker ID
        controller_epoch: Controller epoch number
        partition_updates: Partition metadata updates
        broker_updates: Broker metadata updates
        deleted_topics: Topics that were deleted
    """
    controller_id: int
    controller_epoch: int
    partition_updates: List[PartitionMetadataUpdate]
    broker_updates: List[BrokerMetadataUpdate]
    deleted_topics: List[str] = None
    
    def __post_init__(self):
        if self.deleted_topics is None:
            self.deleted_topics = []


@dataclass
class UpdateMetadataResponse:
    """
    UpdateMetadata RPC response.
    
    Attributes:
        error_code: Error code (0 = success)
        error_message: Error message if failed
    """
    error_code: int = 0
    error_message: str = ""


class MetadataPropagator:
    """
    Propagates metadata updates to brokers.
    
    Coordinates with controller to send UpdateMetadata RPCs.
    """
    
    def __init__(self, controller):
        """
        Initialize metadata propagator.
        
        Args:
            controller: ClusterController instance
        """
        self.controller = controller
        
        # Track last metadata version sent to each broker
        self.broker_metadata_versions: Dict[int, int] = {}
        
        logger.info("MetadataPropagator initialized")
    
    async def propagate_metadata(
        self,
        broker_ids: List[int],
        partition_updates: List[PartitionMetadataUpdate] = None,
        broker_updates: List[BrokerMetadataUpdate] = None,
    ) -> Dict[int, bool]:
        """
        Propagate metadata to brokers.
        
        Args:
            broker_ids: Broker IDs to send updates to
            partition_updates: Partition metadata updates
            broker_updates: Broker metadata updates
        
        Returns:
            Map of broker_id -> success
        """
        if not self.controller.state.is_active:
            logger.warning("Not controller, cannot propagate metadata")
            return {}
        
        request = UpdateMetadataRequest(
            controller_id=self.controller.broker_id,
            controller_epoch=self.controller.state.controller_epoch,
            partition_updates=partition_updates or [],
            broker_updates=broker_updates or [],
        )
        
        results = {}
        
        for broker_id in broker_ids:
            if broker_id == self.controller.broker_id:
                # Don't send to self
                continue
            
            try:
                response = await self._send_update_metadata(broker_id, request)
                
                if response.error_code == 0:
                    results[broker_id] = True
                    self.broker_metadata_versions[broker_id] = \
                        self.controller.state.controller_epoch
                else:
                    results[broker_id] = False
                    logger.warning(
                        "Failed to update metadata",
                        broker_id=broker_id,
                        error=response.error_message,
                    )
            except Exception as e:
                results[broker_id] = False
                logger.error(
                    "Error sending metadata update",
                    broker_id=broker_id,
                    error=str(e),
                )
        
        return results
    
    async def _send_update_metadata(
        self,
        broker_id: int,
        request: UpdateMetadataRequest,
    ) -> UpdateMetadataResponse:
        """
        Send UpdateMetadata RPC to broker.
        
        Args:
            broker_id: Target broker ID
            request: Update request
        
        Returns:
            Update response
        """
        # TODO: Implement actual RPC call
        # For now, return success
        
        logger.debug(
            "Sent metadata update",
            broker_id=broker_id,
            controller_epoch=request.controller_epoch,
            partition_count=len(request.partition_updates),
        )
        
        return UpdateMetadataResponse()
    
    async def propagate_partition_leader_change(
        self,
        topic: str,
        partition: int,
        leader: int,
        isr: Set[int],
        replicas: List[int],
    ) -> None:
        """
        Propagate partition leader change to all brokers.
        
        Args:
            topic: Topic name
            partition: Partition number
            leader: New leader broker ID
            isr: In-Sync Replica broker IDs
            replicas: All replica broker IDs
        """
        update = PartitionMetadataUpdate(
            topic=topic,
            partition=partition,
            leader=leader,
            isr=sorted(list(isr)),
            replicas=replicas,
            leader_epoch=self.controller.state.controller_epoch,
        )
        
        # Send to all replicas
        await self.propagate_metadata(
            broker_ids=replicas,
            partition_updates=[update],
        )
        
        logger.info(
            "Propagated partition leader change",
            topic=topic,
            partition=partition,
            leader=leader,
            isr=sorted(list(isr)),
        )
    
    async def propagate_broker_registration(
        self,
        broker_id: int,
        host: str,
        port: int,
    ) -> None:
        """
        Propagate broker registration to all brokers.
        
        Args:
            broker_id: Broker ID
            host: Broker host
            port: Broker port
        """
        update = BrokerMetadataUpdate(
            broker_id=broker_id,
            host=host,
            port=port,
        )
        
        # Get all broker IDs
        broker_ids = list(self.controller.broker_last_heartbeat.keys())
        
        await self.propagate_metadata(
            broker_ids=broker_ids,
            broker_updates=[update],
        )
        
        logger.info(
            "Propagated broker registration",
            broker_id=broker_id,
            host=host,
            port=port,
        )


class MetadataCache:
    """
    Metadata cache maintained by each broker.
    
    Receives and stores UpdateMetadata RPCs from controller.
    """
    
    def __init__(self, broker_id: int):
        """
        Initialize metadata cache.
        
        Args:
            broker_id: This broker's ID
        """
        self.broker_id = broker_id
        
        # Controller info
        self.controller_id: Optional[int] = None
        self.controller_epoch = 0
        
        # Partition metadata: (topic, partition) -> PartitionMetadataUpdate
        self.partition_metadata: Dict[tuple, PartitionMetadataUpdate] = {}
        
        # Broker metadata: broker_id -> BrokerMetadataUpdate
        self.broker_metadata: Dict[int, BrokerMetadataUpdate] = {}
        
        logger.info("MetadataCache initialized", broker_id=broker_id)
    
    async def handle_update_metadata(
        self,
        request: UpdateMetadataRequest,
    ) -> UpdateMetadataResponse:
        """
        Handle UpdateMetadata RPC from controller.
        
        Args:
            request: Update request
        
        Returns:
            Update response
        """
        # Check controller epoch
        if request.controller_epoch < self.controller_epoch:
            logger.warning(
                "Rejected stale metadata update",
                current_epoch=self.controller_epoch,
                request_epoch=request.controller_epoch,
            )
            
            return UpdateMetadataResponse(
                error_code=1,
                error_message="Stale controller epoch",
            )
        
        # Update controller info
        self.controller_id = request.controller_id
        self.controller_epoch = request.controller_epoch
        
        # Update partition metadata
        for update in request.partition_updates:
            key = (update.topic, update.partition)
            self.partition_metadata[key] = update
        
        # Update broker metadata
        for update in request.broker_updates:
            self.broker_metadata[update.broker_id] = update
        
        # Handle deleted topics
        if request.deleted_topics:
            for topic in request.deleted_topics:
                self._delete_topic_metadata(topic)
        
        logger.info(
            "Updated metadata cache",
            controller_epoch=self.controller_epoch,
            partition_updates=len(request.partition_updates),
            broker_updates=len(request.broker_updates),
        )
        
        return UpdateMetadataResponse()
    
    def _delete_topic_metadata(self, topic: str) -> None:
        """
        Delete metadata for a topic.
        
        Args:
            topic: Topic name
        """
        keys_to_delete = [
            key for key in self.partition_metadata.keys()
            if key[0] == topic
        ]
        
        for key in keys_to_delete:
            del self.partition_metadata[key]
    
    def get_partition_leader(self, topic: str, partition: int) -> Optional[int]:
        """
        Get partition leader from cache.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            Leader broker ID or None
        """
        key = (topic, partition)
        
        if key in self.partition_metadata:
            return self.partition_metadata[key].leader
        
        return None
    
    def get_partition_replicas(self, topic: str, partition: int) -> List[int]:
        """
        Get partition replicas from cache.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            List of replica broker IDs
        """
        key = (topic, partition)
        
        if key in self.partition_metadata:
            return self.partition_metadata[key].replicas.copy()
        
        return []
    
    def get_broker_address(self, broker_id: int) -> Optional[tuple]:
        """
        Get broker address from cache.
        
        Args:
            broker_id: Broker ID
        
        Returns:
            (host, port) tuple or None
        """
        if broker_id in self.broker_metadata:
            broker = self.broker_metadata[broker_id]
            return (broker.host, broker.port)
        
        return None
