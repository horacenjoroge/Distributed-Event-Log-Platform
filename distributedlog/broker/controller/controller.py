"""
Cluster controller for centralized coordination.

Manages partition leadership, replica assignment, and cluster metadata.
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

from distributedlog.broker.metadata import BrokerMetadata, ClusterMetadata, PartitionMetadata
from distributedlog.consensus.raft.node import RaftNode
from distributedlog.consensus.raft.state import LogEntry
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ControllerState:
    """
    Controller state.
    
    Attributes:
        controller_id: Current controller broker ID
        controller_epoch: Monotonic epoch number
        is_active: Whether this broker is the active controller
    """
    controller_id: Optional[int] = None
    controller_epoch: int = 0
    is_active: bool = False


@dataclass
class PartitionAssignment:
    """
    Partition replica assignment.
    
    Attributes:
        topic: Topic name
        partition: Partition number
        replicas: List of replica broker IDs
        leader: Current leader broker ID
        isr: In-Sync Replica broker IDs
    """
    topic: str
    partition: int
    replicas: List[int]
    leader: Optional[int] = None
    isr: Set[int] = field(default_factory=set)


class ClusterController:
    """
    Centralized cluster controller.
    
    Responsibilities:
    - Partition leader election
    - Replica assignment
    - ISR management
    - Broker failure detection
    - Metadata propagation
    
    Uses Raft for controller election and coordination.
    """
    
    def __init__(
        self,
        broker_id: int,
        raft_node: RaftNode,
        min_isr: int = 1,
        replica_lag_threshold: int = 10,
    ):
        """
        Initialize cluster controller.
        
        Args:
            broker_id: This broker's ID
            raft_node: Raft node for consensus
            min_isr: Minimum ISR size
            replica_lag_threshold: Max lag for ISR membership
        """
        self.broker_id = broker_id
        self.raft_node = raft_node
        self.min_isr = min_isr
        self.replica_lag_threshold = replica_lag_threshold
        
        # Controller state
        self.state = ControllerState()
        
        # Cluster metadata
        self.cluster_metadata = ClusterMetadata()
        
        # Partition assignments: (topic, partition) -> PartitionAssignment
        self.partition_assignments: Dict[tuple, PartitionAssignment] = {}
        
        # Broker liveness tracking
        self.broker_last_heartbeat: Dict[int, int] = {}
        self.broker_health_timeout = 30000  # 30 seconds
        
        # Background tasks
        self._monitoring_task: Optional[asyncio.Task] = None
        self._running = False
        
        # Set Raft callback for applying committed entries
        self.raft_node.apply_callback = self._apply_raft_command
        
        logger.info(
            "ClusterController initialized",
            broker_id=broker_id,
            min_isr=min_isr,
        )
    
    async def start(self) -> None:
        """Start controller."""
        if self._running:
            return
        
        self._running = True
        
        # Start Raft node
        await self.raft_node.start()
        
        # Start monitoring task
        self._monitoring_task = asyncio.create_task(self._monitoring_loop())
        
        logger.info("ClusterController started")
    
    async def stop(self) -> None:
        """Stop controller."""
        self._running = False
        
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
        
        await self.raft_node.stop()
        
        logger.info("ClusterController stopped")
    
    async def _monitoring_loop(self) -> None:
        """Monitor controller state and broker health."""
        while self._running:
            try:
                # Check if we're the Raft leader (i.e., controller)
                await self._check_controller_status()
                
                # If we're controller, perform monitoring
                if self.state.is_active:
                    await self._monitor_brokers()
                    await self._monitor_partitions()
                
                await asyncio.sleep(1.0)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(
                    "Error in monitoring loop",
                    error=str(e),
                )
                await asyncio.sleep(1.0)
    
    async def _check_controller_status(self) -> None:
        """Check if we're the active controller."""
        is_leader = self.raft_node.is_leader()
        
        if is_leader and not self.state.is_active:
            # Became controller
            await self._become_controller()
        elif not is_leader and self.state.is_active:
            # Lost controller status
            await self._resign_controller()
    
    async def _become_controller(self) -> None:
        """Become the active controller."""
        self.state.controller_id = self.broker_id
        self.state.controller_epoch += 1
        self.state.is_active = True
        
        logger.info(
            "Became controller",
            controller_id=self.broker_id,
            epoch=self.state.controller_epoch,
        )
        
        # Propose controller change to Raft
        await self.raft_node.propose(
            "controller_change",
            {
                "controller_id": self.broker_id,
                "epoch": self.state.controller_epoch,
            },
        )
        
        # Initialize controller state
        await self._initialize_controller_state()
    
    async def _resign_controller(self) -> None:
        """Resign from controller role."""
        logger.info(
            "Resigned as controller",
            controller_id=self.broker_id,
            epoch=self.state.controller_epoch,
        )
        
        self.state.is_active = False
    
    async def _initialize_controller_state(self) -> None:
        """Initialize controller state after becoming controller."""
        # Reload cluster metadata
        # Detect failed brokers
        # Elect leaders for leaderless partitions
        
        logger.info("Initialized controller state")
    
    async def _monitor_brokers(self) -> None:
        """Monitor broker health."""
        current_time = int(time.time() * 1000)
        failed_brokers = []
        
        for broker_id, last_heartbeat in self.broker_last_heartbeat.items():
            if current_time - last_heartbeat > self.broker_health_timeout:
                failed_brokers.append(broker_id)
        
        # Handle failed brokers
        for broker_id in failed_brokers:
            await self._handle_broker_failure(broker_id)
    
    async def _monitor_partitions(self) -> None:
        """Monitor partition health and ISR."""
        for key, assignment in self.partition_assignments.items():
            # Check if leader is alive
            if assignment.leader and not self._is_broker_alive(assignment.leader):
                await self._elect_partition_leader(assignment.topic, assignment.partition)
            
            # Check ISR health
            await self._update_partition_isr(assignment.topic, assignment.partition)
    
    def _is_broker_alive(self, broker_id: int) -> bool:
        """
        Check if broker is alive.
        
        Args:
            broker_id: Broker ID
        
        Returns:
            True if alive
        """
        if broker_id not in self.broker_last_heartbeat:
            return False
        
        current_time = int(time.time() * 1000)
        last_heartbeat = self.broker_last_heartbeat[broker_id]
        
        return current_time - last_heartbeat <= self.broker_health_timeout
    
    # Partition Management
    
    async def create_partition(
        self,
        topic: str,
        partition: int,
        replication_factor: int,
    ) -> bool:
        """
        Create a new partition.
        
        Args:
            topic: Topic name
            partition: Partition number
            replication_factor: Number of replicas
        
        Returns:
            True if successful
        """
        if not self.state.is_active:
            logger.warning("Not controller, cannot create partition")
            return False
        
        # Assign replicas
        replicas = await self._assign_replicas(replication_factor)
        
        if not replicas:
            logger.error("Failed to assign replicas")
            return False
        
        # Create assignment
        assignment = PartitionAssignment(
            topic=topic,
            partition=partition,
            replicas=replicas,
            leader=replicas[0],  # First replica is initial leader
            isr={replicas[0]},   # Initially only leader in ISR
        )
        
        key = (topic, partition)
        self.partition_assignments[key] = assignment
        
        # Propose to Raft
        await self.raft_node.propose(
            "create_partition",
            {
                "topic": topic,
                "partition": partition,
                "replicas": replicas,
                "leader": assignment.leader,
            },
        )
        
        logger.info(
            "Created partition",
            topic=topic,
            partition=partition,
            replicas=replicas,
            leader=assignment.leader,
        )
        
        return True
    
    async def _assign_replicas(self, count: int) -> List[int]:
        """
        Assign replicas to brokers.
        
        Uses round-robin assignment.
        
        Args:
            count: Number of replicas
        
        Returns:
            List of broker IDs
        """
        alive_brokers = [
            broker_id
            for broker_id in self.broker_last_heartbeat.keys()
            if self._is_broker_alive(broker_id)
        ]
        
        if len(alive_brokers) < count:
            logger.error(
                "Not enough brokers for replication",
                required=count,
                available=len(alive_brokers),
            )
            return []
        
        # Simple round-robin for now
        replicas = alive_brokers[:count]
        
        return replicas
    
    async def _elect_partition_leader(
        self,
        topic: str,
        partition: int,
    ) -> Optional[int]:
        """
        Elect leader for partition.
        
        Leader is elected from ISR (preferred) or replicas.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            New leader broker ID or None
        """
        key = (topic, partition)
        
        if key not in self.partition_assignments:
            return None
        
        assignment = self.partition_assignments[key]
        
        # Try to elect from ISR
        for broker_id in assignment.isr:
            if self._is_broker_alive(broker_id):
                await self._set_partition_leader(topic, partition, broker_id)
                return broker_id
        
        # Unclean election: elect from any alive replica
        logger.warning(
            "No ISR available, performing unclean leader election",
            topic=topic,
            partition=partition,
        )
        
        for broker_id in assignment.replicas:
            if self._is_broker_alive(broker_id):
                await self._set_partition_leader(topic, partition, broker_id)
                return broker_id
        
        logger.error(
            "No replicas available for leader election",
            topic=topic,
            partition=partition,
        )
        
        return None
    
    async def _set_partition_leader(
        self,
        topic: str,
        partition: int,
        leader_id: int,
    ) -> None:
        """
        Set partition leader.
        
        Args:
            topic: Topic name
            partition: Partition number
            leader_id: New leader broker ID
        """
        key = (topic, partition)
        
        if key not in self.partition_assignments:
            return
        
        assignment = self.partition_assignments[key]
        old_leader = assignment.leader
        assignment.leader = leader_id
        
        # Reset ISR to just the new leader
        assignment.isr = {leader_id}
        
        # Propose to Raft
        await self.raft_node.propose(
            "change_partition_leader",
            {
                "topic": topic,
                "partition": partition,
                "leader": leader_id,
                "epoch": self.state.controller_epoch,
            },
        )
        
        logger.info(
            "Elected partition leader",
            topic=topic,
            partition=partition,
            old_leader=old_leader,
            new_leader=leader_id,
            epoch=self.state.controller_epoch,
        )
    
    async def _update_partition_isr(
        self,
        topic: str,
        partition: int,
    ) -> None:
        """
        Update partition ISR.
        
        Args:
            topic: Topic name
            partition: Partition number
        """
        key = (topic, partition)
        
        if key not in self.partition_assignments:
            return
        
        assignment = self.partition_assignments[key]
        
        # Get current ISR from replication manager (would integrate with Task 11)
        # For now, just ensure ISR members are alive
        
        alive_isr = {
            broker_id
            for broker_id in assignment.isr
            if self._is_broker_alive(broker_id)
        }
        
        if alive_isr != assignment.isr:
            assignment.isr = alive_isr
            
            logger.info(
                "Updated partition ISR",
                topic=topic,
                partition=partition,
                isr=sorted(list(alive_isr)),
            )
    
    # Broker Management
    
    async def register_broker(self, broker_id: int, metadata: BrokerMetadata) -> None:
        """
        Register broker with controller.
        
        Args:
            broker_id: Broker ID
            metadata: Broker metadata
        """
        self.cluster_metadata.brokers[broker_id] = metadata
        self.broker_last_heartbeat[broker_id] = int(time.time() * 1000)
        
        logger.info(
            "Registered broker",
            broker_id=broker_id,
            host=metadata.host,
            port=metadata.port,
        )
        
        if self.state.is_active:
            # Propose to Raft
            await self.raft_node.propose(
                "register_broker",
                {
                    "broker_id": broker_id,
                    "host": metadata.host,
                    "port": metadata.port,
                },
            )
    
    async def broker_heartbeat(self, broker_id: int) -> None:
        """
        Record broker heartbeat.
        
        Args:
            broker_id: Broker ID
        """
        self.broker_last_heartbeat[broker_id] = int(time.time() * 1000)
    
    async def _handle_broker_failure(self, broker_id: int) -> None:
        """
        Handle broker failure.
        
        Args:
            broker_id: Failed broker ID
        """
        logger.warning("Detected broker failure", broker_id=broker_id)
        
        # Remove from heartbeat tracking
        del self.broker_last_heartbeat[broker_id]
        
        # Elect new leaders for partitions where this broker was leader
        for key, assignment in self.partition_assignments.items():
            if assignment.leader == broker_id:
                topic, partition = key
                await self._elect_partition_leader(topic, partition)
            
            # Remove from ISR
            if broker_id in assignment.isr:
                assignment.isr.discard(broker_id)
        
        # Propose to Raft
        if self.state.is_active:
            await self.raft_node.propose(
                "broker_failure",
                {"broker_id": broker_id},
            )
    
    # Raft Integration
    
    async def _apply_raft_command(self, entry: LogEntry) -> None:
        """
        Apply committed Raft command.
        
        Args:
            entry: Raft log entry
        """
        command = entry.command
        data = entry.data or {}
        
        logger.debug(
            "Applying Raft command",
            command=command,
            data=data,
        )
        
        if command == "controller_change":
            await self._handle_controller_change(data)
        elif command == "create_partition":
            await self._handle_create_partition(data)
        elif command == "change_partition_leader":
            await self._handle_change_partition_leader(data)
        elif command == "register_broker":
            await self._handle_register_broker(data)
        elif command == "broker_failure":
            await self._handle_broker_failure_command(data)
    
    async def _handle_controller_change(self, data: Dict) -> None:
        """Handle controller change command."""
        controller_id = data.get("controller_id")
        epoch = data.get("epoch")
        
        # Update controller state
        if epoch and epoch > self.state.controller_epoch:
            self.state.controller_id = controller_id
            self.state.controller_epoch = epoch
            
            logger.info(
                "Controller changed",
                controller_id=controller_id,
                epoch=epoch,
            )
    
    async def _handle_create_partition(self, data: Dict) -> None:
        """Handle create partition command."""
        topic = data.get("topic")
        partition = data.get("partition")
        replicas = data.get("replicas", [])
        leader = data.get("leader")
        
        if topic and partition is not None:
            key = (topic, partition)
            
            if key not in self.partition_assignments:
                assignment = PartitionAssignment(
                    topic=topic,
                    partition=partition,
                    replicas=replicas,
                    leader=leader,
                    isr={leader} if leader else set(),
                )
                
                self.partition_assignments[key] = assignment
    
    async def _handle_change_partition_leader(self, data: Dict) -> None:
        """Handle change partition leader command."""
        topic = data.get("topic")
        partition = data.get("partition")
        leader = data.get("leader")
        
        if topic and partition is not None and leader:
            key = (topic, partition)
            
            if key in self.partition_assignments:
                assignment = self.partition_assignments[key]
                assignment.leader = leader
                assignment.isr = {leader}
    
    async def _handle_register_broker(self, data: Dict) -> None:
        """Handle register broker command."""
        broker_id = data.get("broker_id")
        
        if broker_id:
            self.broker_last_heartbeat[broker_id] = int(time.time() * 1000)
    
    async def _handle_broker_failure_command(self, data: Dict) -> None:
        """Handle broker failure command."""
        broker_id = data.get("broker_id")
        
        if broker_id and broker_id in self.broker_last_heartbeat:
            del self.broker_last_heartbeat[broker_id]
    
    # Query Methods
    
    def get_partition_leader(self, topic: str, partition: int) -> Optional[int]:
        """
        Get partition leader.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            Leader broker ID or None
        """
        key = (topic, partition)
        
        if key in self.partition_assignments:
            return self.partition_assignments[key].leader
        
        return None
    
    def get_partition_isr(self, topic: str, partition: int) -> Set[int]:
        """
        Get partition ISR.
        
        Args:
            topic: Topic name
            partition: Partition number
        
        Returns:
            Set of ISR broker IDs
        """
        key = (topic, partition)
        
        if key in self.partition_assignments:
            return self.partition_assignments[key].isr.copy()
        
        return set()
    
    def is_controller(self) -> bool:
        """
        Check if this broker is the active controller.
        
        Returns:
            True if controller
        """
        return self.state.is_active
    
    def get_controller_epoch(self) -> int:
        """
        Get current controller epoch.
        
        Returns:
            Controller epoch
        """
        return self.state.controller_epoch
