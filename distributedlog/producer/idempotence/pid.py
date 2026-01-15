"""
Producer ID (PID) management for idempotence.

Assigns unique producer IDs and manages their lifecycle.
"""

import time
from dataclasses import dataclass
from typing import Dict, Optional

from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ProducerIdInfo:
    """
    Information about a producer ID.
    
    Attributes:
        producer_id: Unique producer ID (PID)
        producer_epoch: Producer epoch (for fencing)
        assigned_time: Timestamp when assigned (ms)
        last_used_time: Last time this PID was used (ms)
    """
    producer_id: int
    producer_epoch: int
    assigned_time: int
    last_used_time: int
    
    def is_expired(self, timeout_ms: int) -> bool:
        """
        Check if PID is expired.
        
        Args:
            timeout_ms: Timeout in milliseconds
        
        Returns:
            True if expired
        """
        current_time = int(time.time() * 1000)
        return current_time - self.last_used_time > timeout_ms
    
    def update_last_used(self) -> None:
        """Update last used timestamp."""
        self.last_used_time = int(time.time() * 1000)


class ProducerIdManager:
    """
    Manages producer ID assignment.
    
    Assigns unique PIDs to producers for idempotent message delivery.
    """
    
    def __init__(
        self,
        start_id: int = 0,
        pid_timeout_ms: int = 300000,  # 5 minutes
    ):
        """
        Initialize producer ID manager.
        
        Args:
            start_id: Starting producer ID
            pid_timeout_ms: PID expiration timeout
        """
        self._next_id = start_id
        self._pid_timeout_ms = pid_timeout_ms
        
        # Active PIDs: producer_id -> ProducerIdInfo
        self._active_pids: Dict[int, ProducerIdInfo] = {}
        
        # Client ID to PID mapping (for reuse)
        self._client_to_pid: Dict[str, int] = {}
        
        logger.info(
            "ProducerIdManager initialized",
            start_id=start_id,
            timeout_ms=pid_timeout_ms,
        )
    
    def assign_producer_id(
        self,
        client_id: Optional[str] = None,
    ) -> ProducerIdInfo:
        """
        Assign a new producer ID.
        
        Args:
            client_id: Optional client ID for reuse
        
        Returns:
            Producer ID info
        """
        # Check if client already has a PID
        if client_id and client_id in self._client_to_pid:
            pid = self._client_to_pid[client_id]
            
            if pid in self._active_pids:
                pid_info = self._active_pids[pid]
                
                # Reuse if not expired
                if not pid_info.is_expired(self._pid_timeout_ms):
                    pid_info.update_last_used()
                    
                    logger.info(
                        "Reused producer ID",
                        client_id=client_id,
                        producer_id=pid,
                    )
                    
                    return pid_info
        
        # Allocate new PID
        producer_id = self._allocate_next_id()
        current_time = int(time.time() * 1000)
        
        pid_info = ProducerIdInfo(
            producer_id=producer_id,
            producer_epoch=0,
            assigned_time=current_time,
            last_used_time=current_time,
        )
        
        self._active_pids[producer_id] = pid_info
        
        if client_id:
            self._client_to_pid[client_id] = producer_id
        
        logger.info(
            "Assigned producer ID",
            client_id=client_id,
            producer_id=producer_id,
        )
        
        return pid_info
    
    def _allocate_next_id(self) -> int:
        """
        Allocate next producer ID.
        
        Handles wrap-around for overflow.
        
        Returns:
            Next producer ID
        """
        producer_id = self._next_id
        self._next_id += 1
        
        # Handle overflow (wrap around at max int)
        if self._next_id >= 2**31:
            self._next_id = 0
            
            logger.warning(
                "Producer ID overflow, wrapping around",
                next_id=self._next_id,
            )
        
        return producer_id
    
    def get_producer_id_info(
        self,
        producer_id: int,
    ) -> Optional[ProducerIdInfo]:
        """
        Get producer ID info.
        
        Args:
            producer_id: Producer ID
        
        Returns:
            Producer ID info or None
        """
        return self._active_pids.get(producer_id)
    
    def update_last_used(
        self,
        producer_id: int,
    ) -> None:
        """
        Update last used time for producer ID.
        
        Args:
            producer_id: Producer ID
        """
        if producer_id in self._active_pids:
            self._active_pids[producer_id].update_last_used()
    
    def expire_old_pids(self) -> int:
        """
        Expire old producer IDs.
        
        Returns:
            Number of expired PIDs
        """
        expired = []
        
        for pid, info in self._active_pids.items():
            if info.is_expired(self._pid_timeout_ms):
                expired.append(pid)
        
        # Remove expired PIDs
        for pid in expired:
            del self._active_pids[pid]
            
            # Also remove from client mapping
            for client_id, client_pid in list(self._client_to_pid.items()):
                if client_pid == pid:
                    del self._client_to_pid[client_id]
        
        if expired:
            logger.info(
                "Expired producer IDs",
                count=len(expired),
                pids=expired,
            )
        
        return len(expired)
    
    def get_stats(self) -> Dict:
        """
        Get manager statistics.
        
        Returns:
            Statistics dict
        """
        return {
            "active_pids": len(self._active_pids),
            "next_id": self._next_id,
            "timeout_ms": self._pid_timeout_ms,
        }
