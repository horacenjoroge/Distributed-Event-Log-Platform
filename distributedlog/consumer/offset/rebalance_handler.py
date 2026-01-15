"""
Offset handling during consumer group rebalancing.

Handles offset commit/fetch operations during the rebalance protocol.
"""

import asyncio
from typing import Dict, Optional, Set

from distributedlog.consumer.offset import (
    OffsetAndMetadata,
    TopicPartition,
)
from distributedlog.consumer.offset.offset_topic import OffsetTopic
from distributedlog.consumer.offset.reset_strategy import (
    OffsetResetHandler,
    OffsetResetStrategy,
)
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


class RebalanceOffsetHandler:
    """
    Handles offset operations during rebalancing.
    
    Responsibilities:
    - Commit offsets before rebalance
    - Fetch offsets after partition assignment
    - Apply offset reset strategy when needed
    - Handle edge cases (commit during rebalance, etc)
    """
    
    def __init__(
        self,
        offset_topic: OffsetTopic,
        reset_strategy: str = OffsetResetStrategy.LATEST,
    ):
        """
        Initialize rebalance offset handler.
        
        Args:
            offset_topic: Internal offset topic
            reset_strategy: Offset reset strategy
        """
        self._offset_topic = offset_topic
        self._reset_handler = OffsetResetHandler(reset_strategy)
        self._lock = asyncio.Lock()
        
        logger.info(
            "RebalanceOffsetHandler initialized",
            reset_strategy=reset_strategy,
        )
    
    async def prepare_for_rebalance(
        self,
        group_id: str,
        current_positions: Dict[TopicPartition, int],
    ) -> None:
        """
        Prepare for rebalancing by committing current positions.
        
        Called before rebalance starts to ensure offsets are persisted.
        
        Args:
            group_id: Consumer group ID
            current_positions: Current read positions per partition
        """
        async with self._lock:
            for tp, offset in current_positions.items():
                self._offset_topic.commit_offset(
                    group_id=group_id,
                    topic_partition=tp,
                    offset=offset,
                )
            
            logger.info(
                "Committed offsets before rebalance",
                group_id=group_id,
                count=len(current_positions),
            )
    
    async def fetch_offsets_after_rebalance(
        self,
        group_id: str,
        assigned_partitions: Set[TopicPartition],
        log_end_offsets: Optional[Dict[TopicPartition, int]] = None,
    ) -> Dict[TopicPartition, int]:
        """
        Fetch offsets after rebalance completes.
        
        For each assigned partition:
        1. Try to fetch committed offset
        2. If no committed offset, apply reset strategy
        3. Return map of partition -> start offset
        
        Args:
            group_id: Consumer group ID
            assigned_partitions: Newly assigned partitions
            log_end_offsets: Current log end offsets (optional)
        
        Returns:
            Map of partition to starting offset
        """
        async with self._lock:
            starting_offsets: Dict[TopicPartition, int] = {}
            
            for tp in assigned_partitions:
                offset_metadata = self._offset_topic.fetch_offset(
                    group_id=group_id,
                    topic_partition=tp,
                )
                
                if offset_metadata:
                    starting_offsets[tp] = offset_metadata.offset
                    
                    logger.debug(
                        "Fetched committed offset",
                        group_id=group_id,
                        topic=tp.topic,
                        partition=tp.partition,
                        offset=offset_metadata.offset,
                    )
                else:
                    log_end_offset = None
                    if log_end_offsets:
                        log_end_offset = log_end_offsets.get(tp)
                    
                    try:
                        reset_offset = self._reset_handler.reset_offset(
                            topic_partition=tp,
                            log_end_offset=log_end_offset,
                        )
                        starting_offsets[tp] = reset_offset
                        
                        logger.info(
                            "Applied offset reset",
                            group_id=group_id,
                            topic=tp.topic,
                            partition=tp.partition,
                            offset=reset_offset,
                            strategy=self._reset_handler.strategy,
                        )
                    except ValueError as e:
                        logger.error(
                            "Offset reset failed",
                            group_id=group_id,
                            topic=tp.topic,
                            partition=tp.partition,
                            error=str(e),
                        )
                        raise
            
            logger.info(
                "Fetched offsets after rebalance",
                group_id=group_id,
                count=len(starting_offsets),
            )
            
            return starting_offsets
    
    async def handle_offset_commit_during_rebalance(
        self,
        group_id: str,
        topic_partition: TopicPartition,
        offset: int,
    ) -> bool:
        """
        Handle offset commit that happens during rebalancing.
        
        This is an edge case where a consumer tries to commit while
        rebalance is in progress. We allow the commit but log a warning.
        
        Args:
            group_id: Consumer group ID
            topic_partition: Topic-partition
            offset: Offset to commit
        
        Returns:
            True if commit succeeded
        """
        async with self._lock:
            try:
                self._offset_topic.commit_offset(
                    group_id=group_id,
                    topic_partition=topic_partition,
                    offset=offset,
                )
                
                logger.warning(
                    "Committed offset during rebalance",
                    group_id=group_id,
                    topic=topic_partition.topic,
                    partition=topic_partition.partition,
                    offset=offset,
                )
                
                return True
            
            except Exception as e:
                logger.error(
                    "Offset commit during rebalance failed",
                    group_id=group_id,
                    topic=topic_partition.topic,
                    partition=topic_partition.partition,
                    error=str(e),
                )
                return False
    
    def get_reset_strategy(self) -> str:
        """Get configured reset strategy."""
        return self._reset_handler.strategy.value
