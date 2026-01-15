"""Tests for rebalance offset handler."""

import tempfile
from pathlib import Path

import pytest

from distributedlog.consumer.offset import TopicPartition
from distributedlog.consumer.offset.offset_topic import OffsetTopic
from distributedlog.consumer.offset.rebalance_handler import RebalanceOffsetHandler


class TestRebalanceOffsetHandler:
    """Test RebalanceOffsetHandler."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)
    
    @pytest.fixture
    def offset_topic(self, temp_dir):
        """Create offset topic."""
        topic = OffsetTopic(data_dir=temp_dir, num_partitions=3)
        yield topic
        topic.close()
    
    @pytest.fixture
    def handler(self, offset_topic):
        """Create rebalance handler."""
        return RebalanceOffsetHandler(
            offset_topic=offset_topic,
            reset_strategy="latest",
        )
    
    @pytest.mark.asyncio
    async def test_prepare_for_rebalance(self, handler, offset_topic):
        """Test preparing for rebalance."""
        tp1 = TopicPartition("topic1", 0)
        tp2 = TopicPartition("topic1", 1)
        
        positions = {
            tp1: 100,
            tp2: 200,
        }
        
        await handler.prepare_for_rebalance("test-group", positions)
        
        offset1 = offset_topic.fetch_offset("test-group", tp1)
        offset2 = offset_topic.fetch_offset("test-group", tp2)
        
        assert offset1.offset == 100
        assert offset2.offset == 200
    
    @pytest.mark.asyncio
    async def test_fetch_committed_offsets(self, handler, offset_topic):
        """Test fetching committed offsets after rebalance."""
        tp1 = TopicPartition("topic1", 0)
        tp2 = TopicPartition("topic1", 1)
        
        offset_topic.commit_offset("test-group", tp1, 150)
        offset_topic.commit_offset("test-group", tp2, 250)
        
        assigned = {tp1, tp2}
        offsets = await handler.fetch_offsets_after_rebalance(
            "test-group",
            assigned,
        )
        
        assert offsets[tp1] == 150
        assert offsets[tp2] == 250
    
    @pytest.mark.asyncio
    async def test_fetch_with_reset_strategy(self, handler):
        """Test fetching offsets with reset strategy."""
        tp1 = TopicPartition("topic1", 0)
        tp2 = TopicPartition("topic1", 1)
        
        assigned = {tp1, tp2}
        log_end_offsets = {tp1: 1000, tp2: 2000}
        
        offsets = await handler.fetch_offsets_after_rebalance(
            "test-group",
            assigned,
            log_end_offsets,
        )
        
        assert offsets[tp1] == 1000  # Reset to latest
        assert offsets[tp2] == 2000  # Reset to latest
    
    @pytest.mark.asyncio
    async def test_earliest_reset_strategy(self, offset_topic):
        """Test earliest reset strategy."""
        handler = RebalanceOffsetHandler(
            offset_topic=offset_topic,
            reset_strategy="earliest",
        )
        
        tp = TopicPartition("topic1", 0)
        assigned = {tp}
        
        offsets = await handler.fetch_offsets_after_rebalance(
            "test-group",
            assigned,
        )
        
        assert offsets[tp] == 0  # Reset to earliest
    
    @pytest.mark.asyncio
    async def test_none_reset_strategy(self, offset_topic):
        """Test 'none' reset strategy raises error."""
        handler = RebalanceOffsetHandler(
            offset_topic=offset_topic,
            reset_strategy="none",
        )
        
        tp = TopicPartition("topic1", 0)
        assigned = {tp}
        
        with pytest.raises(ValueError, match="No committed offset"):
            await handler.fetch_offsets_after_rebalance(
                "test-group",
                assigned,
            )
    
    @pytest.mark.asyncio
    async def test_offset_commit_during_rebalance(self, handler):
        """Test handling offset commit during rebalance."""
        tp = TopicPartition("topic1", 0)
        
        success = await handler.handle_offset_commit_during_rebalance(
            "test-group",
            tp,
            500,
        )
        
        assert success
    
    def test_get_reset_strategy(self, handler):
        """Test getting reset strategy."""
        strategy = handler.get_reset_strategy()
        
        assert strategy == "latest"
