"""Tests for offset reset strategies."""

import pytest

from distributedlog.consumer.offset import TopicPartition
from distributedlog.consumer.offset.reset_strategy import (
    OffsetResetHandler,
    OffsetResetStrategy,
    get_beginning_offset,
    get_end_offset,
)


class TestOffsetResetStrategy:
    """Test OffsetResetStrategy enum."""
    
    def test_strategy_values(self):
        """Test strategy enum values."""
        assert OffsetResetStrategy.EARLIEST == "earliest"
        assert OffsetResetStrategy.LATEST == "latest"
        assert OffsetResetStrategy.NONE == "none"


class TestOffsetResetHandler:
    """Test OffsetResetHandler."""
    
    def test_create_handler_earliest(self):
        """Test creating handler with earliest strategy."""
        handler = OffsetResetHandler(strategy="earliest")
        
        assert handler.strategy == OffsetResetStrategy.EARLIEST
    
    def test_create_handler_latest(self):
        """Test creating handler with latest strategy."""
        handler = OffsetResetHandler(strategy="latest")
        
        assert handler.strategy == OffsetResetStrategy.LATEST
    
    def test_reset_to_earliest(self):
        """Test resetting to earliest offset."""
        handler = OffsetResetHandler(strategy="earliest")
        tp = TopicPartition("test-topic", 0)
        
        offset = handler.reset_offset(tp)
        
        assert offset == 0
    
    def test_reset_to_latest(self):
        """Test resetting to latest offset."""
        handler = OffsetResetHandler(strategy="latest")
        tp = TopicPartition("test-topic", 0)
        
        offset = handler.reset_offset(tp, log_end_offset=1000)
        
        assert offset == 1000
    
    def test_reset_to_latest_without_log_end(self):
        """Test resetting to latest without log end offset."""
        handler = OffsetResetHandler(strategy="latest")
        tp = TopicPartition("test-topic", 0)
        
        offset = handler.reset_offset(tp)
        
        assert offset == -1  # Placeholder
    
    def test_reset_strategy_none(self):
        """Test strategy 'none' raises exception."""
        handler = OffsetResetHandler(strategy="none")
        tp = TopicPartition("test-topic", 0)
        
        with pytest.raises(ValueError, match="No committed offset"):
            handler.reset_offset(tp)
    
    def test_invalid_strategy(self):
        """Test invalid strategy raises exception."""
        with pytest.raises(ValueError):
            OffsetResetHandler(strategy="invalid")
    
    def test_should_reset_with_no_offset(self):
        """Test should reset when no committed offset."""
        handler = OffsetResetHandler()
        tp = TopicPartition("test-topic", 0)
        
        assert handler.should_reset(tp, committed_offset=None)
    
    def test_should_not_reset_with_offset(self):
        """Test should not reset when offset exists."""
        handler = OffsetResetHandler()
        tp = TopicPartition("test-topic", 0)
        
        assert not handler.should_reset(tp, committed_offset=100)


class TestOffsetHelpers:
    """Test offset helper functions."""
    
    def test_get_beginning_offset(self):
        """Test getting beginning offset."""
        tp = TopicPartition("test-topic", 5)
        
        offset = get_beginning_offset(tp)
        
        assert offset == 0
    
    def test_get_end_offset(self):
        """Test getting end offset."""
        tp = TopicPartition("test-topic", 5)
        
        offset = get_end_offset(tp)
        
        assert offset == -1  # Placeholder
