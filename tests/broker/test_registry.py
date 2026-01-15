"""Tests for broker registry."""

import pytest

from distributedlog.broker.metadata import BrokerState
from distributedlog.broker.registry import BrokerRegistry


class TestBrokerRegistry:
    """Test BrokerRegistry."""
    
    @pytest.fixture
    def registry(self):
        """Create broker registry."""
        return BrokerRegistry(cluster_id="test-cluster")
    
    @pytest.mark.asyncio
    async def test_register_broker(self, registry):
        """Test broker registration."""
        broker = await registry.register_broker(
            broker_id=1,
            host="localhost",
            port=9092,
        )
        
        assert broker.broker_id == 1
        assert broker.host == "localhost"
        assert broker.port == 9092
    
    @pytest.mark.asyncio
    async def test_first_broker_becomes_controller(self, registry):
        """Test first broker becomes controller."""
        await registry.register_broker(
            broker_id=1,
            host="localhost",
            port=9092,
        )
        
        controller = await registry.get_controller()
        
        assert controller is not None
        assert controller.broker_id == 1
    
    @pytest.mark.asyncio
    async def test_deregister_broker(self, registry):
        """Test broker deregistration."""
        await registry.register_broker(
            broker_id=1,
            host="localhost",
            port=9092,
        )
        
        await registry.deregister_broker(1)
        
        broker = await registry.get_broker(1)
        assert broker is None
    
    @pytest.mark.asyncio
    async def test_update_heartbeat(self, registry):
        """Test heartbeat update."""
        await registry.register_broker(
            broker_id=1,
            host="localhost",
            port=9092,
        )
        
        result = await registry.update_heartbeat(1)
        
        assert result is True
        
        broker = await registry.get_broker(1)
        assert broker.state == BrokerState.RUNNING
    
    @pytest.mark.asyncio
    async def test_heartbeat_nonexistent_broker(self, registry):
        """Test heartbeat for nonexistent broker."""
        result = await registry.update_heartbeat(999)
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_mark_broker_failed(self, registry):
        """Test marking broker as failed."""
        await registry.register_broker(
            broker_id=1,
            host="localhost",
            port=9092,
        )
        
        await registry.mark_broker_failed(1)
        
        broker = await registry.get_broker(1)
        assert broker.state == BrokerState.FAILED
    
    @pytest.mark.asyncio
    async def test_get_all_brokers(self, registry):
        """Test getting all brokers."""
        await registry.register_broker(
            broker_id=1,
            host="localhost",
            port=9092,
        )
        await registry.register_broker(
            broker_id=2,
            host="localhost",
            port=9093,
        )
        
        brokers = await registry.get_all_brokers()
        
        assert len(brokers) == 2
    
    @pytest.mark.asyncio
    async def test_get_live_brokers(self, registry):
        """Test getting live brokers."""
        await registry.register_broker(
            broker_id=1,
            host="localhost",
            port=9092,
        )
        await registry.update_heartbeat(1)  # Mark as running
        
        await registry.register_broker(
            broker_id=2,
            host="localhost",
            port=9093,
        )
        await registry.mark_broker_failed(2)  # Mark as failed
        
        live_brokers = await registry.get_live_brokers()
        
        assert len(live_brokers) == 1
        assert live_brokers[0].broker_id == 1
    
    @pytest.mark.asyncio
    async def test_controller_reelection_on_failure(self, registry):
        """Test controller re-election on failure."""
        await registry.register_broker(
            broker_id=1,
            host="localhost",
            port=9092,
        )
        await registry.update_heartbeat(1)
        
        await registry.register_broker(
            broker_id=2,
            host="localhost",
            port=9093,
        )
        await registry.update_heartbeat(2)
        
        controller = await registry.get_controller()
        assert controller.broker_id == 1
        
        await registry.mark_broker_failed(1)
        
        controller = await registry.get_controller()
        assert controller.broker_id == 2
    
    @pytest.mark.asyncio
    async def test_check_broker_health(self, registry):
        """Test broker health checking."""
        await registry.register_broker(
            broker_id=1,
            host="localhost",
            port=9092,
        )
        
        broker = await registry.get_broker(1)
        broker.last_heartbeat = 0  # Very old heartbeat
        
        await registry.check_broker_health()
        
        broker = await registry.get_broker(1)
        assert broker.state == BrokerState.FAILED
