"""Tests for producer retry logic."""

import pytest

from distributedlog.producer.retry import (
    CircuitBreaker,
    NonRetryableError,
    RetryableError,
    RetryConfig,
    RetryManager,
    is_retryable_error,
)


class TestRetryManager:
    """Test RetryManager."""
    
    def test_success_no_retry(self):
        """Test successful operation needs no retry."""
        manager = RetryManager()
        
        call_count = 0
        
        def operation():
            nonlocal call_count
            call_count += 1
            return "success"
        
        result = manager.execute_with_retry(operation)
        
        assert result == "success"
        assert call_count == 1
    
    def test_retry_on_failure(self):
        """Test retry on retryable failure."""
        config = RetryConfig(max_retries=3, retry_backoff_ms=10)
        manager = RetryManager(config)
        
        call_count = 0
        
        def operation():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RetryableError("Temporary failure")
            return "success"
        
        result = manager.execute_with_retry(operation)
        
        assert result == "success"
        assert call_count == 3
    
    def test_no_retry_on_non_retryable(self):
        """Test no retry on non-retryable error."""
        manager = RetryManager()
        
        call_count = 0
        
        def operation():
            nonlocal call_count
            call_count += 1
            raise NonRetryableError("Permanent failure")
        
        with pytest.raises(NonRetryableError):
            manager.execute_with_retry(operation)
        
        assert call_count == 1
    
    def test_exhausted_retries(self):
        """Test all retries exhausted."""
        config = RetryConfig(max_retries=2, retry_backoff_ms=1)
        manager = RetryManager(config)
        
        call_count = 0
        
        def operation():
            nonlocal call_count
            call_count += 1
            raise RetryableError("Always fails")
        
        with pytest.raises(RetryableError):
            manager.execute_with_retry(operation)
        
        assert call_count == 3


class TestCircuitBreaker:
    """Test CircuitBreaker."""
    
    def test_closed_state_allows_calls(self):
        """Test closed state allows calls."""
        breaker = CircuitBreaker(failure_threshold=3)
        
        result = breaker.call(lambda: "success")
        assert result == "success"
        assert breaker.get_state() == "CLOSED"
    
    def test_opens_after_failures(self):
        """Test opens after threshold failures."""
        breaker = CircuitBreaker(failure_threshold=3)
        
        for _ in range(3):
            try:
                breaker.call(lambda: (_ for _ in ()).throw(Exception("fail")))
            except:
                pass
        
        assert breaker.get_state() == "OPEN"
    
    def test_open_rejects_calls(self):
        """Test open state rejects calls."""
        breaker = CircuitBreaker(failure_threshold=1, recovery_timeout_ms=10000)
        
        try:
            breaker.call(lambda: (_ for _ in ()).throw(Exception("fail")))
        except:
            pass
        
        with pytest.raises(NonRetryableError, match="Circuit breaker is OPEN"):
            breaker.call(lambda: "success")
    
    def test_reset(self):
        """Test manual reset."""
        breaker = CircuitBreaker(failure_threshold=1)
        
        try:
            breaker.call(lambda: (_ for _ in ()).throw(Exception("fail")))
        except:
            pass
        
        breaker.reset()
        assert breaker.get_state() == "CLOSED"


class TestErrorClassification:
    """Test error classification."""
    
    def test_retryable_error(self):
        """Test retryable error classification."""
        assert is_retryable_error(RetryableError("test"))
        assert is_retryable_error(Exception("timeout"))
        assert is_retryable_error(Exception("connection refused"))
    
    def test_non_retryable_error(self):
        """Test non-retryable error classification."""
        assert not is_retryable_error(NonRetryableError("test"))
        assert not is_retryable_error(Exception("unknown error"))
