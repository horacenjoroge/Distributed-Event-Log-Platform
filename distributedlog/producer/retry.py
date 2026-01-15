"""
Retry logic with exponential backoff for producer.

Handles transient failures with intelligent retry strategies.
"""

import random
import time
from dataclasses import dataclass
from typing import Callable, Optional, TypeVar

from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)

T = TypeVar('T')


@dataclass
class RetryConfig:
    """
    Configuration for retry behavior.
    
    Attributes:
        max_retries: Maximum number of retry attempts
        retry_backoff_ms: Initial backoff in milliseconds
        retry_backoff_max_ms: Maximum backoff in milliseconds
        retry_jitter_ms: Random jitter to add to backoff
    """
    max_retries: int = 3
    retry_backoff_ms: int = 100
    retry_backoff_max_ms: int = 32000
    retry_jitter_ms: int = 20


class RetryableError(Exception):
    """Exception that should trigger retry."""
    pass


class NonRetryableError(Exception):
    """Exception that should not be retried."""
    pass


class RetryManager:
    """
    Manages retry logic with exponential backoff.
    
    Implements:
    - Exponential backoff: delay doubles each retry
    - Maximum backoff: caps delay at maximum
    - Random jitter: prevents thundering herd
    """
    
    def __init__(self, config: Optional[RetryConfig] = None):
        """
        Initialize retry manager.
        
        Args:
            config: Retry configuration
        """
        self.config = config or RetryConfig()
        
        logger.info(
            "Initialized retry manager",
            max_retries=self.config.max_retries,
            retry_backoff_ms=self.config.retry_backoff_ms,
        )
    
    def execute_with_retry(
        self,
        operation: Callable[[], T],
        operation_name: str = "operation",
    ) -> T:
        """
        Execute operation with retry logic.
        
        Args:
            operation: Callable to execute
            operation_name: Name for logging
        
        Returns:
            Result from operation
        
        Raises:
            Exception: If all retries exhausted
        """
        last_exception = None
        
        for attempt in range(self.config.max_retries + 1):
            try:
                result = operation()
                
                if attempt > 0:
                    logger.info(
                        f"{operation_name} succeeded after retry",
                        attempt=attempt,
                    )
                
                return result
            
            except NonRetryableError as e:
                logger.error(
                    f"{operation_name} failed with non-retryable error",
                    error=str(e),
                )
                raise
            
            except RetryableError as e:
                last_exception = e
                
                if attempt < self.config.max_retries:
                    backoff_ms = self._calculate_backoff(attempt)
                    
                    logger.warning(
                        f"{operation_name} failed, retrying",
                        attempt=attempt,
                        backoff_ms=backoff_ms,
                        error=str(e),
                    )
                    
                    time.sleep(backoff_ms / 1000.0)
                else:
                    logger.error(
                        f"{operation_name} failed after all retries",
                        attempts=attempt + 1,
                        error=str(e),
                    )
            
            except Exception as e:
                last_exception = e
                
                if attempt < self.config.max_retries:
                    backoff_ms = self._calculate_backoff(attempt)
                    
                    logger.warning(
                        f"{operation_name} failed, retrying",
                        attempt=attempt,
                        backoff_ms=backoff_ms,
                        error=str(e),
                    )
                    
                    time.sleep(backoff_ms / 1000.0)
                else:
                    logger.error(
                        f"{operation_name} failed after all retries",
                        attempts=attempt + 1,
                        error=str(e),
                    )
        
        raise last_exception
    
    def _calculate_backoff(self, attempt: int) -> int:
        """
        Calculate backoff delay with exponential growth and jitter.
        
        Formula: min(base * 2^attempt + jitter, max)
        
        Args:
            attempt: Current attempt number (0-indexed)
        
        Returns:
            Backoff delay in milliseconds
        """
        exponential_backoff = self.config.retry_backoff_ms * (2 ** attempt)
        
        backoff = min(exponential_backoff, self.config.retry_backoff_max_ms)
        
        jitter = random.randint(0, self.config.retry_jitter_ms)
        
        return backoff + jitter


class CircuitBreaker:
    """
    Circuit breaker to prevent cascading failures.
    
    States:
    - CLOSED: Normal operation
    - OPEN: Failing, reject requests immediately
    - HALF_OPEN: Testing if service recovered
    """
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout_ms: int = 60000,
        success_threshold: int = 2,
    ):
        """
        Initialize circuit breaker.
        
        Args:
            failure_threshold: Failures before opening circuit
            recovery_timeout_ms: Time before trying again
            success_threshold: Successes needed to close circuit
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout_ms = recovery_timeout_ms
        self.success_threshold = success_threshold
        
        self._failure_count = 0
        self._success_count = 0
        self._state = "CLOSED"
        self._last_failure_time = 0
        
        logger.info(
            "Initialized circuit breaker",
            failure_threshold=failure_threshold,
            recovery_timeout_ms=recovery_timeout_ms,
        )
    
    def call(self, operation: Callable[[], T]) -> T:
        """
        Execute operation through circuit breaker.
        
        Args:
            operation: Operation to execute
        
        Returns:
            Result from operation
        
        Raises:
            Exception: If circuit is open or operation fails
        """
        if self._state == "OPEN":
            if self._should_attempt_reset():
                self._state = "HALF_OPEN"
                logger.info("Circuit breaker entering HALF_OPEN state")
            else:
                raise NonRetryableError("Circuit breaker is OPEN")
        
        try:
            result = operation()
            self._on_success()
            return result
        
        except Exception as e:
            self._on_failure()
            raise
    
    def _on_success(self) -> None:
        """Handle successful operation."""
        self._failure_count = 0
        
        if self._state == "HALF_OPEN":
            self._success_count += 1
            
            if self._success_count >= self.success_threshold:
                self._state = "CLOSED"
                self._success_count = 0
                logger.info("Circuit breaker closed")
    
    def _on_failure(self) -> None:
        """Handle failed operation."""
        self._failure_count += 1
        self._last_failure_time = int(time.time() * 1000)
        
        if self._failure_count >= self.failure_threshold:
            self._state = "OPEN"
            logger.warning(
                "Circuit breaker opened",
                failure_count=self._failure_count,
            )
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time passed to attempt reset."""
        current_time = int(time.time() * 1000)
        time_since_failure = current_time - self._last_failure_time
        return time_since_failure >= self.recovery_timeout_ms
    
    def get_state(self) -> str:
        """Get current circuit breaker state."""
        return self._state
    
    def reset(self) -> None:
        """Manually reset circuit breaker."""
        self._failure_count = 0
        self._success_count = 0
        self._state = "CLOSED"
        logger.info("Circuit breaker manually reset")


def is_retryable_error(error: Exception) -> bool:
    """
    Determine if error is retryable.
    
    Args:
        error: Exception to check
    
    Returns:
        True if error should be retried
    """
    if isinstance(error, RetryableError):
        return True
    
    if isinstance(error, NonRetryableError):
        return False
    
    error_str = str(error).lower()
    
    retryable_patterns = [
        "timeout",
        "connection refused",
        "connection reset",
        "broken pipe",
        "temporarily unavailable",
        "service unavailable",
    ]
    
    for pattern in retryable_patterns:
        if pattern in error_str:
            return True
    
    return False
