"""
Structured logging infrastructure using structlog.

This module provides centralized logging configuration with:
- JSON formatting for production
- Console formatting for development
- Context variables for request tracing
- Log level management
"""

import logging
import sys
from typing import Any

import structlog
from structlog.types import EventDict, Processor


def add_app_context(logger: Any, method_name: str, event_dict: EventDict) -> EventDict:
    """Add application context to log entries."""
    event_dict["app"] = "distributedlog"
    return event_dict


def configure_logging(
    log_level: str = "INFO",
    log_format: str = "json",
    log_output: str = "stdout",
) -> None:
    """
    Configure structured logging for the application.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Output format (json or console)
        log_output: Output destination (stdout, stderr, or file path)
    """
    # Configure standard logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout if log_output == "stdout" else sys.stderr,
        level=getattr(logging, log_level.upper()),
    )
    
    # Common processors for all formats
    shared_processors: list[Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        add_app_context,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
    ]
    
    # Format-specific configuration
    if log_format == "json":
        processors = shared_processors + [
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ]
    else:  # console format
        processors = shared_processors + [
            structlog.processors.format_exc_info,
            structlog.dev.ConsoleRenderer(colors=True),
        ]
    
    # Configure structlog
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """
    Get a configured logger instance.
    
    Args:
        name: Logger name (typically __name__)
    
    Returns:
        Configured structlog logger
    """
    return structlog.get_logger(name)
