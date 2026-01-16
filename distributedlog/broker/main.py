#!/usr/bin/env python3
"""
Main entry point for running a DistributedLog broker.

Usage:
    # Single broker
    python -m distributedlog.broker.main --broker-id 1 --port 9092
    
    # Multi-broker cluster
    python -m distributedlog.broker.main --broker-id 1 --port 9092 --peers broker2:9093,broker3:9094
"""

import argparse
import asyncio
import logging
import signal
import sys

from distributedlog.broker.async_broker import AsyncBroker, AsyncBrokerConfig
from distributedlog.utils.logging import get_logger

logger = get_logger(__name__)


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description='DistributedLog Broker - A high-performance distributed commit log'
    )
    
    parser.add_argument(
        '--broker-id',
        type=str,
        required=True,
        help='Unique broker identifier (e.g., broker-1)'
    )
    
    parser.add_argument(
        '--host',
        type=str,
        default='0.0.0.0',
        help='Host to bind to (default: 0.0.0.0)'
    )
    
    parser.add_argument(
        '--port',
        type=int,
        default=9092,
        help='Port to listen on (default: 9092)'
    )
    
    parser.add_argument(
        '--data-dir',
        type=str,
        default='./data',
        help='Data directory for log storage (default: ./data)'
    )
    
    parser.add_argument(
        '--max-connections',
        type=int,
        default=10000,
        help='Maximum concurrent connections (default: 10000)'
    )
    
    parser.add_argument(
        '--disk-threads',
        type=int,
        default=8,
        help='Thread pool size for disk I/O (default: 8)'
    )
    
    parser.add_argument(
        '--log-level',
        type=str,
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level (default: INFO)'
    )
    
    return parser.parse_args()


async def shutdown(broker, loop):
    """Graceful shutdown handler."""
    logger.info("Shutting down broker...")
    
    # Stop broker
    await broker.stop()
    
    # Cancel all tasks
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    
    await asyncio.gather(*tasks, return_exceptions=True)
    
    loop.stop()
    
    logger.info("Broker stopped")


def main():
    """Main entry point."""
    args = parse_args()
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    logger.info(
        "Starting DistributedLog Broker",
        broker_id=args.broker_id,
        host=args.host,
        port=args.port,
        data_dir=args.data_dir,
    )
    
    # Create broker config
    config = AsyncBrokerConfig(
        max_connections=args.max_connections,
        disk_io_threads=args.disk_threads,
    )
    
    # Create broker
    broker = AsyncBroker(
        broker_id=args.broker_id,
        host=args.host,
        port=args.port,
        config=config,
    )
    
    # Setup event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # Setup signal handlers
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(
            sig,
            lambda: asyncio.create_task(shutdown(broker, loop))
        )
    
    try:
        # Start broker
        loop.run_until_complete(broker.start())
        
        logger.info(
            "Broker started successfully",
            broker_id=args.broker_id,
            address=f"{args.host}:{args.port}",
        )
        
        # Run forever
        loop.run_forever()
    
    except KeyboardInterrupt:
        logger.info("Received interrupt signal")
    
    except Exception as e:
        logger.error("Broker error", error=str(e), exc_info=True)
        sys.exit(1)
    
    finally:
        loop.close()
        logger.info("Event loop closed")


if __name__ == '__main__':
    main()
