#!/usr/bin/env python3
"""
Consumer example demonstrating various features.
"""

import json
import argparse

from distributedlog.consumer.consumer import Consumer


def main():
    parser = argparse.ArgumentParser(description='DistributedLog Consumer Example')
    parser.add_argument('--broker', default='localhost:9092', help='Broker address')
    parser.add_argument('--topic', default='test-topic', help='Topic name')
    parser.add_argument('--group', default='example-group', help='Consumer group ID')
    args = parser.parse_args()
    
    print(f"Consuming from topic '{args.topic}' in group '{args.group}'")
    print("Press Ctrl+C to stop...\n")
    
    # Create consumer
    consumer = Consumer(
        bootstrap_servers=[args.broker],
        group_id=args.group,
        client_id='example-consumer'
    )
    
    # Subscribe
    consumer.subscribe([args.topic])
    
    try:
        message_count = 0
        
        while True:
            # Poll for messages
            messages = consumer.poll(timeout_ms=1000)
            
            for message in messages:
                # Deserialize
                try:
                    data = json.loads(message.value.decode('utf-8'))
                    print(f"[Offset {message.offset}] {data}")
                    message_count += 1
                except Exception as e:
                    print(f"Error decoding message: {e}")
            
            # Commit offsets periodically
            if message_count > 0 and message_count % 10 == 0:
                consumer.commit()
                print(f"  (committed offsets after {message_count} messages)")
    
    except KeyboardInterrupt:
        print(f"\n\nConsumed {message_count} messages total")
    
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
