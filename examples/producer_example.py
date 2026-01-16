#!/usr/bin/env python3
"""
Producer example demonstrating various features.
"""

import json
import time
import argparse

from distributedlog.producer.producer import Producer


def main():
    parser = argparse.ArgumentParser(description='DistributedLog Producer Example')
    parser.add_argument('--broker', default='localhost:9092', help='Broker address')
    parser.add_argument('--topic', default='test-topic', help='Topic name')
    parser.add_argument('--messages', type=int, default=100, help='Number of messages to send')
    parser.add_argument('--rate', type=int, default=10, help='Messages per second')
    args = parser.parse_args()
    
    print(f"Producing {args.messages} messages to topic '{args.topic}' at {args.rate} msg/sec")
    
    # Create producer
    producer = Producer(
        bootstrap_servers=[args.broker],
        client_id='example-producer'
    )
    
    delay = 1.0 / args.rate  # Delay between messages
    
    try:
        for i in range(args.messages):
            # Create message
            message = {
                'id': i,
                'timestamp': int(time.time() * 1000),
                'value': f'Message number {i}',
            }
            
            # Send
            metadata = producer.send(
                topic=args.topic,
                value=json.dumps(message).encode('utf-8'),
                key=f'key-{i % 10}'.encode('utf-8'),  # 10 different keys
            )
            
            if (i + 1) % 10 == 0:
                print(f"Sent {i + 1} messages...")
            
            time.sleep(delay)
        
        # Flush remaining
        producer.flush()
        print(f"\n[OK] Successfully sent {args.messages} messages!")
    
    finally:
        producer.close()


if __name__ == '__main__':
    main()
