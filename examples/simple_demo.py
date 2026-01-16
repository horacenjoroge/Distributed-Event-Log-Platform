#!/usr/bin/env python3
"""
Simple demo of DistributedLog producer and consumer.

This demonstrates the basic producer/consumer pattern.
"""

import time
import json
from distributedlog.producer.producer import Producer
from distributedlog.consumer.consumer import Consumer

def main():
    print("=" * 60)
    print("DistributedLog - Simple Producer/Consumer Demo")
    print("=" * 60)
    
    # Create producer
    print("\n[1] Creating producer...")
    producer = Producer(
        bootstrap_servers=['localhost:9092'],
        client_id='demo-producer'
    )
    print("✅ Producer created")
    
    # Create consumer
    print("\n[2] Creating consumer...")
    consumer = Consumer(
        bootstrap_servers=['localhost:9092'],
        group_id='demo-group',
        client_id='demo-consumer'
    )
    consumer.subscribe(['demo-topic'])
    print("✅ Consumer created and subscribed to 'demo-topic'")
    
    # Produce messages
    print("\n[3] Producing 10 messages...")
    for i in range(10):
        message = {
            'id': i,
            'timestamp': int(time.time()),
            'data': f'Hello from DistributedLog! Message #{i}'
        }
        
        # Serialize to bytes
        message_bytes = json.dumps(message).encode('utf-8')
        
        try:
            # Send message (returns Future)
            future = producer.send(
                topic='demo-topic',
                value=message_bytes,
                key=f'key-{i}'.encode('utf-8')
            )
            # Get result from future
            metadata = future.result(timeout=5.0)
            print(f"  ✅ Sent message {i}: offset={metadata.offset}, partition={metadata.partition}")
        except Exception as e:
            print(f"  ❌ Failed to send message {i}: {e}")
    
    # Flush pending messages
    producer.flush()
    print("✅ All messages flushed")
    
    # Consume messages
    print("\n[4] Consuming messages...")
    consumed = 0
    timeout = 10  # seconds
    start_time = time.time()
    
    while consumed < 10 and (time.time() - start_time) < timeout:
        messages = consumer.poll(timeout_ms=1000)
        
        for message in messages:
            # Deserialize
            data = json.loads(message.value.decode('utf-8'))
            print(f"  ✅ Consumed: {data}")
            consumed += 1
            
            if consumed >= 10:
                break
    
    if consumed == 10:
        print(f"\n✅ Successfully consumed all {consumed} messages!")
    else:
        print(f"\n⚠️  Only consumed {consumed}/10 messages")
    
    # Cleanup
    print("\n[5] Cleaning up...")
    producer.close()
    consumer.close()
    print("✅ Resources cleaned up")
    
    print("\n" + "=" * 60)
    print("Demo completed successfully!")
    print("=" * 60)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nDemo interrupted by user")
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
