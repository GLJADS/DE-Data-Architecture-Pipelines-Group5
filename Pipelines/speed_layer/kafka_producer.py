#!/usr/bin/env python3
"""
Kafka Producer Script for Netflix Watch Events

This script reads watch_history.csv and sends each row as a JSON message to Kafka.
Run this script separately to populate the Kafka topic before starting the streaming pipeline.

Usage:
    python kafka_producer.py
    
Or with custom parameters:
    python kafka_producer.py --csv-path /path/to/watch_history.csv --kafka-servers kafka1:9093 --topic netflix_watch_events --delay 0.01
"""

import json
import csv
import time
import argparse
from datetime import datetime
from kafka import KafkaProducer


def produce_watch_events(csv_file_path, kafka_servers, topic, delay=0.1):
    """
    Read watch_history.csv and send each row as a JSON message to Kafka.
    
    Args:
        csv_file_path: Path to watch_history.csv
        kafka_servers: Kafka bootstrap servers (e.g., 'kafka1:9093')
        topic: Kafka topic name
        delay: Delay between messages in seconds (to simulate real-time)
    """
    producer = KafkaProducer(
        bootstrap_servers=[kafka_servers],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    print(f"📤 Starting to produce events to topic: {topic}")
    print(f"   CSV file: {csv_file_path}")
    print(f"   Kafka servers: {kafka_servers}")
    print(f"   Delay between messages: {delay} seconds")
    print()
    
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            count = 0
            
            for row in reader:
                # Convert CSV row to JSON
                # Add current timestamp for streaming (simulating real-time events)
                current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                event = {
                    'session_id': row['session_id'],
                    'user_id': row['user_id'],
                    'movie_id': row['movie_id'],
                    'watch_date': row['watch_date'],
                    'event_timestamp': current_timestamp,  # Add timestamp for streaming
                    'device_type': row['device_type'],
                    'watch_duration_minutes': float(row['watch_duration_minutes']) if row['watch_duration_minutes'] else None,
                    'progress_percentage': float(row['progress_percentage']) if row['progress_percentage'] else None,
                    'action': row['action'],
                    'quality': row['quality'],
                    'location_country': row['location_country'],
                    'is_download': row['is_download'].lower() == 'true',
                    'user_rating': int(row['user_rating']) if row['user_rating'] else None
                }
                
                # Send to Kafka
                producer.send(topic, value=event)
                count += 1
                
                if count % 1000 == 0:
                    print(f"   Sent {count} events...")
                
                time.sleep(delay)  # Simulate real-time streaming
        
        producer.flush()
        print(f"\n✅ Finished producing {count} events to topic: {topic}")
        
    except FileNotFoundError:
        print(f"❌ Error: CSV file not found: {csv_file_path}")
        return
    except Exception as e:
        print(f"❌ Error producing events: {e}")
        return
    finally:
        producer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Kafka Producer for Netflix Watch Events')
    parser.add_argument('--csv-path', 
                        default='/home/jovyan/data/raw/watch_history.csv',
                        help='Path to watch_history.csv file')
    parser.add_argument('--kafka-servers',
                        default='kafka1:9093',
                        help='Kafka bootstrap servers (e.g., kafka1:9093)')
    parser.add_argument('--topic',
                        default='netflix_watch_events',
                        help='Kafka topic name')
    parser.add_argument('--delay',
                        type=float,
                        default=0.01,
                        help='Delay between messages in seconds (default: 0.01)')
    
    args = parser.parse_args()
    
    produce_watch_events(
        csv_file_path=args.csv_path,
        kafka_servers=args.kafka_servers,
        topic=args.topic,
        delay=args.delay
    )

