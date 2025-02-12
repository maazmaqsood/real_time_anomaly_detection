import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime, timedelta

# Kafka Producer Configuration
KAFKA_BROKER = "kafka:9092"
TOPIC_NAME = "logs"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Sample logs (some normal, some anomalous)
log_entries = [
    {"level": "INFO", "message": "User logged in successfully"},
    {"level": "INFO", "message": "User accessed homepage"},
    {"level": "INFO", "message": "Checkout completed"},
    {"level": "ERROR", "message": "Database connection failed"},
    {"level": "ERROR", "message": "SQL Injection Attempt: ' OR 1=1 --"},
    {"level": "WARNING", "message": "High memory usage detected"},
    {"level": "ERROR", "message": "Unauthorized access attempt from unknown IP"},
]

def generate_log():
    """Simulate log messages"""
    return {"timestamp": time.time(), **random.choice(log_entries)}

def run_producer(duration_seconds=20):
    """Run producer for specified duration"""
    end_time = datetime.now() + timedelta(seconds=duration_seconds)
    messages_sent = 0
    
    print(f"Starting producer for {duration_seconds} seconds...")
    
    while datetime.now() < end_time:
        log_entry = generate_log()
        producer.send(TOPIC_NAME, value=log_entry)
        messages_sent += 1
        print(f"Produced: {log_entry}")
        time.sleep(0.5)  # Send a message every 0.5 seconds
    
    producer.flush()
    print(f"\nProducer finished. Sent {messages_sent} messages.")

if __name__ == "__main__":
    run_producer()
