#!/bin/bash

echo "Starting anomaly detection test..."


echo "Starting test sequence..."

# Start the consumer first (it will wait for messages)
docker exec kafka_consumer python kafka_consumer.py &
CONSUMER_PID=$!

# Small delay to ensure consumer is ready
sleep 5

# Run the producer
echo "Running producer..."
docker exec kafka_consumer python kafka_producer.py

# Wait for consumer to finish processing
wait $CONSUMER_PID

echo "Test complete!"