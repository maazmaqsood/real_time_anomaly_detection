import json
import faiss
import numpy as np
import requests
from kafka import KafkaConsumer
from datetime import datetime, timedelta
import time
from config import KAFKA_BROKER, TOPIC_NAME, OLLAMA_ENDPOINT
from faiss_store import FaissIndex

class Stats:
    def __init__(self):
        self.total_logs = 0
        self.anomalies = 0
        self.processing_times = []
        self.start_time = None
        self.end_time = None

    def start(self):
        self.start_time = datetime.now()

    def finish(self):
        self.end_time = datetime.now()

    def update(self, is_anomaly, processing_time):
        self.total_logs += 1
        if is_anomaly:
            self.anomalies += 1
        self.processing_times.append(processing_time)

    def print_stats(self):
        if not self.processing_times:
            print("No logs processed")
            return

        avg_time = sum(self.processing_times) / len(self.processing_times)
        total_time = (self.end_time - self.start_time).total_seconds()
        
        print("\n====== Anomaly Detection Results ======")
        print(f"Total Runtime: {total_time:.2f} seconds")
        print(f"Total logs processed: {self.total_logs}")
        print(f"Anomalies detected: {self.anomalies}")
        print(f"Normal logs: {self.total_logs - self.anomalies}")
        print(f"Anomaly rate: {(self.anomalies/self.total_logs)*100:.2f}%")
        print(f"Average processing time: {avg_time:.3f} seconds")
        print(f"Messages per second: {self.total_logs/total_time:.2f}")
        print("=====================================")

def run_consumer(duration_seconds=25):
    print("Starting consumer...")
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset='latest',
        group_id='anomaly_detector'
    )
    
    vector_db = FaissIndex()
    vector_db.clear()  # Clear the index at the start of each run
    
    stats = Stats()
    stats.start()
    
    end_time = datetime.now() + timedelta(seconds=duration_seconds)
    
    print("Waiting for messages...")
    
    while datetime.now() < end_time:
        messages = consumer.poll(timeout_ms=1000)
        if not messages:
            continue
            
        for topic_partition, msgs in messages.items():
            for message in msgs:
                start_time = time.time()
                log = message.value
                log_text = log["message"]
                
                # Generate embedding & detect anomaly
                is_anomaly = detect_anomaly(log_text, vector_db)
                
                if is_anomaly:
                    print(f"🚨 Anomaly Detected: {log_text}")
                else:
                    print(f"✅ Normal Log: {log_text}")
                
                processing_time = time.time() - start_time
                stats.update(is_anomaly, processing_time)
    
    stats.finish()
    stats.print_stats()
    consumer.close()

def get_embedding(text):
    """Generate an embedding using mxbai-embed-large on Ollama"""
    try:
        payload = {
            "model": "mxbai-embed-large",
            "input": text
        }
        response = requests.post(OLLAMA_ENDPOINT, json=payload)
        response.raise_for_status()
        
        # Log the response for debugging
        # print(f"Response from Ollama: {response.text}")
        
        response_data = response.json()
        
        if 'embeddings' in response_data:
            # Extract the first embedding vector
            embedding = np.array(response_data['embeddings'][0], dtype=np.float32)
            # print(f"Embedding shape: {embedding.shape}")  # Log the shape of the embedding
            return embedding
        else:
            print(f"Unexpected response format: {response_data}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Error getting embedding: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")
        return None

def detect_anomaly(log_message, vector_db):
    vector = get_embedding(log_message)
    if vector is None:
        print("Error getting embedding")
        return False
    
    # Ensure the vector is a 2D array with shape (1, vector_dim)
    vector = np.expand_dims(vector, axis=0)
    
    # Check if the vector has the correct shape
    if vector.shape[1] != vector_db.index.d:
        print(f"Vector dimensionality mismatch: expected {vector_db.index.d}, got {vector.shape[1]}")
        return False
    
    distance, _ = vector_db.search(vector, 1)
    vector_db.add_vector(vector)
    is_anomaly = distance[0][0] > 0.5
    
    return is_anomaly

if __name__ == "__main__":
    run_consumer()
