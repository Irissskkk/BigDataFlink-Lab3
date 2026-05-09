import pandas as pd
import json
import os
import time
import logging
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from faker import Faker
import random

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
fake = Faker()

KAFKA_TOPIC = 'input-topic'
KAFKA_BOOTSTRAP_SERVERS = 'kafka:9092'  # Внутри Docker сети

def generate_csv_files():
    """Генерирует 10 CSV файлов по 1000 строк каждый"""
    if not os.path.exists('/data'):
        os.makedirs('/data')
    
    existing_files = [f for f in os.listdir('/data') if f.startswith('mock_data') and f.endswith('.csv')]
    if existing_files:
        logger.info(f"Found {len(existing_files)} existing CSV files, skipping generation")
        return
    
    logger.info("Generating 10 CSV files with 1000 rows each...")
    
    for file_num in range(1, 11):
        data = []
        for i in range(1000):
            row = {
                'transaction_id': f"TXN_{file_num}_{i}_{fake.uuid4()[:8]}",
                'product_id': random.randint(1, 100),
                'product_name': fake.word().capitalize() + " " + fake.word(),
                'category': random.choice(['Electronics', 'Clothing', 'Books', 'Food', 'Sports']),
                'customer_id': random.randint(1000, 9999),
                'customer_name': fake.name(),
                'city': fake.city(),
                'store_id': random.randint(1, 20),
                'sale_amount': round(random.uniform(10, 1000), 2),
                'sale_timestamp': fake.date_time_this_year().isoformat()
            }
            data.append(row)
        
        df = pd.DataFrame(data)
        csv_path = f'/data/mock_data_{file_num}.csv'
        df.to_csv(csv_path, index=False)
        logger.info(f"Created {csv_path}")
    
    logger.info("All CSV files generated successfully!")

def create_producer():
    """Создает подключение к Kafka"""
    max_retries = 15
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                max_block_ms=5000,
                request_timeout_ms=5000,
                api_version_auto_timeout_ms=5000
            )
            logger.info("✓ Connected to Kafka successfully!")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"Waiting for Kafka... attempt {attempt + 1}/{max_retries}")
            time.sleep(retry_delay)
    
    raise Exception("Could not connect to Kafka")

def send_data_to_kafka():
    """Отправляет данные из CSV в Kafka"""
    producer = create_producer()

    
    csv_files = sorted([f for f in os.listdir('/data') if f.startswith('mock_data') and f.endswith('.csv')])
    
    if not csv_files:
        logger.error("No CSV files found!")
        return
    
    logger.info(f"Found {len(csv_files)} CSV files")
    
    total_messages = 0
    
    for csv_file in csv_files:
        logger.info(f"Processing {csv_file}...")
        df = pd.read_csv(os.path.join('/data', csv_file))
        
        for idx, row in df.iterrows():
            message = row.to_dict()

            message = {k: (v.item() if hasattr(v, 'item') else v) for k, v in message.items()}
            
            try:
            
                future = producer.send(KAFKA_TOPIC, key=str(idx).encode(), value=message)
                future.get(timeout=2)
                total_messages += 1
                
                if total_messages % 100 == 0:
                    logger.info(f"Sent {total_messages} messages so far...")
                    
            except Exception as e:
                logger.error(f"Failed to send message: {e}")
                continue
        
        logger.info(f"Finished {csv_file}")
    
    producer.flush()
    logger.info(f"Done! Sent {total_messages} messages to Kafka topic '{KAFKA_TOPIC}'")
    producer.close()

if __name__ == "__main__":
    logger.info("Starting data producer...")
    generate_csv_files()
    time.sleep(3)
    send_data_to_kafka()
