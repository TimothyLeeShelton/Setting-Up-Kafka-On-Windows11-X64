import json
from kafka import KafkaConsumer
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()

# Kafka Consumer configuration
consumer = KafkaConsumer(
    'wikipedia_edits',
    bootstrap_servers=[os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='wikipedia_processor',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# PostgreSQL connection
conn = psycopg2.connect(
    dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT')
)
cur = conn.cursor()

# Drop the existing table if it exists
cur.execute("DROP TABLE IF EXISTS page_changes")
conn.commit()

# Create the page_changes table with a proper constraint
cur.execute("""
    CREATE TABLE page_changes (
        window_start TIMESTAMP,
        window_end TIMESTAMP,
        title TEXT,
        count_of_changes INTEGER,
        CONSTRAINT page_changes_pkey PRIMARY KEY (window_start, window_end, title)
    )
""")
conn.commit()

# Process messages
window_duration = timedelta(minutes=1)  # Adjust as needed
for message in consumer:
    change = message.value
    title = change.get('title', 'Unknown')
    current_time = datetime.now()
    window_start = current_time.replace(second=0, microsecond=0)
    window_end = window_start + window_duration
    
    # Update the count in the database
    cur.execute("""
        INSERT INTO page_changes (window_start, window_end, title, count_of_changes)
        VALUES (%s, %s, %s, 1)
        ON CONFLICT (window_start, window_end, title)
        DO UPDATE SET count_of_changes = page_changes.count_of_changes + 1
    """, (window_start, window_end, title))
    conn.commit()
    
    print(f"Processed change for page: {title} in window {window_start} to {window_end}")

# Close connections
cur.close()
conn.close()
consumer.close()