import os
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from kafka import KafkaConsumer
import json

import psycopg2

from database import DatabaseHandler

load_dotenv()


spark = (SparkSession.builder
         .appName("FootReel Analytic")
         .getOrCreate())

schema = StructType([
    StructField("match_id", StringType(), True),
    StructField("team1", StringType(), True),
    StructField("team2", StringType(), True),
    StructField("score1", IntegerType(), True),
    StructField("score2", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

consumer = KafkaConsumer(
    'football-matches',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='footreel-consumers'
)

db_handler = DatabaseHandler(
    dbname=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD')
)
print(os.getenv('POSTGRES_DB'))
print(os.getenv('POSTGRES_USER'))
print(os.getenv('POSTGRES_PASSWORD'))

def process_batch(messages, db_handler):
    for msg in messages:
        try:
            match_data = json.loads(msg.value)
            db_handler.execute_query(
                "INSERT INTO match_statistics (match_id, team1, team2, score1, score2, timestamp, total_score) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (match_data["match_id"], match_data["team1"], match_data["team2"],
                 match_data["score1"], match_data["score2"], match_data["timestamp"],
                 match_data["score1"] + match_data["score2"])
            )
            print("test ok")
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
        except psycopg2.Error as e:
            print(f"PostgreSQL error: {e}")




print("Starting Consumer...")





batch = []
batch_size = 1
for message in consumer:
    batch.append(message)
    print(message.value)  # Print the received message
    if len(batch) >= batch_size:
        process_batch(batch, db_handler)
        batch = []

if batch:
    process_batch(batch, db_handler)

