import os

from dotenv import load_dotenv
from psycopg2 import sql

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from kafka import KafkaConsumer
import json


from database import DatabaseHandler
load_dotenv()

spark = (SparkSession.builder
         .appName("FootReel Analytic")
         .getOrCreate())

consumer = KafkaConsumer(
    'football-matches',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='footreel-consumers',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

db_handler = DatabaseHandler(
    dbname=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD')
)

schema = StructType([
    StructField("match_id", IntegerType(), True),
    StructField("event_type", StringType(), True),
    StructField("team", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("scorer", StringType(), True),
    StructField("possession_time", IntegerType(), True),
    StructField("shooter", StringType(), True)
])

match_stats = {
    1: {"team1": "Suisse", "team2": "Italie", "score1": 0, "score2": 0, "possession1": 0, "possession2": 0,
        "timestamp": "2024-07-10 11:00:00"},
    2: {"team1": "Allemagne", "team2": "Danemark", "score1": 0, "score2": 0, "possession1": 0, "possession2": 0,
        "timestamp": "2024-07-10 13:00:00"},
    3: {"team1": "Angleterre", "team2": "Slovaquie", "score1": 0, "score2": 0, "possession1": 0, "possession2": 0,
        "timestamp": "2024-07-10 15:00:00"},
    4: {"team1": "Espagne", "team2": "Géorgie", "score1": 0, "score2": 0, "possession1": 0, "possession2": 0,
        "timestamp": "2024-07-10 17:00:00"},
    5: {"team1": "France", "team2": "Belgique", "score1": 0, "score2": 0, "possession1": 0, "possession2": 0,
        "timestamp": "2024-07-10 19:00:00"}
}


def initialize_matches_table(db_handler):
    for match_id, match in match_stats.items():
        query = sql.SQL(
            "INSERT INTO matches (match_id, team1, team2, timestamp) VALUES (%s, %s, %s, %s)"
        )
        values = (match_id, match['team1'], match['team2'], match['timestamp'])
        try:
            db_handler.execute_query(query, values)
        except Exception as e:
            print(f"Error inserting match {match_id}: {e}")


def process_event(event, db_handler):
    try:
        if event["event_type"] == "goal":
            query = sql.SQL("INSERT INTO goals (match_id, team, scorer, timestamp) VALUES (%s, %s, %s, %s)")
            values = (event["match_id"], event["team"], event["scorer"], event["timestamp"])
        elif event["event_type"] == "possession":
            query = sql.SQL(
                "INSERT INTO possession (match_id, team, possession_time, timestamp) VALUES (%s, %s, %s, %s)")
            values = (event["match_id"], event["team"], event["possession_time"], event["timestamp"])
        elif event["event_type"] == "shoot":
            query = sql.SQL("INSERT INTO shots (match_id, team, shooter, timestamp) VALUES (%s, %s, %s, %s)")
            values = (event["match_id"], event["team"], event["shooter"], event["timestamp"])

        db_handler.execute_query(query, values)

    except Exception as e:
        print(f"Error processing event: {e}")



print("Starting Consumer...")

for message in consumer:
    event = message.value
    print(f"Received event: {event}")
    process_event(event, db_handler)

import atexit

atexit.register(db_handler.close)
