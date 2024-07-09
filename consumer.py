import os
import threading
import time

from dotenv import load_dotenv
from psycopg2 import sql

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

match_stats = {
    1: {"team1": "Suisse", "team2": "Italie", "score1": 0, "score2": 0, "possession1": 0, "possession2": 0},
    2: {"team1": "Allemagne", "team2": "Danemark", "score1": 0, "score2": 0, "possession1": 0, "possession2": 0},
    3: {"team1": "Angleterre", "team2": "Slovaquie", "score1": 0, "score2": 0, "possession1": 0, "possession2": 0},
    4: {"team1": "Espagne", "team2": "Géorgie", "score1": 0, "score2": 0, "possession1": 0, "possession2": 0},
    5: {"team1": "France", "team2": "Belgique", "score1": 0, "score2": 0, "possession1": 0, "possession2": 0}
}

def process_event(event, db_handler):
    try:
        match_id = event["match_id"]
        match = match_stats[match_id]
        print("ok1")

        if event["event_type"] == "goal":
            print("ok2")
            if event["team"] == match["team1"]:
                print("ok3")
                match["score1"] += 1
            else:
                print("ok4")
                match["score2"] += 1
            query = sql.SQL("INSERT INTO match_events (match_id, event_type, team, scorer, timestamp) VALUES (%s, %s, %s, %s, %s)")
            values = (event["match_id"], event["event_type"], event["team"], event["scorer"], event["timestamp"])
        elif event["event_type"] == "possession":
            if event["team"] == match["team1"]:
                match["possession1"] += event["possession_time"]
            else:
                match["possession2"] += event["possession_time"]
            query = sql.SQL("INSERT INTO match_events (match_id, event_type, team, possession_time, timestamp) VALUES (%s, %s, %s, %s, %s)")
            values = (event["match_id"], event["event_type"], event["team"], event["possession_time"], event["timestamp"])
        query = sql.SQL("INSERT INTO match_events (match_id, event_type, team, scorer, timestamp) VALUES (%s, %s, %s, %s, %s)")
        db_handler.execute_query(query, values)
    except Exception as e:
        print(f"Error processing event: {e}")

def display_stats():
    while True:
        print("\nCurrent Match Stats:")
        for match_id, stats in match_stats.items():
            print(f"Match {match_id}: {stats['team1']} vs {stats['team2']}")
            print(f"Score: {stats['team1']} {stats['score1']} - {stats['score2']} {stats['team2']}")
            print(f"Possession: {stats['team1']} {stats['possession1']}% - {stats['possession2']}% {stats['team2']}")
        time.sleep(10)

display_thread = threading.Thread(target=display_stats)
display_thread.start()

print("Starting Consumer...")


for message in consumer:
    event = message.value
    print(f"Received event: {event}")
    process_event(event, db_handler)




