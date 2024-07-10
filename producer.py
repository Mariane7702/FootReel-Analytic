from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8')
                         )
matches = [
    {"match_id": 1, "team1": "Suisse", "team2": "Italie"},
    {"match_id": 2, "team1": "Allemagne", "team2": "Danemark"},
    {"match_id": 3, "team1": "Angleterre", "team2": "Slovaquie"},
    {"match_id": 4, "team1": "Espagne", "team2": "Géorgie"},
    {"match_id": 5, "team1": "France", "team2": "Belgique"}
]


def get_random_match_event():
    match = random.choice(matches)
    events = ["goal", "possession", "shoot"]
    event_type = random.choice(events)
    event_data = {
        "match_id": match["match_id"],
        "event_type": event_type,
        "team": random.choice([match["team1"], match["team2"]]),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    if event_type == "goal":
        event_data["scorer"] = random.choice(["Player A", "Player B", "Player C"])
    elif event_type == "possession":
        event_data["possession_time"] = random.randint(1, 100)
    elif event_type == "shoot":
        event_data["shooter"] = random.choice(["Player A", "Player B", "Player C"])
    return event_data


while True:
    match_event = get_random_match_event()
    producer.send('football-matches', value=match_event)
    print(f"Sent:{match_event}")
    time.sleep(10)

producer.flush()
producer.close()


    
