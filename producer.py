from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8')
                         )


def get_random_match_data():
    return {
        "match_id": str(random.randint(1, 100000000)),
        "team1": random.choice(["Liverpool FC", "Manchester City FC", "Chelsea FC"]),
        "team2": random.choice(["Manchester United FC", "Newcastle United FC","West Ham United FC"]),
        "score1": random.randint(0,5),
        "score2": random.randint(0,5),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }

while True:
    match_data = get_random_match_data()
    producer.send('football-matches', value=match_data)
    print(f"Sent:{match_data}")
    time.sleep(3)

producer.flush()
producer.close()


    
