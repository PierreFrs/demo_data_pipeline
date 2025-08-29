import os, time, ujson, uuid, random, datetime
from kafka import KafkaProducer

topic = os.getenv("TOPIC","orders")
brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS","redpanda:9092")
rps = int(os.getenv("RATE_PER_SEC","5"))

prod = KafkaProducer(
    bootstrap_servers=brokers.split(","),
    value_serializer=lambda v: ujson.dumps(v).encode("utf-8")
)

print("Producing to", topic)
while True:
    now = datetime.datetime.now(datetime.timezone.utc)
    v = {
        "event_id": str(uuid.uuid4()),
        "user_id": random.randint(1, 1000),
        "event_type": random.choice(["view","purchase","add_to_cart"]),
        "price": round(random.uniform(5, 200), 2),
        "ts": now.isoformat().replace("+00:00","Z")
    }
    prod.send(topic, v)
    time.sleep(1.0/max(1,rps))
