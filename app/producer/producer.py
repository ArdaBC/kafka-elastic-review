from kafka import KafkaProducer
from utils import generate_review
import time, json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    review = generate_review(50)
    producer.send('review-topic', review)
    print(f"Sent: {review}")
    time.sleep(10)