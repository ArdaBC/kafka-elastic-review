import os
from confluent_kafka import Producer
from utils import generate_review
import time, json
from dotenv import load_dotenv


load_dotenv()
KAFKA_TOPIC_REVIEW = os.getenv("KAFKA_TOPIC_REVIEW")

conf = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for message: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


while True:
    review = generate_review(50)
    producer.produce(
        topic=KAFKA_TOPIC_REVIEW,
        value=json.dumps(review),
        callback=delivery_report
    )
    
    producer.flush()
    #print(f"Sent: {review}")
    
    time.sleep(3)