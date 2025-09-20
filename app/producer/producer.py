from confluent_kafka import Producer
from utils import generate_review
import time, json


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
        topic='review-topic',
        value=json.dumps(review),
        callback=delivery_report
    )
    
    producer.flush()
    print(f"Sent: {review}")
    time.sleep(3)
