from confluent_kafka import Producer
from utils import generate_review
import time, json, sys, os, signal
from dotenv import load_dotenv


load_dotenv()
KAFKA_TOPIC_REVIEW = os.getenv("KAFKA_TOPIC_REVIEW")


def create_producer():
    return Producer({
        "bootstrap.servers": "localhost:9092",
        "linger.ms": 50,
        "batch.num.messages": 10000,
        "compression.type": "snappy",
        "acks": "all",
    })


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for message: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def shutdown(producer, sig, frame):
    producer.flush()
    sys.exit(0)


def main():
    producer = create_producer()

    #Shutdown signals
    signal.signal(signal.SIGINT, lambda sig, frame: shutdown(producer, sig, frame))
    signal.signal(signal.SIGTERM, lambda sig, frame: shutdown(producer, sig, frame))

    while True:
        #Mock data
        review = generate_review(50)

        producer.produce(
            topic=KAFKA_TOPIC_REVIEW,
            key=str(review["user_id"]),
            value=json.dumps(review),
            callback=delivery_report
        )
        
        producer.poll(0)
        time.sleep(1)


if __name__ == "__main__":
    main()