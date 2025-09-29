from confluent_kafka import Consumer, KafkaException
from pymongo import MongoClient
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import json, sys, os, signal

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")

KAFKA_TOPIC_REVIEW = os.getenv("KAFKA_TOPIC_REVIEW")

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
reviews = db["reviews"]
users = db["users"]
products = db["products"]

#MONGODB CONNECTION
try:
    client.admin.command('ping')
    print("Connected to MongoDB")
except Exception as e:
    print(e)

def create_consumer():
    return Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "review-consumer",
        "auto.offset.reset": "earliest",  
        "enable.auto.commit": False,    
        "max.poll.interval.ms": 300000,
        "fetch.max.bytes": 5 * 1024 * 1024  #5MB per poll
    })


def shutdown(consumer, sig, frame):
    if consumer is not None:
        consumer.commit(asynchronous=False)
        consumer.close()
    sys.exit(0)

# --- Elasticsearch ---
#es = Elasticsearch("http://localhost:9200")

def main():

    consumer = create_consumer()
    consumer.subscribe([KAFKA_TOPIC_REVIEW])

    #Shutdown signals
    signal.signal(signal.SIGINT, lambda sig, frame: shutdown(consumer, sig, frame))
    signal.signal(signal.SIGTERM, lambda sig, frame: shutdown(consumer, sig, frame))

    processed = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                raise KafkaException(msg.error())
            else:
                try:

                    review = json.loads(msg.value().decode("utf-8"))

                    reviews.insert_one(review)

                    users.update_one(
                        {
                            "user_id": review["user_id"]
                        },
                        {
                            "$inc":{
                                "total_ratings": review["rating"], 
                                "total_reviews": 1
                            },
                        }
                    )

                    products.update_one(
                        {
                            "product_id": review["product_id"]
                        },
                        {
                            "$inc":{
                                "total_ratings": review["rating"], 
                                "total_reviews": 1
                            },
                        }
                    )

                    #es.index(index="reviews", document=review)

                    processed += 1

                    print(f"Recieved: {review}")
                    
                    if processed % 50 == 0:  # commit every 50
                        consumer.commit(asynchronous=False)

                except Exception as e:
                    print(f"Error processing message: {e}")
                    consumer.commit(msg)
    finally:
        shutdown(None, None)

if __name__ == "__main__": 
    main()