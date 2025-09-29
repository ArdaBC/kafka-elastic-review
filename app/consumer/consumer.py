import signal, json, os
from confluent_kafka import Consumer, KafkaException
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
KAFKA_TOPIC_REVIEW = os.getenv("KAFKA_TOPIC_REVIEW")

running = True

def create_consumer():
    return Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "review-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300000,
        "fetch.max.bytes": 5 * 1024 * 1024
    })


def handle_shutdown(sig, frame):
    global running
    print("Shutdown signal received")
    running = False 


def main():
    global running

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    reviews = db["reviews"]
    users = db["users"]
    products = db["products"]

    #Check MongoDB connection
    try:
        client.admin.command("ping")
        print("Connected to MongoDB")
    except Exception as e:
        print(e)

    consumer = create_consumer()
    consumer.subscribe([KAFKA_TOPIC_REVIEW])

    #Register signals
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    processed = 0

    try:
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                raise KafkaException(msg.error())
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

                if processed % 10 == 0:
                    consumer.commit(asynchronous=False)

            except Exception as e:
                print(f"Error processing message: {e}")
                consumer.commit(message=msg)  #skip the bad record

    finally:
        print("Closing consumer")
        consumer.commit(asynchronous=False)
        consumer.close()
        print("Consumer closed cleanly")


if __name__ == "__main__":
    main()