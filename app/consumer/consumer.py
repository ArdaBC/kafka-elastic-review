import signal, json, os, datetime
from confluent_kafka import Consumer, KafkaException
from elasticsearch import Elasticsearch
from pymongo import MongoClient
from dotenv import load_dotenv
from elasticsearch import helpers
from utils import create_indices


load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
KAFKA_TOPIC_REVIEW = os.getenv("KAFKA_TOPIC_REVIEW")
ELASTIC_URI = os.getenv("ELASTIC_URI", "http://localhost:9200")
ELASTIC_USER = os.getenv("ELASTIC_USER")
ELASTIC_PASSWORD = os.getenv("ELASTIC_PASSWORD")

ELASTIC_BULK_SIZE = int(os.getenv("ELASTIC_BULK_SIZE",10))
KAFKA_BULK_SIZE = int(os.getenv("KAFKA_BULK_SIZE",10))
bulk_actions = []

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


    print(f"Connecting to Elasticsearch at {ELASTIC_URI} ...")
    es = Elasticsearch(
        ELASTIC_URI,
        basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD),
        verify_certs=False,
        request_timeout=30
    )


    try:
        if es.ping():
            print("Connected to Elasticsearch")
            create_indices(es)
        else:
            print("Elasticsearch is not responding")
    except Exception as e:
        print(f"Elasticsearch connection error: {e}")

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

                #Insert into Mongo
                result = reviews.insert_one(review)

                users.update_one(
                    {
                        "user_id": review["user_id"]
                    },
                    {
                        "$inc": {
                            "total_ratings": review["rating"], 
                            "total_reviews": 1
                        }
                    },
                    upsert=True
                )

                products.update_one(
                    {
                        "product_id": review["product_id"]
                    },
                    {
                        "$inc": {
                            "total_ratings": review["rating"], 
                            "total_reviews": 1
                        }
                    },
                    upsert=True
                )

                #Compute avg_rating, upsert into Elasticsearch for Kibana
                try:
                    prod = products.find_one({"product_id": review["product_id"]})
                    if prod:
                        total_ratings = float(prod.get("total_ratings", 0.0))
                        total_reviews = int(prod.get("total_reviews", 0))
                        avg_rating = total_ratings / total_reviews if total_reviews > 0 else 0.0

                        product_doc = {
                            "product_id": prod.get("product_id"),
                            "name": prod.get("name"),
                            "total_ratings": total_ratings,
                            "total_reviews": total_reviews,
                            "avg_rating": avg_rating
                        }

                        es.index(index="products", id=str(product_doc["product_id"]), body=product_doc)

                    usr = users.find_one({"user_id": review["user_id"]})
                    if usr:
                        total_ratings = float(usr.get("total_ratings", 0.0))
                        total_reviews = int(usr.get("total_reviews", 0))
                        avg_rating = total_ratings / total_reviews if total_reviews > 0 else 0.0

                        #Exclude password from indexing
                        user_doc = {
                            "user_id": usr.get("user_id"),
                            "name": usr.get("name"),
                            "email": usr.get("email"),
                            "phone": usr.get("phone"),
                            "total_ratings": total_ratings,
                            "total_reviews": total_reviews,
                            "avg_rating": avg_rating
                        }

                        es.index(index="users", id=str(user_doc["user_id"]), body=user_doc)

                except Exception as e:
                    print(f"Warning: failed to update product/user summary in ES: {e}")

                inserted_id = result.inserted_id
                doc_id = str(inserted_id)
                doc = {k: v for k, v in review.items() if k != "_id"}

                #Make timestamp is string
                if isinstance(doc.get("timestamp"), datetime.datetime):
                    doc["timestamp"] = doc["timestamp"].isoformat()

                bulk_actions.append({
                    "_index": "reviews",
                    "_id": doc_id,
                    "_source": doc
                })

                processed += 1

                print(f"Recieved: {review}")

                if processed % KAFKA_BULK_SIZE == 0:
                    consumer.commit(asynchronous=False)

                if len(bulk_actions) >= ELASTIC_BULK_SIZE:
                    try:
                        helpers.bulk(es, bulk_actions)
                    except Exception as e:
                        print(f"Error performing ES bulk for reviews: {e}")
                    bulk_actions.clear()

            except Exception as e:
                print(f"Error processing message: {e}")
                consumer.commit(message=msg)  #Skip the bad record


    finally:
        print("Closing consumer")

        if bulk_actions:
            try:
                helpers.bulk(es, bulk_actions)
            except Exception as e:
                print(f"Error performing final ES bulk: {e}")
            bulk_actions.clear()

        consumer.commit(asynchronous=False)
        consumer.close()
        print("Consumer closed cleanly")


if __name__ == "__main__":
    main()