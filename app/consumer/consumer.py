from confluent_kafka import Consumer
from pymongo import MongoClient
from elasticsearch import Elasticsearch
from dotenv import load_dotenv
import os, json

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")


conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'review-consumer',
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(conf)
consumer.subscribe(['review-topic'])


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

# --- Elasticsearch ---
#es = Elasticsearch("http://localhost:9200")

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"Consumer error: {msg.error()}")
        continue

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

    except Exception as e:
        print(f"Error processing message: {e}")