import os
from confluent_kafka import Producer
from utils import generate_review
import time, json
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi



load_dotenv()

conf = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(conf)


#MONGODB CONNECTION
uri = os.getenv("MONGO_URI")
client = MongoClient(uri, server_api=ServerApi('1'))

try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

#TO_DO: Get from .env for future
db = client["Kafka-Elastic-Review"]
collection = db["Reviews"]


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
    #print(f"Sent: {review}")
    
    try:
        collection.insert_one(review)
        print("Inserted into MongoDB:", review)
    except Exception as e:
        print("MongoDB insert error:", e)
        
    
    time.sleep(3)
    
    

