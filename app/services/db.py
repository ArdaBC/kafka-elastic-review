from pymongo.mongo_client import MongoClient
from dotenv import load_dotenv
import os


def get_db():
    load_dotenv()

    MONGO_URI = os.getenv("MONGO_URI")
    MONGO_DB = os.getenv("MONGO_DB")

    client = MongoClient(MONGO_URI)
    return client[MONGO_DB]
