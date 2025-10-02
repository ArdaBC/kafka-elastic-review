import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from pymongo import MongoClient
from elasticsearch import Elasticsearch, helpers
from bson import ObjectId

# Load environment variables
load_dotenv()

# MongoDB configuration
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB").strip('"')  # remove accidental quotes
MONGO_COLLECTION = "reviews"

# Elasticsearch configuration
ELASTIC_URI = os.getenv("ELASTIC_URI")
ELASTIC_INDEX = "reviews"
ELASTIC_BULK_SIZE = int(os.getenv("ELASTIC_BULK_SIZE", 500))

# State tracking (simplest approach: store last synced ObjectId in a file)
STATE_FILE = "/opt/airflow/dags/mongo_to_es_last_id.txt"


def get_last_synced_id():
    """Retrieve the last synced ObjectId from state file."""
    if not os.path.exists(STATE_FILE):
        return None
    with open(STATE_FILE, "r") as f:
        last_id = f.read().strip()
        return ObjectId(last_id) if last_id else None


def set_last_synced_id(last_id):
    """Persist the last synced ObjectId to state file."""
    with open(STATE_FILE, "w") as f:
        f.write(str(last_id))


def sync_mongo_to_es():
    logging.info("Starting MongoDB → Elasticsearch sync")

    # Initialize clients
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[MONGO_DB]
    collection = db[MONGO_COLLECTION]

    es = Elasticsearch([ELASTIC_URI])

    # Track last synced ObjectId
    last_synced_id = get_last_synced_id()
    logging.info(f"Last synced ObjectId: {last_synced_id}")

    query = {}
    if last_synced_id:
        query["_id"] = {"$gt": last_synced_id}

    # Fetch new documents
    cursor = collection.find(query).sort("_id", 1)

    actions = []
    latest_id = None

    for doc in cursor:
        doc_id = str(doc["_id"])
        latest_id = doc["_id"]

        # Convert ObjectId to string for ES
        doc["_id"] = doc_id

        action = {
            "_op_type": "index",
            "_index": ELASTIC_INDEX,
            "_id": doc_id,
            "_source": doc,
        }
        actions.append(action)

        # Bulk insert in chunks
        if len(actions) >= ELASTIC_BULK_SIZE:
            helpers.bulk(es, actions)
            logging.info(f"Indexed {len(actions)} docs into {ELASTIC_INDEX}")
            actions.clear()

    if actions:
        helpers.bulk(es, actions)
        logging.info(f"Indexed {len(actions)} docs into {ELASTIC_INDEX}")

    if latest_id:
        set_last_synced_id(latest_id)
        logging.info(f"Updated last synced ObjectId → {latest_id}")

    logging.info("MongoDB → Elasticsearch sync completed")


# Define DAG
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="mongo_to_es_sync",
    default_args=default_args,
    description="Daily sync of MongoDB 'reviews' collection to Elasticsearch 'reviews' index",
    schedule_interval="0 0 * * *",  # Midnight daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["mongo", "elasticsearch", "sync"],
) as dag:

    sync_task = PythonOperator(
        task_id="sync_reviews",
        python_callable=sync_mongo_to_es,
    )
