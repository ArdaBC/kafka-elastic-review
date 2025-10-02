import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv

load_dotenv()

# Environment variables
ELASTIC_URI = os.getenv("ELASTIC_URI")
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
ELASTIC_INDEX = os.getenv("ELASTIC_INDEX", "reviews")
ELASTIC_BULK_SIZE = int(os.getenv("ELASTIC_BULK_SIZE", 10))

# Airflow Variable name to store last synced _id 
LAST_SYNC_ID_VAR = "last_mongo_sync_id"

def nightly_sync():

    from pymongo import MongoClient
    from bson.objectid import ObjectId
    from elasticsearch import Elasticsearch, helpers
    from airflow.models import Variable
    


 
    
    """Check MongoDB reviews and add missing docs to Elasticsearch using last synced _id."""
    print("Starting nightly MongoDB → Elasticsearch sync...")

    mongo = MongoClient(MONGO_URI)
    db = mongo[MONGO_DB]
    collection = db["reviews"]

    es = Elasticsearch(ELASTIC_URI)

    # Get last synced _id from Airflow Variables
    last_id_str = Variable.get(LAST_SYNC_ID_VAR, default_var=None)
    if last_id_str:
        last_id = ObjectId(last_id_str)
        query = {"_id": {"$gt": last_id}}
    else:
        query = {}  # first run: sync everything

    # Sort by _id ascending to ensure order
    docs = collection.find(query).sort("_id", 1)

    bulk_actions = []
    processed = 0
    added = 0
    last_synced_id = None

    for doc in docs:
        processed += 1
        doc_id = str(doc["_id"])
        last_synced_id = doc["_id"]

        if not es.exists(index=ELASTIC_INDEX, id=doc_id):
            bulk_actions.append({
                "_index": ELASTIC_INDEX,
                "_id": doc_id,
                "_source": doc
            })
            added += 1

        if len(bulk_actions) >= ELASTIC_BULK_SIZE:
            try:
                helpers.bulk(es, bulk_actions)
            except Exception as e:
                print(f"Error performing ES bulk: {e}")
            bulk_actions.clear()

    # Flush remaining bulk
    if bulk_actions:
        try:
            helpers.bulk(es, bulk_actions)
        except Exception as e:
            print(f"Error performing ES bulk: {e}")
        bulk_actions.clear()

    # Save last synced _id to Airflow Variable
    if last_synced_id:
        Variable.set(LAST_SYNC_ID_VAR, str(last_synced_id))

    print(f"Nightly sync finished. Processed: {processed}, Added: {added}")
    
    
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nightly_mongo_to_es",
    default_args=default_args,
    description="Nightly sync MongoDB reviews to Elasticsearch using last _id",
    start_date=datetime(2025, 10, 2),
    schedule_interval="54 14 * * *",  # every night at 2AM
    catchup=False,
    max_active_runs=1,
    tags=["mongo", "elasticsearch", "sync"]
) as dag:

    sync_task = PythonOperator(
        task_id="check_and_sync_mongo_es",
        python_callable=nightly_sync,
    )
