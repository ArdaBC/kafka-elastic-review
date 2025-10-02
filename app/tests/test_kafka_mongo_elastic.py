import os
import time
import json
import subprocess
from pathlib import Path

import pytest
from confluent_kafka import Producer
from pymongo import MongoClient
from elasticsearch import Elasticsearch
from dotenv import load_dotenv

from testcontainers.kafka import KafkaContainer
from testcontainers.elasticsearch import ElasticSearchContainer

load_dotenv()

pytestmark = pytest.mark.integration

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI İS not set in .env")

MONGO_WAIT_TIMEOUT = int(os.getenv("MONGO_WAIT_TIMEOUT"))
ES_WAIT_TIMEOUT = int(os.getenv("ES_WAIT_TIMEOUT"))
KAFKA_FLUSH_TIMEOUT = int(os.getenv("KAFKA_FLUSH_TIMEOUT"))
CONSUMER_SHUTDOWN_TIMEOUT = int(os.getenv("CONSUMER_SHUTDOWN_TIMEOUT"))
KAFKA_TEST_TOPIC = os.getenv("KAFKA_TEST_TOPIC")
NUM_TEST_MESSAGES = int(os.getenv("NUM_TEST_MESSAGES"))


def produce_reviews(bootstrap: str, topic: str, reviews: list):
    p = Producer({"bootstrap.servers": bootstrap})
    for r in reviews:
        p.produce(topic=topic, key=str(r.get("user_id", "k")), value=json.dumps(r))
    p.flush(KAFKA_FLUSH_TIMEOUT)


def wait_for_mongo_doc_count(mongo_uri: str, db_name: str, coll_name: str, expected_count: int, timeout: int = MONGO_WAIT_TIMEOUT):
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=2000)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            client.admin.command("ping")
            count = client[db_name][coll_name].count_documents({})
            if count >= expected_count:
                return count
        except Exception:
            pass
        time.sleep(0.5)
    raise TimeoutError(f"Timed out waiting for {db_name}.{coll_name} >= {expected_count}")


def wait_for_es_count(es_client: Elasticsearch, index: str, expected_count: int, timeout: int = ES_WAIT_TIMEOUT):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if es_client.indices.exists(index=index):
                resp = es_client.count(index=index)
                if resp.get("count", 0) >= expected_count:
                    return resp.get("count", 0)
        except Exception:
            pass
        time.sleep(0.5)
    raise TimeoutError(f"Timed out waiting for ES index '{index}' to reach {expected_count} docs")


def _start_consumer_process(env: dict):
    repo_root = Path(__file__).resolve().parents[3]
    cmd = ["python", "-u", "app/consumer/consumer.py"]
    return subprocess.Popen(cmd, env=env, cwd=str(repo_root), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def test_kafka_to_mongo_and_elastic():

    test_db = f"test_reviews_db_{int(time.time())}"

    with KafkaContainer() as kafka_cont, ElasticSearchContainer() as es_cont:
        
        #normalize Kafka bootstrap server (KafkaContainer returns PLAINTEXT://host:port sometimes)
        kafka_bootstrap = kafka_cont.get_bootstrap_server()
        if kafka_bootstrap.startswith("PLAINTEXT://"):
            kafka_bootstrap = kafka_bootstrap.split("PLAINTEXT://", 1)[1]

        es_url = es_cont.get_url()

        messages = []
        for i in range(NUM_TEST_MESSAGES):
            messages.append({
                "user_id": 101 + i,
                "product_id": 201 + (i % 2),
                "rating": 5,
                "review_text": f"integration + es test {i}",
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")
            })

        produce_reviews(kafka_bootstrap, KAFKA_TEST_TOPIC, messages)

        env = os.environ.copy()
        env.update({
            "KAFKA_BOOTSTRAP": kafka_bootstrap,
            "KAFKA_TEST_TOPIC_REVIEW": KAFKA_TEST_TOPIC,
            "MONGO_URI": MONGO_URI,
            "MONGO_DB": test_db,
            "ELASTIC_URI": es_url,
            "LOG_LEVEL": "DEBUG",
            "ELASTIC_BULK_SIZE": "2",
            "KAFKA_BULK_SIZE": "2",
        })

        proc = _start_consumer_process(env)
        try:
            #consumer inserts into Mongo
            wait_for_mongo_doc_count(MONGO_URI, test_db, "reviews", expected_count=NUM_TEST_MESSAGES, timeout=MONGO_WAIT_TIMEOUT)

            #Elasticsearch indexES 'reviews'
            es_client = Elasticsearch(es_url)
            wait_for_es_count(es_client, "reviews", expected_count=NUM_TEST_MESSAGES, timeout=ES_WAIT_TIMEOUT)

        finally:
            proc.terminate()
            try:
                proc.wait(timeout=CONSUMER_SHUTDOWN_TIMEOUT)
            except Exception:
                proc.kill()
                proc.wait(timeout=5)

            try:
                out, err = proc.communicate(timeout=1)
            except Exception:
                out, err = "", ""
            print("=== consumer stdout (tail) ===")
            print("\n".join(out.splitlines()[-100:]))
            print("=== consumer stderr (tail) ===")
            print("\n".join(err.splitlines()[-100:]))

            try:
                client = MongoClient(MONGO_URI)
                client.drop_database(test_db)
            except Exception:
                pass