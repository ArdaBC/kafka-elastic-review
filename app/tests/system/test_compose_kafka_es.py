# app/tests/system/test_compose_kafka_es.py
"""
System smoke test (simple):
- Requires docker-compose installed and your repo's docker-compose.yaml to expose Kafka & Elasticsearch ports.
- Uses your ONLINE MongoDB (MONGO_URI).
- Brings up kafka + elasticsearch via docker-compose, produces messages, launches the consumer,
  and verifies:
    * documents in Mongo
    * reviews index exists in Elasticsearch
This test is intentionally minimal: it's a smoke test rather than a full exhaustive E2E.
"""

import os
import time
import subprocess
from pathlib import Path

import pytest
from pymongo import MongoClient
from elasticsearch import Elasticsearch
from confluent_kafka import Producer

pytestmark = pytest.mark.system

DOCKER_COMPOSE_FILE = "docker-compose.yaml"
WAIT_SECONDS_FOR_SERVICES = 20
NUM_MESSAGES = 3
KAFKA_TOPIC = "reviews_system_smoke"


def produce_reviews(bootstrap, topic, messages):
    p = Producer({"bootstrap.servers": bootstrap})
    for m in messages:
        p.produce(topic=topic, key=str(m.get("user_id", "k")), value=json.dumps(m))
    p.flush(10)


def wait_for_es(es_url: str, timeout: int = 30):
    es = Elasticsearch(es_url)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if es.ping():
                return es
        except Exception:
            pass
        time.sleep(0.5)
    raise TimeoutError("ES did not become ready")


def test_system_smoke_compose_up():
    # This test expects docker-compose.yaml to define services for kafka and elasticsearch
    repo_root = Path(__file__).resolve().parents[3]
    # start compose
    subprocess.check_call(["docker-compose", "-f", DOCKER_COMPOSE_FILE, "up", "-d"], cwd=str(repo_root))

    try:
        # Give services some time to start
        time.sleep(WAIT_SECONDS_FOR_SERVICES)

        # Mongo is online — read MONGO_URI from env
        mongo_uri = os.getenv("MONGO_URI")
        if not mongo_uri:
            pytest.skip("MONGO_URI not set — system test uses online MongoDB")

        mongo_client = MongoClient(mongo_uri, serverSelectionTimeoutMS=2000)
        mongo_client.admin.command("ping")

        # Elastic endpoint: adjust to match your docker-compose mapping (default below)
        es_url = os.getenv("ELASTIC_URI", "http://localhost:9200")
        es = wait_for_es(es_url)

        # Kafka bootstrap (host:port) - must match how compose maps it; default: localhost:9092
        kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

        # produce a few test messages
        messages = []
        for i in range(NUM_MESSAGES):
            messages.append({
                "user_id": 10000 + i,
                "product_id": 20000 + (i % 2),
                "rating": 4,
                "review_text": f"system smoke {i}",
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")
            })
        produce_reviews(kafka_bootstrap, KAFKA_TOPIC, messages)

        # Start the consumer pointing to same kafka and your online mongo
        env = os.environ.copy()
        env.update({
            "KAFKA_BOOTSTRAP": kafka_bootstrap,
            "KAFKA_TOPIC_REVIEW": KAFKA_TOPIC,
            "MONGO_URI": mongo_uri,
            "MONGO_DB": "system_test_db",
            "ELASTIC_URI": es_url
        })
        proc = subprocess.Popen(["python", "-u", "app/consumer/consumer.py"], env=env, cwd=str(repo_root), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        try:
            # Wait for Mongo to have documents
            deadline = time.time() + 30
            ok = False
            while time.time() < deadline:
                try:
                    cnt = mongo_client["system_test_db"]["reviews"].count_documents({})
                    if cnt >= NUM_MESSAGES:
                        ok = True
                        break
                except Exception:
                    pass
                time.sleep(0.5)
            assert ok, "Expected reviews in Mongo after running consumer"

            # check reviews index presence in ES
            deadline = time.time() + 30
            es_ok = False
            while time.time() < deadline:
                try:
                    if es.indices.exists(index="reviews"):
                        es_ok = True
                        break
                except Exception:
                    pass
                time.sleep(0.5)
            assert es_ok, "Expected 'reviews' index in Elasticsearch"

        finally:
            proc.terminate()
            try:
                proc.wait(timeout=15)
            except Exception:
                proc.kill()
                proc.wait(timeout=5)
            out, err = proc.communicate(timeout=1)
            print("consumer stdout (tail):", out.splitlines()[-50:])
            print("consumer stderr (tail):", err.splitlines()[-50:])

    finally:
        # tear down compose
        subprocess.call(["docker-compose", "-f", DOCKER_COMPOSE_FILE, "down", "--volumes"], cwd=str(repo_root))
