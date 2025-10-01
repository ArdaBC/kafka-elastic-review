# app/tests/integration/test_kafka_mongo_elastic.py
"""
Integration test (minimal + Elasticsearch verification)

What this test does:
- Loads environment variables from the repository .env (so you don't need to export MONGO_URI manually).
- Starts ephemeral Kafka and Elasticsearch using testcontainers.
- Produces N messages to Kafka.
- Starts your consumer as a subprocess configured to point at:
    - ephemeral Kafka (KAFKA_BOOTSTRAP)
    - ephemeral Elasticsearch (ELASTIC_URI)
    - your existing Mongo (MONGO_URI read from .env)
- Waits and asserts:
    - documents present in Mongo (reviews collection)
    - 'reviews' index exists in Elasticsearch and contains at least expected_count docs
Notes:
- Requires Docker (testcontainers) locally or on the CI agent.
- Requires the one-line consumer change so create_consumer reads KAFKA_BOOTSTRAP.
"""

import os
import time
import json
import subprocess
from pathlib import Path

import pytest
from confluent_kafka import Producer
from pymongo import MongoClient
from elasticsearch import Elasticsearch

# testcontainers imports
from testcontainers.kafka import KafkaContainer
from testcontainers.elasticsearch import ElasticSearchContainer

# Use pytest marker for filtering if desired
pytestmark = pytest.mark.integration

# timeouts (seconds)
MONGO_WAIT_TIMEOUT = 40
ES_WAIT_TIMEOUT = 40
KAFKA_FLUSH_TIMEOUT = 10
CONSUMER_SHUTDOWN_TIMEOUT = 15

def load_dotenv_from_repo_root():
    """
    Load .env from repository root so tests use the same environment variables
    as your application (MONGO_URI etc.). This removes the need to manually
    export MONGO_URI before running tests.
    """
    try:
        # local import to avoid requiring python-dotenv unless test runs
        from dotenv import load_dotenv
    except Exception as e:
        raise RuntimeError("python-dotenv is required for this test. Ensure you appended test deps to requirements.txt") from e

    repo_root = Path(__file__).resolve().parents[3]
    dotenv_path = repo_root / ".env"
    if dotenv_path.exists():
        load_dotenv(dotenv_path)
    else:
        # If no .env, env vars must be present in the environment.
        pass


def produce_reviews(bootstrap: str, topic: str, reviews: list):
    """Produce messages to Kafka synchronously."""
    p = Producer({"bootstrap.servers": bootstrap})
    for r in reviews:
        p.produce(topic=topic, key=str(r.get("user_id", "k")), value=json.dumps(r))
    p.flush(KAFKA_FLUSH_TIMEOUT)


def wait_for_mongo_doc_count(mongo_uri: str, db_name: str, coll_name: str, expected_count: int, timeout: int = MONGO_WAIT_TIMEOUT):
    """Poll Mongo until collection has at least expected_count documents."""
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=2000)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            client.admin.command("ping")
            count = client[db_name][coll_name].count_documents({})
            if count >= expected_count:
                return count
        except Exception:
            # keep polling until timeout
            pass
        time.sleep(0.5)
    raise TimeoutError(f"Timed out waiting for {db_name}.{coll_name} >= {expected_count}")


def wait_for_es_count(es_client: Elasticsearch, index: str, expected_count: int, timeout: int = ES_WAIT_TIMEOUT):
    """Poll Elasticsearch until index exists and has at least expected_count documents."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if es_client.indices.exists(index=index):
                # _count is light-weight and reliable
                resp = es_client.count(index=index)
                if resp.get("count", 0) >= expected_count:
                    return resp.get("count", 0)
        except Exception:
            # possible transient errors while ES cluster forms; keep trying
            pass
        time.sleep(0.5)
    raise TimeoutError(f"Timed out waiting for ES index '{index}' to reach {expected_count} docs")


def _start_consumer_process(env: dict):
    """
    Start the consumer script as a subprocess. We run it with -u (unbuffered)
    so logs are available immediately in CI.
    """
    repo_root = Path(__file__).resolve().parents[3]
    cmd = ["python", "-u", "app/consumer/consumer.py"]
    return subprocess.Popen(cmd, env=env, cwd=str(repo_root), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def test_kafka_to_mongo_and_elastic():
    """
    Full minimal integration:
    - ephemeral Kafka via testcontainers
    - ephemeral Elasticsearch via testcontainers
    - use your ONLINE Mongo (MONGO_URI from .env)
    """

    # Load .env from repo root (so MONGO_URI is picked up automatically)
    load_dotenv_from_repo_root()

    # Read MONGO_URI from the environment loaded above (or from system env)
    mongo_uri = os.getenv("MONGO_URI")
    if not mongo_uri:
        pytest.skip("MONGO_URI not set in .env or environment; this test expects an accessible MongoDB")

    # Use a fresh test DB to avoid colliding with other data in your online DB
    test_db = f"test_reviews_db_{int(time.time())}"

    # basic test parameters
    KAFKA_TOPIC = "integration_test_reviews"
    NUM_MESSAGES = 4

    # Start ephemeral Kafka and ES containers
    with KafkaContainer() as kafka_cont, ElasticSearchContainer() as es_cont:
        # normalize Kafka bootstrap server (KafkaContainer returns PLAINTEXT://host:port sometimes)
        kafka_bootstrap = kafka_cont.get_bootstrap_server()
        if kafka_bootstrap.startswith("PLAINTEXT://"):
            kafka_bootstrap = kafka_bootstrap.split("PLAINTEXT://", 1)[1]

        # ES URL (testcontainer provides e.g. http://host:port)
        es_url = es_cont.get_url()

        # Prepare a few review messages
        messages = []
        for i in range(NUM_MESSAGES):
            messages.append({
                "user_id": 2000 + i,
                "product_id": 3000 + (i % 2),
                "rating": 5,
                "review_text": f"integration + es test {i}",
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S")
            })

        # Produce messages to Kafka BEFORE starting consumer (consumer may pick them up when spun)
        produce_reviews(kafka_bootstrap, KAFKA_TOPIC, messages)

        # Start the consumer process with environment pointing at ephemeral Kafka and ES, and online Mongo
        env = os.environ.copy()
        env.update({
            # ephemeral Kafka bootstrap for consumer to connect to
            "KAFKA_BOOTSTRAP": kafka_bootstrap,
            # the topic we produced to above
            "KAFKA_TOPIC_REVIEW": KAFKA_TOPIC,
            # use test DB name to avoid colliding with production data
            "MONGO_URI": mongo_uri,
            "MONGO_DB": test_db,
            # point consumer's ES connection to the ephemeral ES container
            "ELASTIC_URI": es_url,
            "LOG_LEVEL": "DEBUG",
            # ensure consumer bulk sizes are small to flush quickly in tests (optional)
            "ELASTIC_BULK_SIZE": "2",
            "KAFKA_BULK_SIZE": "2",
        })

        # Launch consumer subprocess
        proc = _start_consumer_process(env)
        try:
            # 1) Wait for Mongo to show inserted review docs (consumer inserts into Mongo)
            wait_for_mongo_doc_count(mongo_uri, test_db, "reviews", expected_count=NUM_MESSAGES, timeout=MONGO_WAIT_TIMEOUT)

            # 2) Wait for Elasticsearch index 'reviews' to exist and contain at least NUM_MESSAGES documents
            es_client = Elasticsearch(es_url)
            wait_for_es_count(es_client, "reviews", expected_count=NUM_MESSAGES, timeout=ES_WAIT_TIMEOUT)

        finally:
            # Ask consumer to shut down cleanly and gather logs for debugging
            proc.terminate()
            try:
                proc.wait(timeout=CONSUMER_SHUTDOWN_TIMEOUT)
            except Exception:
                proc.kill()
                proc.wait(timeout=5)

            # capture last output for CI logs
            try:
                out, err = proc.communicate(timeout=1)
            except Exception:
                out, err = "", ""
            print("=== consumer stdout (tail) ===")
            print("\n".join(out.splitlines()[-100:]))
            print("=== consumer stderr (tail) ===")
            print("\n".join(err.splitlines()[-100:]))

            # optional cleanup in the online Mongo: remove the test DB so you don't leave test data (comment out if you want to inspect)
            try:
                client = MongoClient(mongo_uri)
                client.drop_database(test_db)
            except Exception:
                # don't fail the test due to cleanup failure
                pass
