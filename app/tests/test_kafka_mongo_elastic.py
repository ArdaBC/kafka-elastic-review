import os
import time
import json

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
    raise RuntimeError("MONGO_URI is not set in .env")

MONGO_WAIT_TIMEOUT = int(os.getenv("MONGO_WAIT_TIMEOUT", "60"))
ES_WAIT_TIMEOUT = int(os.getenv("ES_WAIT_TIMEOUT", "180"))
KAFKA_FLUSH_TIMEOUT = int(os.getenv("KAFKA_FLUSH_TIMEOUT", "10"))
CONSUMER_SHUTDOWN_TIMEOUT = int(os.getenv("CONSUMER_SHUTDOWN_TIMEOUT", "10"))
KAFKA_TEST_TOPIC = os.getenv("KAFKA_TEST_TOPIC", "reviews")
NUM_TEST_MESSAGES = int(os.getenv("NUM_TEST_MESSAGES", "5"))


def produce_reviews(bootstrap: str, topic: str, reviews: list):
    p = Producer({"bootstrap.servers": bootstrap})
    for r in reviews:
        p.produce(topic=topic, key=str(r.get("user_id", "k")), value=json.dumps(r))
    p.flush(KAFKA_FLUSH_TIMEOUT)


def wait_for_mongo_doc_count(
    mongo_uri: str,
    db_name: str,
    coll_name: str,
    expected_count: int,
    timeout: int = MONGO_WAIT_TIMEOUT,
):
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
    raise TimeoutError(
        f"Timed out waiting for {db_name}.{coll_name} >= {expected_count}"
    )


def wait_for_es_count(
    es_client: Elasticsearch,
    index: str,
    expected_count: int,
    timeout: int = ES_WAIT_TIMEOUT,
):
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
    raise TimeoutError(
        f"Timed out waiting for ES index '{index}' to reach {expected_count} docs"
    )


def test_kafka_to_elasticsearch_integration():
    with KafkaContainer(image="confluentinc/cp-kafka:7.6.1") as kafka_cont:
        es_cont = (
            ElasticSearchContainer(
                image="docker.elastic.co/elasticsearch/elasticsearch:8.15.0"
            )
            .with_env("discovery.type", "single-node")
            .with_env("xpack.security.enabled", "false")
            .with_env("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
        )

        es_cont.start()
        try:
            kafka_bootstrap = kafka_cont.get_bootstrap_server()
            es_url = es_cont.get_url()

            es = Elasticsearch(es_url, verify_certs=False)
            print(f"Waiting for Elasticsearch at {es_url} ...")

            max_wait = 120  # seconds
            for i in range(max_wait):
                try:
                    if es.ping():
                        print("Elasticsearch is ready!")
                        break
                except Exception:
                    pass
                time.sleep(1)
            else:
                logs = es_cont.get_logs().decode("utf-8", errors="ignore")
                print("=== Elasticsearch container logs (tail) ===")
                print("\n".join(logs.splitlines()[-100:]))
                pytest.fail("Elasticsearch did not start in time")

            # Simple test indexing
            index_name = "test_index"
            doc = {"msg": "hello es"}
            es.index(index=index_name, id=1, document=doc)
            es.indices.refresh(index=index_name)

            count = es.count(index=index_name)["count"]
            assert count == 1, "Elasticsearch index count mismatch"

        finally:
            es_cont.stop()
