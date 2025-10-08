"""
DAG: mongo -> elasticsearch daily reindex (based on ObjectId timestamps)
Uses environment variables loaded from .env (via python-dotenv).

Required environment variables (example .env):
ELASTIC_URI=http://user:pass@es-host:9200
MONGO_URI=mongodb://user:pass@mongo-host:27017
MONGO_DB=my_db
ELASTIC_INDEX=reviews
ELASTIC_BULK_SIZE=500
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Iterable, List, Dict, Any, Set

from dotenv import load_dotenv
load_dotenv()

import logging
from bson.objectid import ObjectId
from pymongo import MongoClient
from elasticsearch import Elasticsearch, helpers as es_helpers

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

# Environment variables (from your snippet)
ELASTIC_URI = os.getenv("ELASTIC_URI")
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
ELASTIC_INDEX = os.getenv("ELASTIC_INDEX", "reviews")
# default to 500 when not provided or provided as small number; user provided 10 in snippet but likely wants larger.
ELASTIC_BULK_SIZE = int(os.getenv("ELASTIC_BULK_SIZE", 10))

# Safe defaults and sanity checks
if not (ELASTIC_URI and MONGO_URI and MONGO_DB):
    raise RuntimeError("Environment variables ELASTIC_URI, MONGO_URI and MONGO_DB must be set.")

# Tuneable constants (can be env-driven)
MGET_BATCH = int(os.getenv("ELASTIC_MGET_BATCH", 500))
MONGO_CURSOR_BATCH = int(os.getenv("MONGO_CURSOR_BATCH", 1000))
ES_REQUEST_TIMEOUT = int(os.getenv("ES_REQUEST_TIMEOUT", 60))


def _time_window_from_context(context: dict, dag_tz: str = "UTC") -> tuple[datetime, datetime]:
    """
    Determine start/end datetimes for this DAG run.
    Prefer `data_interval_start`/`data_interval_end` when present (Airflow 2+).
    Fallback: use logical_date's day midnight -> next midnight in DAG timezone.
    Returns timezone-aware datetimes.
    """
    tz = timezone.get_timezone(dag_tz)

    if context.get("data_interval_start"):
        start = context["data_interval_start"]
        end = context.get("data_interval_end") or (start + timedelta(days=1))
        # Ensure in requested tz
        start = timezone.convert_to_utc(start).astimezone(tz)
        end = timezone.convert_to_utc(end).astimezone(tz)
        LOG.info("Using data_interval window %s -> %s", start.isoformat(), end.isoformat())
        return start, end

    # fallback to logical_date / execution_date midnight -> next day midnight
    logical_date = context.get("logical_date") or context.get("execution_date") or timezone.now(tz)
    start = logical_date.astimezone(tz).replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    LOG.info("Using fallback day window %s -> %s", start.isoformat(), end.isoformat())
    return start, end


def _objectid_bounds_from_window(start_dt: datetime, end_dt: datetime) -> tuple[ObjectId, ObjectId]:
    """
    Convert datetimes to ObjectId bounds.
    ObjectId.from_datetime uses the timestamp portion only (UTC).
    We'll query for _id >= start_obj and _id < end_obj.
    """
    # ObjectId.from_datetime expects naive datetime in UTC or aware datetime; we'll convert to UTC naive
    start_utc = start_dt.astimezone(timezone.utc).replace(tzinfo=None)
    end_utc = end_dt.astimezone(timezone.utc).replace(tzinfo=None)
    return ObjectId.from_datetime(start_utc), ObjectId.from_datetime(end_utc)


def _iter_id_chunks_for_window(mongo_coll, start_obj: ObjectId, end_obj: ObjectId,
                              chunk_size: int = 1000) -> Iterable[List[str]]:
    """
    Stream _id hex strings for documents with ObjectId timestamp in [start_obj, end_obj),
    yielding lists of hex ids in chunks of size `chunk_size`.
    """
    cursor = mongo_coll.find(
        {"_id": {"$gte": start_obj, "$lt": end_obj}},
        projection={"_id": 1},
        no_cursor_timeout=True,
        batch_size=MONGO_CURSOR_BATCH,
    )
    try:
        chunk: List[str] = []
        for doc in cursor:
            chunk.append(str(doc["_id"]))
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk
    finally:
        try:
            cursor.close()
        except Exception:
            pass


def _existing_ids_in_es(es_client: Elasticsearch, index: str, ids: List[str]) -> Set[str]:
    """
    Use ES mget (may be large) but chunk to avoid too-large requests.
    Returns set of ids that exist in ES.
    """
    existing: Set[str] = set()
    for i in range(0, len(ids), MGET_BATCH):
        batch = ids[i : i + MGET_BATCH]
        resp = es_client.mget(index=index, body={"ids": batch}, request_timeout=ES_REQUEST_TIMEOUT)
        for d in resp.get("docs", []):
            if d.get("found"):
                existing.add(str(d.get("_id")))
    return existing


def _fetch_docs_by_ids(mongo_coll, ids: List[str]) -> List[Dict[str, Any]]:
    """
    Fetch documents from Mongo for the provided hex id strings.
    Converts top-level _id to str for ES.
    """
    object_ids = [ObjectId(h) for h in ids]
    docs_cursor = mongo_coll.find({"_id": {"$in": object_ids}})
    docs: List[Dict[str, Any]] = []
    for doc in docs_cursor:
        doc["_id"] = str(doc["_id"])
        # If needed, normalize other non-JSON serializable types here
        docs.append(doc)
    return docs


def _prepare_es_actions(index: str, docs: List[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
    """
    Create actions for elasticsearch.helpers.bulk.
    """
    for doc in docs:
        _id = doc.get("_id")
        yield {
            "_op_type": "index",
            "_index": index,
            "_id": _id,
            "_source": doc,
        }


def reindex_missing_documents(**context):
    """
    Main operator function:
      1. Determine window
      2. Stream ids from Mongo in the window
      3. For each chunk, find which are missing in ES via mget
      4. Fetch missing docs and bulk index them
    """
    # Build DB/clients from env
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB]
    coll_name = os.getenv("MONGO_COLLECTION", "reviews")  # optional: MONGO_COLLECTION
    mongo_coll = mongo_db[coll_name]

    es_client = Elasticsearch([ELASTIC_URI])

    # Time window
    start_dt, end_dt = _time_window_from_context(context, dag_tz=os.getenv("DAG_TIMEZONE", "UTC"))
    start_obj, end_obj = _objectid_bounds_from_window(start_dt, end_dt)

    total_candidates = 0
    total_indexed = 0

    # Stream candidate id-chunks from Mongo
    for id_chunk in _iter_id_chunks_for_window(mongo_coll, start_obj, end_obj, chunk_size=ELASTIC_BULK_SIZE * 4):
        total_candidates += len(id_chunk)
        LOG.info("Processing candidate id chunk (size=%d)", len(id_chunk))

        # Ask ES which ones already exist
        existing = _existing_ids_in_es(es_client, ELASTIC_INDEX, id_chunk)
        missing = [i for i in id_chunk if i not in existing]

        LOG.info("Chunk summary: %d candidates, %d existing in ES, %d missing", len(id_chunk), len(existing), len(missing))

        if not missing:
            continue

        # Fetch missing docs from Mongo in batches and bulk index
        for i in range(0, len(missing), ELASTIC_BULK_SIZE):
            batch_ids = missing[i : i + ELASTIC_BULK_SIZE]
            docs = _fetch_docs_by_ids(mongo_coll, batch_ids)
            if not docs:
                LOG.warning("No docs retrieved from Mongo for ids: %s", batch_ids[:5])
                continue

            actions = list(_prepare_es_actions(ELASTIC_INDEX, docs))
            # helpers.bulk returns (success_count, errors) only in older versions; for safety wrap in try/except
            try:
                resp = es_helpers.bulk(es_client, actions, chunk_size=ELASTIC_BULK_SIZE, request_timeout=ES_REQUEST_TIMEOUT)
                # resp is a tuple (success_count, errors) depending on client; we log the success count if available
                if isinstance(resp, tuple) and len(resp) >= 1:
                    LOG.info("Bulk index result: %s", resp[0])
                total_indexed += len(actions)
                LOG.info("Indexed %d documents (batch size=%d).", len(actions), len(actions))
            except Exception as exc:
                # surface the exception so Airflow retry can handle it
                LOG.exception("Bulk indexing failed for batch starting with id %s. Raising to trigger retry.", batch_ids[0])
                raise

    LOG.info("Reindex run complete. Candidates scanned=%d, documents indexed=%d", total_candidates, total_indexed)
    # Close clients
    try:
        mongo_client.close()
    except Exception:
        pass


# ---------- Airflow DAG ----------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="mongo_to_es_reindex_env",
    default_args=default_args,
    description="Re-index MongoDB documents (ObjectId timestamp) into Elasticsearch using .env configuration",
    start_date=timezone.datetime(2025, 10, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["mongo", "elasticsearch", "indexing"],
) as dag:

    task_reindex_missing = PythonOperator(
        task_id="reindex_missing_documents",
        python_callable=reindex_missing_documents,
        # PythonOperator will pass context to callables that accept **kwargs in modern Airflow.
        retries=3,
        retry_delay=timedelta(minutes=5),
    )

    task_reindex_missing
