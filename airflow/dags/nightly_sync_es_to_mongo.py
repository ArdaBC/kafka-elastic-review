"""
DAG: mongo -> elasticsearch daily reindex (based on ObjectId timestamps)
Uses environment variables loaded from .env (via python-dotenv).

Required environment variables (example .env):
ELASTIC_URI_AIRFLOW=http://user:pass@es-host:9200
MONGO_URI=mongodb://user:pass@mongo-host:27017
MONGO_DB=my_db
ELASTIC_INDEX=reviews
ELASTIC_BULK_SIZE=500
"""
from __future__ import annotations

import os
import logging
import time
from datetime import datetime, timedelta
from typing import Iterable, List, Dict, Any, Set

from dotenv import load_dotenv

load_dotenv()

from bson.objectid import ObjectId
from pymongo import MongoClient
from elasticsearch import Elasticsearch, helpers as es_helpers
from elasticsearch.helpers import BulkIndexError

from airflow import DAG
from airflow.utils import timezone
from airflow.operators.python import PythonOperator

import pendulum

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.INFO)

# Environment variables (from your snippet) - default elastic host to service name used in docker-compose
ELASTIC_URI_AIRFLOW = os.getenv("ELASTIC_URI_AIRFLOW", "http://elasticsearch:9200")
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
ELASTIC_INDEX = os.getenv("ELASTIC_INDEX", "reviews")
ELASTIC_BULK_SIZE = int(os.getenv("ELASTIC_BULK_SIZE", 10))

# Safe defaults and sanity checks
if not (ELASTIC_URI_AIRFLOW and MONGO_URI and MONGO_DB):
    raise RuntimeError("Environment variables ELASTIC_URI_AIRFLOW, MONGO_URI and MONGO_DB must be set.")

# Tunables
MGET_BATCH = int(os.getenv("ELASTIC_MGET_BATCH", 500))
MONGO_CURSOR_BATCH = int(os.getenv("MONGO_CURSOR_BATCH", 1000))
ES_REQUEST_TIMEOUT = int(os.getenv("ES_REQUEST_TIMEOUT", 60))


def _time_window_from_context(context: dict, dag_tz: str = "UTC") -> tuple[datetime, datetime]:
    """
    Daily window for the current day in `dag_tz`:
      - start: midnight (00:00:00) of *today* in dag_tz
      - end: now() in dag_tz

    This intentionally ignores Airflow data_interval/logical_date and
    returns a dynamic window that updates daily.
    """
    tz = pendulum.timezone(dag_tz)
    now = pendulum.now(tz)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    LOG.info("Using daily window %s -> %s", start.isoformat(), now.isoformat())
    return start, now



def _objectid_bounds_from_window(start_dt: datetime, end_dt: datetime) -> tuple[ObjectId, ObjectId]:
    """
    Convert datetimes to ObjectId bounds.
    ObjectId.from_datetime uses the timestamp portion only (UTC).
    We'll query for _id >= start_obj and _id < end_obj.
    """
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
        batch = ids[i: i + MGET_BATCH]
        try:
            resp = es_client.mget(index=index, body={"ids": batch}, request_timeout=ES_REQUEST_TIMEOUT)
        except Exception as exc:
            LOG.warning("mget failed for batch starting with %s: %s", batch[0] if batch else None, exc, exc_info=True)
            # On connectivity or other intermittent errors return empty set (we will attempt full insert)
            continue
        for d in resp.get("docs", []):
            # mget returns _id and found flag
            if d.get("found"):
                existing.add(str(d.get("_id")))
    return existing


def _normalize_value(v):
    """
    Convert Mongo types to JSON-serializable values for ES indexing.
    - datetime -> ISO format string
    - ObjectId -> str
    - recursively handle lists/dicts
    """
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, ObjectId):
        return str(v)
    if isinstance(v, dict):
        return {k: _normalize_value(val) for k, val in v.items()}
    if isinstance(v, (list, tuple)):
        return [_normalize_value(x) for x in v]
    # add other conversions if needed
    return v


def _fetch_docs_by_ids(mongo_coll, ids: List[str]) -> List[Dict[str, Any]]:
    """
    Fetch documents from Mongo for the provided hex id strings.
    Converts top-level _id to 'id' and removes leading-underscore fields, normalizes common BSON types.
    """
    object_ids = [ObjectId(h) for h in ids]
    docs_cursor = mongo_coll.find({"_id": {"$in": object_ids}})
    docs: List[Dict[str, Any]] = []
    for doc in docs_cursor:
        original_id = doc.get("_id")
        # remove the _id from source to avoid underscore-prefixed field issues in ES mappings
        if "_id" in doc:
            del doc["_id"]
        # normalize nested fields and ObjectIds/datetimes
        normalized = {k: _normalize_value(v) for k, v in doc.items()}
        # expose the id as a normal field (not underscore) in the document source
        normalized["id"] = str(original_id)
        docs.append(normalized)
    return docs


def _prepare_es_actions(index: str, docs: List[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
    """
    Create actions for elasticsearch.helpers.bulk.
    Note: we set ES internal _id from the 'id' field and keep the source free of '_id' fields.
    """
    for doc in docs:
        _id = doc.get("id")
        # keep the source without the internal id field if you prefer (we keep id as well)
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
      4. Fetch missing docs and bulk index them (with per-document fallback for failures)
    """
    # Build DB/clients from env
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB]
    coll_name = os.getenv("MONGO_COLLECTION", "reviews")
    mongo_coll = mongo_db[coll_name]

    es_client = Elasticsearch(ELASTIC_URI_AIRFLOW)

    # quick ES connectivity check (gives clearer error early)
    try:
        info = es_client.info(request_timeout=ES_REQUEST_TIMEOUT)
        LOG.info("Connected to ES node %s (cluster %s)", info.get("name"), info.get("cluster_name"))
    except Exception as exc:
        LOG.exception("Cannot connect to Elasticsearch at %s: %s", ELASTIC_URI_AIRFLOW, exc)
        # Let Airflow retry by raising
        raise

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
            batch_ids = missing[i: i + ELASTIC_BULK_SIZE]
            docs = _fetch_docs_by_ids(mongo_coll, batch_ids)
            if not docs:
                LOG.warning("No docs retrieved from Mongo for ids: %s", batch_ids[:5])
                continue

            actions = list(_prepare_es_actions(ELASTIC_INDEX, docs))

            # Attempt bulk index with robust handling of BulkIndexError
            try:
                resp = es_helpers.bulk(es_client, actions, chunk_size=ELASTIC_BULK_SIZE, request_timeout=ES_REQUEST_TIMEOUT)
                # older clients may return (count, errors). Log success
                if isinstance(resp, tuple) and len(resp) >= 1:
                    LOG.info("Bulk index result: %s", resp[0])
                    total_indexed += len(actions)
                    LOG.info("Indexed %d documents (batch size=%d).", len(actions), len(actions))
                else:
                    # If no tuple, assume success (bulk didn't raise)
                    total_indexed += len(actions)
                    LOG.info("Bulk index completed for %d documents.", len(actions))
            except BulkIndexError as bie:
                # BulkIndexError contains the list of errors for failed documents
                LOG.error("BulkIndexError: %s", str(bie))
                # try to extract errors list if present
                errors = getattr(bie, "errors", None)
                LOG.info("Attempting per-document retry for the failed batch...")
                # fallback: try index documents one-by-one so one bad doc doesn't fail whole batch
                for action in actions:
                    _id = action.get("_id")
                    _source = action.get("_source")
                    try:
                        es_client.index(index=action["_index"], id=_id, document=_source, request_timeout=ES_REQUEST_TIMEOUT)
                        total_indexed += 1
                    except Exception as e:
                        LOG.exception("Failed to index document id=%s individually, skipping. Error: %s", _id, e)
                LOG.info("Per-document retry complete for this batch.")
            except Exception as exc:
                # unexpected error during bulk; log and raise for Airflow retry policy
                LOG.exception("Unexpected exception during bulk indexing. Raising to trigger retry.")
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
        retries=3,
        retry_delay=timedelta(minutes=5),
    )

    task_reindex_missing
