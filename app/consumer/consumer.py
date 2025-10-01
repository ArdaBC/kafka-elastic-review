import signal, json, os, datetime, logging, sys
from confluent_kafka import Consumer, KafkaException
from elasticsearch import Elasticsearch
from pymongo import MongoClient
from dotenv import load_dotenv
from elasticsearch import helpers
from utils import create_indices
from datetime import timezone
from logging.handlers import TimedRotatingFileHandler


load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")
KAFKA_TOPIC_REVIEW = os.getenv("KAFKA_TOPIC_REVIEW")
ELASTIC_URI = os.getenv("ELASTIC_URI", "http://localhost:9200")
ELASTIC_USER = os.getenv("ELASTIC_USER")
ELASTIC_PASSWORD = os.getenv("ELASTIC_PASSWORD")

ELASTIC_BULK_SIZE = int(os.getenv("ELASTIC_BULK_SIZE",10))
KAFKA_BULK_SIZE = int(os.getenv("KAFKA_BULK_SIZE",10))


LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG").upper()
# -------------------------------------------

# ---------- JSON logger to stdout ----------
class JsonFormatter(logging.Formatter):
    def format(self, record):
        ts = datetime.datetime.now(timezone.utc).replace(tzinfo=timezone.utc).isoformat()
        base = {
            "timestamp": ts,
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "line": record.lineno,
            "process": record.process,
            "thread": record.thread
        }
        if record.exc_info:
            base["exc_info"] = self.formatException(record.exc_info)
        extra = getattr(record, "extra", None)
        if isinstance(extra, dict):
            base.update(extra)
        return json.dumps(base, default=str, ensure_ascii=False)

# --- log directory relative to this file ---
current_dir = os.path.dirname(os.path.abspath(__file__))  # consumer.py folder
log_dir = os.path.join(current_dir, "../logs")            # ../logs
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "consumer.log")

# --- StreamHandler (stdout) ---
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(JsonFormatter())

# --- FileHandler (writes to consumer.log) ---
file_handler = TimedRotatingFileHandler(
    log_file,
    when="midnight",   # rotate at midnight
    interval=1,
    backupCount=7      # keep 7 days
)
file_handler.setFormatter(JsonFormatter())

# --- Logger setup ---
logger = logging.getLogger("review-consumer")
logger.setLevel(logging.DEBUG)
logger.addHandler(stream_handler)
logger.addHandler(file_handler)
logger.propagate = False

logger.info("Logger initialized", extra={"extra": {"log_file": log_file}})
# -------------------------------------------


bulk_actions = []

running = True

def create_consumer():
    # Allow overriding kafka bootstrap servers via env for tests and CI.
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    return Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": "review-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300000,
        "fetch.max.bytes": 5 * 1024 * 1024
    })

def handle_shutdown(sig, frame):
    global running
    logger.info("shutdown_signal_received", extra={"extra": {"signal": sig}})
    running = False 


def main():
    global running, bulk_actions

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    reviews = db["reviews"]
    users = db["users"]
    products = db["products"]

    #Check MongoDB connection
    try:
        client.admin.command("ping")
        logger.info("mongodb_connected")
    except Exception as e:
        logger.exception("mongodb_connection_failed")
        raise

    es = None
    try:
        es = Elasticsearch(ELASTIC_URI, verify_certs=False, request_timeout=30)
        if es.ping():
            logger.info("elasticsearch_connected", extra={"extra": {"uri": ELASTIC_URI}})
            try:
                create_indices(es)
            except Exception:
                logger.exception("create_indices_failed")
        else:
            logger.warning("elasticsearch_not_responding", extra={"extra": {"uri": ELASTIC_URI}})
            es = None
    except Exception:
        logger.exception("elasticsearch_connection_error")
        es = None

    consumer = create_consumer()
    consumer.subscribe([KAFKA_TOPIC_REVIEW])

    #Register signals
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    processed = 0

    try:
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("kafka_consumer_error", extra={"extra": {"error": str(msg.error())}})
                raise KafkaException(msg.error())
            try:
                review = json.loads(msg.value().decode("utf-8"))

                #Insert into Mongo
                result = reviews.insert_one(review)
                logger.debug("inserted_review_mongo", extra={"extra": {"mongo_id": str(result.inserted_id)}})

                users.update_one(
                    {
                        "user_id": review["user_id"]
                    },
                    {
                        "$inc": {
                            "total_ratings": review["rating"], 
                            "total_reviews": 1
                        }
                    },
                    upsert=True
                )

                products.update_one(
                    {
                        "product_id": review["product_id"]
                    },
                    {
                        "$inc": {
                            "total_ratings": review["rating"], 
                            "total_reviews": 1
                        }
                    },
                    upsert=True
                )


                #Compute avg_rating, upsert into Elasticsearch for Kibana
                if es:
                    try:
                        prod = products.find_one({"product_id": review["product_id"]})
                        if prod:
                            total_ratings = float(prod.get("total_ratings", 0.0))
                            total_reviews = int(prod.get("total_reviews", 0))
                            avg_rating = total_ratings / total_reviews if total_reviews > 0 else 0.0

                            product_doc = {
                                "product_id": prod.get("product_id"),
                                "name": prod.get("name"),
                                "total_ratings": total_ratings,
                                "total_reviews": total_reviews,
                                "avg_rating": avg_rating
                            }

                            es.index(index="products", id=str(product_doc["product_id"]), document=product_doc)

                        usr = users.find_one({"user_id": review["user_id"]})
                        if usr:
                            total_ratings = float(usr.get("total_ratings", 0.0))
                            total_reviews = int(usr.get("total_reviews", 0))
                            avg_rating = total_ratings / total_reviews if total_reviews > 0 else 0.0

                            #Exclude password from indexing
                            user_doc = {
                                "user_id": usr.get("user_id"),
                                "name": usr.get("name"),
                                "email": usr.get("email"),
                                "phone": usr.get("phone"),
                                "total_ratings": total_ratings,
                                "total_reviews": total_reviews,
                                "avg_rating": avg_rating
                            }

                            es.index(index="users", id=str(user_doc["user_id"]), document=user_doc)

                    except Exception as e:
                        logger.exception("failed_update_user_product_summary_es")

                inserted_id = result.inserted_id
                doc_id = str(inserted_id)
                doc = {k: v for k, v in review.items() if k != "_id"}

                #Make timestamp is string
                if isinstance(doc.get("timestamp"), datetime.datetime):
                    doc["timestamp"] = doc["timestamp"].isoformat()

                if es:
                    bulk_actions.append({
                        "_index": "reviews",
                        "_id": doc_id,
                        "_source": doc
                    })
                else:
                    logger.debug("skipped_review_indexing", extra={"extra": {"doc_id": doc_id}})

                processed += 1

                logger.info("review_consumed", extra={"extra": {"kafka_offset": msg.offset(), "topic": msg.topic(), "processed_count": processed}})

                if processed % KAFKA_BULK_SIZE == 0:
                    consumer.commit(asynchronous=False)
                    logger.debug("consumer_committed", extra={"extra": {"processed_since_last_commit": KAFKA_BULK_SIZE}})

                if len(bulk_actions) >= ELASTIC_BULK_SIZE and es:
                    try:
                        helpers.bulk(es, bulk_actions)
                        logger.info("bulk_index_success", extra={"extra": {"count": len(bulk_actions)}})
                    except Exception:
                        logger.exception("bulk_index_error")
                    bulk_actions.clear()

            except Exception as e:
                logger.exception("error_processing_message")
                try:
                    consumer.commit(message=msg)  # Skip bad record
                except Exception:
                    logger.exception("failed_commit_on_bad_record")


    finally:
        logger.info("closing_consumer")

        if bulk_actions and es:
            try:
                helpers.bulk(es, bulk_actions)
            except Exception:
                logger.exception("error_performing_final_bulk")
            bulk_actions.clear()
        try:
            consumer.commit(asynchronous=False)
        except Exception:
            logger.exception("consumer_commit_failed_on_shutdown")
        try:
            consumer.close()
        except Exception:
            logger.exception("consumer_close_failed")
        logger.info("consumer_closed_cleanly")



if __name__ == "__main__":
    main()