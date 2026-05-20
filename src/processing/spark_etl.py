"""Spark-based ETL: Kafka batch → PySpark DataFrame → MLlib inference → PostgreSQL.

This replaces the row-by-row Python ETL for large-volume batch processing.
Falls back gracefully when Spark or the model is unavailable.

Flow:
  1. Poll Kafka topic ``social_raw_posts`` (batch via timeout-based consumer)
  2. Deserialise JSON → Spark DataFrame
  3. Text cleaning + feature extraction via loaded MLlib PipelineModel
  4. Bulk JDBC write to PostgreSQL ``posts``
  5. Mark processed in MongoDB (lightweight Python loop after Spark job)
"""
import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Optional

# ── Must be set BEFORE any pyspark import ──────────────────────────────────────
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

logger = logging.getLogger(__name__)

# ── Lazy imports so the module can be imported without Spark installed ─────────
_spark_available = False
try:
    from pyspark.ml import PipelineModel
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col, concat_ws, lit, when
    from pyspark.sql.types import (
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
    _spark_available = True
except ImportError:
    pass


# Kafka consumer timeout – collect messages for up to this many ms before processing
_KAFKA_POLL_TIMEOUT_MS = int(os.getenv("KAFKA_POLL_TIMEOUT_MS", "30000"))
_KAFKA_MAX_RECORDS     = int(os.getenv("KAFKA_MAX_RECORDS", "5000"))

_RAW_SCHEMA = StructType([
    StructField("doc_id",       StringType(), True),
    StructField("collection",   StringType(), True),
    StructField("source",       StringType(), True),
    StructField("brand",        StringType(), True),
    StructField("title",        StringType(), True),
    StructField("content",      StringType(), True),
    StructField("url",          StringType(), True),
    StructField("author",       StringType(), True),
    StructField("published_at", StringType(), True),
    StructField("crawled_at",   StringType(), True),
]) if _spark_available else None


def _build_spark() -> "SparkSession":
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[*]")
        .appName("SocialLearning-SparkETL")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .config(
            "spark.driver.extraJavaOptions",
            "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
            "-Djava.security.manager=allow",
        )
        .config(
            "spark.executor.extraJavaOptions",
            "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
            "-Djava.security.manager=allow",
        )
        .getOrCreate()
    )


def _load_model(model_path: str) -> Optional["PipelineModel"]:
    from pyspark.ml import PipelineModel

    latest_marker = os.path.join(model_path, "latest.txt")
    if os.path.isfile(latest_marker):
        with open(latest_marker) as f:
            versioned_path = f.read().strip()
        if os.path.isdir(versioned_path):
            try:
                model = PipelineModel.load(versioned_path)
                logger.info("Loaded model from %s", versioned_path)
                return model
            except Exception as e:
                logger.warning("Failed to load versioned model: %s", e)

    if os.path.isdir(model_path):
        try:
            model = PipelineModel.load(model_path)
            logger.info("Loaded model from %s", model_path)
            return model
        except Exception as e:
            logger.warning("Failed to load model: %s", e)
    return None


def _poll_kafka(bootstrap_servers: str, topic: str) -> list[dict]:
    """Collect up to _KAFKA_MAX_RECORDS messages from Kafka (batch consumer)."""
    try:
        from kafka import KafkaConsumer
    except ImportError:
        logger.warning("kafka-python not installed — Spark ETL skipped.")
        return []

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        group_id="spark_etl_consumer",
        enable_auto_commit=False,
        consumer_timeout_ms=_KAFKA_POLL_TIMEOUT_MS,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )

    records: list[dict] = []
    try:
        for message in consumer:
            records.append(message.value)
            if len(records) >= _KAFKA_MAX_RECORDS:
                break
    finally:
        if records:
            consumer.commit()
        consumer.close()

    logger.info("Polled %d records from Kafka topic '%s'", len(records), topic)
    return records


def _write_to_postgres(df, jdbc_url: str) -> int:
    """Bulk-write prediction results to PostgreSQL via JDBC."""
    from pyspark.sql.functions import current_timestamp, lit

    out = (
        df.select(
            col("doc_id"),
            col("source"),
            col("brand"),
            col("title"),
            col("content"),
            col("url"),
            col("author"),
            col("published_at"),
            col("crawled_at"),
            col("sentiment"),
            col("sentiment_score"),
            lit("vi").alias("language"),
        )
    )

    count = out.count()

    out.write.format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "posts") \
        .option("driver", "org.postgresql.Driver") \
        .option("batchsize", 500) \
        .mode("append") \
        .save()

    logger.info("Wrote %d rows to PostgreSQL posts table", count)
    return count


def _mark_processed_mongo(doc_ids: list[str], collection_map: dict[str, list[str]]) -> None:
    """Mark documents as processed in MongoDB (Python loop — lightweight)."""
    from src.utils import get_collection

    now = datetime.now(timezone.utc).isoformat()
    for col_name, ids in collection_map.items():
        if not ids:
            continue
        coll = get_collection(col_name)
        coll.update_many(
            {"doc_id": {"$in": ids}},
            {"$set": {"processed_at": now}},
        )
        logger.info("Marked %d docs processed in %s", len(ids), col_name)


def run_spark_etl() -> dict:
    """Entry point: poll Kafka → Spark ETL → PostgreSQL → MongoDB mark done.

    Returns stats dict, e.g. ``{"processed": 42, "errors": 0}``.
    Raises RuntimeError if Spark is not available (caller should fall back).
    """
    if not _spark_available:
        raise RuntimeError("PySpark not installed")

    from src.config import (
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC_RAW,
        MODEL_PATH,
        POSTGRES_URI,
    )

    # 1. Poll Kafka
    records = _poll_kafka(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_RAW)
    if not records:
        logger.info("No Kafka records — Spark ETL nothing to do.")
        return {"processed": 0, "errors": 0}

    spark = _build_spark()
    spark.sparkContext.setLogLevel("WARN")
    stats = {"processed": 0, "errors": 0}

    try:
        # 2. Create DataFrame from Kafka batch
        df = spark.createDataFrame(records, schema=_RAW_SCHEMA)
        df = df.withColumn(
            "text",
            concat_ws(" ", col("title"), col("content")),
        )

        # 3. Load model and predict
        model = _load_model(MODEL_PATH)
        if model is not None:
            df = model.transform(df)
            df = df.withColumn(
                "sentiment",
                when(col("prediction") == 1.0, "positive").otherwise("negative"),
            )
            # Use probability of positive class as score
            from pyspark.sql.functions import udf
            from pyspark.sql.types import FloatType
            from pyspark.ml.linalg import Vector

            def _pos_prob(vec) -> float:
                return float(vec[1]) if vec is not None else 0.5

            pos_prob_udf = udf(_pos_prob, FloatType())
            df = df.withColumn("sentiment_score", pos_prob_udf(col("probability")))
        else:
            logger.warning("No trained model found — using 'neutral' as default sentiment.")
            df = df.withColumn("sentiment", lit("neutral"))
            df = df.withColumn("sentiment_score", lit(0.5))

        # 4. Persist to PostgreSQL
        jdbc_url = POSTGRES_URI.replace("postgresql+psycopg2://", "jdbc:postgresql://")
        stats["processed"] = _write_to_postgres(df, jdbc_url)

        # 5. Mark processed in MongoDB per collection
        collection_map: dict[str, list[str]] = {}
        for row in df.select("doc_id", "collection").toLocalIterator():
            coll = row["collection"] or "unknown_raw"
            collection_map.setdefault(coll, []).append(row["doc_id"])
        _mark_processed_mongo(list(collection_map.keys()), collection_map)

    except Exception as exc:
        logger.error("Spark ETL error: %s", exc, exc_info=True)
        stats["errors"] += 1
        raise
    finally:
        spark.stop()

    logger.info("Spark ETL done: %s", stats)
    return stats
