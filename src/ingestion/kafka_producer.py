"""Kafka producer utility for ingestion modules.

Publishes raw documents to the ``social_raw_posts`` topic as JSON.
Designed for fault-tolerance: if Kafka is unavailable the ingestion pipeline
continues normally (dual-write pattern — MongoDB remains the primary raw store).
"""
import json
import logging
from datetime import datetime, timezone
from typing import Optional

from src.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_RAW

logger = logging.getLogger(__name__)

_producer = None          # module-level singleton
_producer_failed = False  # circuit-breaker: skip after first permanent failure


def _get_producer():
    global _producer, _producer_failed
    if _producer_failed:
        return None
    if _producer is not None:
        return _producer
    try:
        from kafka import KafkaProducer  # lazy import

        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, default=str, ensure_ascii=False).encode("utf-8"),
            acks="all",                     # wait for broker ack
            retries=3,
            request_timeout_ms=5_000,       # fail fast so ingestion isn't blocked
            api_version_auto_timeout_ms=5_000,
        )
        logger.info("Kafka producer connected to %s", KAFKA_BOOTSTRAP_SERVERS)
        return _producer
    except Exception as exc:
        logger.warning("Kafka unavailable (%s) — running without Kafka.", exc)
        _producer_failed = True
        return None


def publish_doc(doc: dict, collection: str) -> bool:
    """Publish a raw document dict to the Kafka topic.

    Args:
        doc:        The raw document (same schema as MongoDB raw collections).
        collection: Source collection name, e.g. ``youtube_raw``.

    Returns:
        True if published successfully, False otherwise.
    """
    producer = _get_producer()
    if producer is None:
        return False

    message = {
        "doc_id":     doc.get("doc_id", ""),
        "collection": collection,
        "source":     doc.get("source", "unknown"),
        "brand":      doc.get("brand", "Other"),
        "title":      (doc.get("title") or "")[:500],
        "content":    (doc.get("content") or "")[:5000],
        "url":        (doc.get("url") or "")[:1000],
        "author":     (doc.get("author") or "")[:255],
        "published_at": doc.get("published_at"),
        "crawled_at": datetime.now(timezone.utc).isoformat(),
    }

    try:
        future = producer.send(KAFKA_TOPIC_RAW, value=message)
        future.get(timeout=5)   # block only briefly — ensures delivery
        return True
    except Exception as exc:
        logger.warning("Failed to publish doc %s to Kafka: %s", doc.get("doc_id"), exc)
        return False


def close() -> None:
    """Flush and close the producer (call at process shutdown if needed)."""
    global _producer
    if _producer is not None:
        try:
            _producer.flush(timeout=10)
            _producer.close(timeout=10)
        except Exception:
            pass
        _producer = None
