"""ETL pipeline: MongoDB (raw) → sentiment analysis → PostgreSQL (processed).

For each collection (youtube_raw, reddit_raw, google_news_raw), reads documents
that have not yet been processed (no 'processed_at' field), runs sentiment
analysis, maps brands, and writes to the PostgreSQL 'posts' table.
Also updates the daily_summary aggregate table.
"""
import logging
from datetime import datetime, timezone, date

from sqlalchemy import create_engine, text

from src.config import POSTGRES_URI
from src.processing.sentiment import analyze_sentiment
from src.utils import detect_brand, get_collection, utcnow

logger = logging.getLogger(__name__)

RAW_COLLECTIONS = ["youtube_raw", "google_news_raw", "vnexpress_raw", "tuoitre_raw"]

_engine = None


def _get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(POSTGRES_URI, pool_pre_ping=True)
    return _engine


def _upsert_post(conn, doc: dict, sentiment: dict) -> None:
    published_at = doc.get("published_at")
    if isinstance(published_at, str):
        try:
            published_at = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        except ValueError:
            published_at = None

    conn.execute(
        text("""
            INSERT INTO posts
                (doc_id, source, brand, title, content, url, author,
                 published_at, crawled_at, sentiment, sentiment_score, language)
            VALUES
                (:doc_id, :source, :brand, :title, :content, :url, :author,
                 :published_at, :crawled_at, :sentiment, :sentiment_score, :language)
            ON CONFLICT (doc_id) DO UPDATE SET
                sentiment       = EXCLUDED.sentiment,
                sentiment_score = EXCLUDED.sentiment_score
        """),
        {
            "doc_id": doc["doc_id"],
            "source": doc.get("source", "unknown"),
            "brand": doc.get("brand", "Other"),
            "title": (doc.get("title") or "")[:500],
            "content": (doc.get("content") or "")[:5000],
            "url": (doc.get("url") or "")[:1000],
            "author": (doc.get("author") or "")[:255],
            "published_at": published_at,
            "crawled_at": utcnow(),
            "sentiment": sentiment["label"],
            "sentiment_score": sentiment["score"],
            "language": "vi",
        },
    )


# Explicit mapping avoids any SQL injection risk from AI-generated sentiment labels
_SENTIMENT_SQL: dict[str, str] = {
    "positive": """
        INSERT INTO daily_summary (date, brand, source, total_mentions, positive_count)
        VALUES (:date, :brand, :source, 1, 1)
        ON CONFLICT (date, brand, source) DO UPDATE SET
            total_mentions = daily_summary.total_mentions + 1,
            positive_count = daily_summary.positive_count + 1
    """,
    "negative": """
        INSERT INTO daily_summary (date, brand, source, total_mentions, negative_count)
        VALUES (:date, :brand, :source, 1, 1)
        ON CONFLICT (date, brand, source) DO UPDATE SET
            total_mentions = daily_summary.total_mentions + 1,
            negative_count = daily_summary.negative_count + 1
    """,
    "neutral": """
        INSERT INTO daily_summary (date, brand, source, total_mentions, neutral_count)
        VALUES (:date, :brand, :source, 1, 1)
        ON CONFLICT (date, brand, source) DO UPDATE SET
            total_mentions = daily_summary.total_mentions + 1,
            neutral_count  = daily_summary.neutral_count + 1
    """,
}


def _update_daily_summary(conn, brand: str, source: str, sentiment_label: str, pub_date: date) -> None:
    sql = _SENTIMENT_SQL.get(sentiment_label, _SENTIMENT_SQL["neutral"])
    conn.execute(text(sql), {"date": pub_date, "brand": brand, "source": source})


def run_etl() -> dict:
    """Process all unprocessed raw documents.

    Tries Spark-based ETL first (Kafka batch → MLlib inference → PostgreSQL JDBC).
    Falls back to the row-by-row Python ETL if Spark or Kafka is unavailable.
    Returns stats dict.
    """
    try:
        from src.processing.spark_etl import run_spark_etl
        stats = run_spark_etl()
        # If Spark processed nothing it may mean Kafka was empty; run Python ETL
        # as a safety net to catch any docs written directly to MongoDB without Kafka.
        if stats.get("processed", 0) == 0:
            logger.info("Spark ETL found nothing — running Python ETL as fallback.")
            stats = _run_python_etl()
        return stats
    except Exception as exc:
        logger.warning("Spark ETL unavailable (%s) — falling back to Python ETL.", exc)
        return _run_python_etl()


def _run_python_etl() -> dict:
    engine = _get_engine()
    stats = {"processed": 0, "skipped": 0, "errors": 0}

    with engine.begin() as conn:
        for col_name in RAW_COLLECTIONS:
            collection = get_collection(col_name)
            # Find documents not yet processed
            cursor = collection.find({"processed_at": {"$exists": False}})

            for doc in cursor:
                try:
                    content = (doc.get("content") or "") + " " + (doc.get("title") or "")
                    content = content.strip()

                    # Re-run brand detection (may be missing from older docs)
                    brand = doc.get("brand") or detect_brand(content) or "Other"
                    doc["brand"] = brand

                    sentiment = analyze_sentiment(content)

                    _upsert_post(conn, doc, sentiment)

                    # Update daily_summary
                    pub_at = doc.get("published_at")
                    if pub_at:
                        try:
                            if isinstance(pub_at, str):
                                pub_at = datetime.fromisoformat(pub_at.replace("Z", "+00:00"))
                            _update_daily_summary(conn, brand, doc.get("source", "unknown"),
                                                  sentiment["label"], pub_at.date())
                        except Exception:
                            pass  # Non-critical

                    # Mark as processed in MongoDB
                    collection.update_one(
                        {"_id": doc["_id"]},
                        {"$set": {"processed_at": utcnow().isoformat(),
                                  "sentiment": sentiment["label"],
                                  "sentiment_score": sentiment["score"]}},
                    )
                    stats["processed"] += 1

                except Exception as e:
                    logger.error("ETL error for doc %s: %s", doc.get("doc_id"), e)
                    stats["errors"] += 1

    logger.info("ETL done: %s", stats)
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(run_etl())
