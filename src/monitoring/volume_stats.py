"""Volume statistics and data scale estimation (Giai đoạn 5 — Báo cáo).

Prints a full report of:
  - Total records per source / per collection in MongoDB
  - Processed records in PostgreSQL with sentiment breakdown
  - Daily ingestion rate (records/day)
  - Estimated weekly/monthly projection for báo cáo Big Data
  - Pipeline health status (last run per asset)
"""
import logging
from datetime import datetime, timezone, timedelta

from sqlalchemy import create_engine, text

from src.config import MONGO_DB, POSTGRES_URI
from src.utils import get_mongo_client

logger = logging.getLogger(__name__)

COLLECTIONS = ["youtube_raw", "reddit_raw", "google_news_raw"]


# ── MongoDB stats ──────────────────────────────────────────────────────────────

def _mongo_stats() -> dict:
    client = get_mongo_client()
    db = client[MONGO_DB]
    stats = {}
    total = 0
    for col_name in COLLECTIONS:
        col = db[col_name]
        count = col.count_documents({})
        processed = col.count_documents({"processed_at": {"$exists": True}})
        stats[col_name] = {"total": count, "processed": processed, "pending": count - processed}
        total += count
    stats["_total_raw"] = total
    return stats


# ── PostgreSQL stats ───────────────────────────────────────────────────────────

def _postgres_stats() -> dict:
    engine = create_engine(POSTGRES_URI, pool_pre_ping=True)
    with engine.connect() as conn:
        # Overall counts
        row = conn.execute(text("SELECT COUNT(*) FROM posts")).scalar()
        total_posts = row or 0

        # Sentiment breakdown
        sent_rows = conn.execute(
            text("SELECT sentiment, COUNT(*) as cnt FROM posts GROUP BY sentiment")
        ).fetchall()
        sentiment = {r[0]: r[1] for r in sent_rows}

        # Brand breakdown
        brand_rows = conn.execute(
            text("SELECT brand, COUNT(*) as cnt FROM posts GROUP BY brand ORDER BY cnt DESC")
        ).fetchall()
        brands = {r[0]: r[1] for r in brand_rows}

        # Source breakdown
        source_rows = conn.execute(
            text("SELECT source, COUNT(*) as cnt FROM posts GROUP BY source")
        ).fetchall()
        sources = {r[0]: r[1] for r in source_rows}

        # Daily rate (last 7 days)
        daily_rows = conn.execute(
            text("""
                SELECT DATE(crawled_at) as day, COUNT(*) as cnt
                FROM posts
                WHERE crawled_at >= NOW() - INTERVAL '7 days'
                GROUP BY day ORDER BY day
            """)
        ).fetchall()
        daily = {str(r[0]): r[1] for r in daily_rows}

        # Alerted posts
        alerted = conn.execute(
            text("SELECT COUNT(*) FROM posts WHERE alerted_at IS NOT NULL")
        ).scalar() or 0

    return {
        "total_processed": total_posts,
        "sentiment": sentiment,
        "brands": brands,
        "sources": sources,
        "daily_last_7d": daily,
        "alerts_sent": alerted,
    }


# ── Volume projection ──────────────────────────────────────────────────────────

def _volume_projection(daily_stats: dict) -> dict:
    days = list(daily_stats.values())
    if not days:
        return {"avg_per_day": 0, "weekly": 0, "monthly": 0, "yearly": 0}
    avg = sum(days) / len(days)
    return {
        "avg_per_day": round(avg),
        "weekly": round(avg * 7),
        "monthly": round(avg * 30),
        "yearly": round(avg * 365),
    }


# ── Report printer ─────────────────────────────────────────────────────────────

def print_volume_report() -> dict:
    """Print a formatted volume/stats report and return the raw data."""
    print("\n" + "=" * 60)
    print("  SOCIAL LISTENING — DATA VOLUME REPORT")
    print(f"  Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)

    # MongoDB raw data
    try:
        mongo = _mongo_stats()
        print("\n[RAW DATA — MongoDB]")
        for col, s in mongo.items():
            if col.startswith("_"):
                continue
            print(f"  {col:<22} total={s['total']:>6,}  processed={s['processed']:>6,}  pending={s['pending']:>6,}")
        print(f"  {'TOTAL RAW':<22} {mongo['_total_raw']:>6,}")
    except Exception as e:
        print(f"  MongoDB unreachable: {e}")
        mongo = {}

    # PostgreSQL processed data
    try:
        pg = _postgres_stats()
        print("\n[PROCESSED DATA — PostgreSQL]")
        print(f"  Total posts in DB : {pg['total_processed']:,}")

        print("\n  Sentiment breakdown:")
        for label, cnt in pg["sentiment"].items():
            pct = cnt / pg["total_processed"] * 100 if pg["total_processed"] else 0
            print(f"    {label:<12} {cnt:>6,}  ({pct:.1f}%)")

        print("\n  Brand breakdown:")
        for brand, cnt in pg["brands"].items():
            pct = cnt / pg["total_processed"] * 100 if pg["total_processed"] else 0
            print(f"    {brand:<16} {cnt:>6,}  ({pct:.1f}%)")

        print("\n  Source breakdown:")
        for src, cnt in pg["sources"].items():
            print(f"    {src:<18} {cnt:>6,}")

        print(f"\n  Telegram alerts sent: {pg['alerts_sent']:,}")

        # Projection
        proj = _volume_projection(pg["daily_last_7d"])
        print("\n[VOLUME PROJECTION (for báo cáo Big Data)]")
        print(f"  Avg posts/day  : {proj['avg_per_day']:>8,}")
        print(f"  Projected/week : {proj['weekly']:>8,}")
        print(f"  Projected/month: {proj['monthly']:>8,}")
        print(f"  Projected/year : {proj['yearly']:>8,}")
        print("\n  Note: Estimates based on last 7 days of crawling rate.")

    except Exception as e:
        print(f"  PostgreSQL unreachable: {e}")
        pg = {}

    print("\n" + "=" * 60 + "\n")
    return {"mongo": mongo, "postgres": pg}


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING)
    print_volume_report()
