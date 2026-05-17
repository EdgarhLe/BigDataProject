"""Pipeline health check — verifies all services are reachable and operational."""
import sys
from datetime import datetime, timezone

from sqlalchemy import create_engine, text

from src.config import (
    MONGO_DB, POSTGRES_URI,
    YOUTUBE_API_KEY, REDDIT_CLIENT_ID, SERP_API_KEY,
    TELEGRAM_BOT_TOKEN,
)
from src.utils import get_mongo_client

PASS = "✅"
FAIL = "❌"
WARN = "⚠️ "


def _check(label: str, fn) -> bool:
    try:
        fn()
        print(f"  {PASS}  {label}")
        return True
    except Exception as e:
        print(f"  {FAIL}  {label}: {e}")
        return False


def run_healthcheck() -> bool:
    print("\n" + "=" * 55)
    print("  PIPELINE HEALTH CHECK")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 55)

    results = []

    # ── Databases ──────────────────────────────────────────────────────────────
    print("\n[Databases]")

    def check_mongo():
        client = get_mongo_client()
        client.admin.command("ping")

    def check_postgres():
        engine = create_engine(POSTGRES_URI, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

    results.append(_check("MongoDB reachable", check_mongo))
    results.append(_check("PostgreSQL reachable", check_postgres))

    # ── Table schema ───────────────────────────────────────────────────────────
    def check_schema():
        engine = create_engine(POSTGRES_URI)
        with engine.connect() as conn:
            conn.execute(text("SELECT id, doc_id, sentiment, alerted_at FROM posts LIMIT 1"))

    results.append(_check("PostgreSQL schema (posts table)", check_schema))

    # ── Collections ───────────────────────────────────────────────────────────
    print("\n[MongoDB Collections]")
    client = get_mongo_client()
    db = client[MONGO_DB]
    for col_name in ["youtube_raw", "reddit_raw", "google_news_raw"]:
        count = db[col_name].count_documents({}) if col_name in db.list_collection_names() else 0
        status = PASS if count >= 0 else FAIL
        print(f"  {status}  {col_name:<22} ({count:,} documents)")

    # ── API Keys ───────────────────────────────────────────────────────────────
    print("\n[API Keys configured]")
    key_checks = [
        ("YouTube API Key",  bool(YOUTUBE_API_KEY)),
        ("Reddit Client ID", bool(REDDIT_CLIENT_ID)),
        ("SerpAPI Key",      bool(SERP_API_KEY)),
        ("Telegram Bot Token", bool(TELEGRAM_BOT_TOKEN)),
    ]
    for label, ok in key_checks:
        icon = PASS if ok else WARN
        note = "" if ok else "(not set — source will be skipped)"
        print(f"  {icon}  {label:<22} {note}")

    # ── PostgreSQL record counts ───────────────────────────────────────────────
    print("\n[PostgreSQL data]")
    try:
        engine = create_engine(POSTGRES_URI)
        with engine.connect() as conn:
            total = conn.execute(text("SELECT COUNT(*) FROM posts")).scalar()
            negative = conn.execute(
                text("SELECT COUNT(*) FROM posts WHERE sentiment='negative'")
            ).scalar()
            unalerted = conn.execute(
                text("SELECT COUNT(*) FROM posts WHERE sentiment='negative' AND alerted_at IS NULL")
            ).scalar()
        print(f"  {PASS}  Total posts processed : {total:,}")
        print(f"  {PASS}  Negative posts        : {negative:,}")
        print(f"  {PASS}  Unalerted negatives   : {unalerted:,}")
    except Exception as e:
        print(f"  {FAIL}  Could not query posts: {e}")
        results.append(False)

    # ── Summary ────────────────────────────────────────────────────────────────
    passed = sum(results)
    total_checks = len(results)
    print("\n" + "=" * 55)
    overall = passed == total_checks
    icon = PASS if overall else FAIL
    print(f"  {icon}  {passed}/{total_checks} checks passed")
    print("=" * 55 + "\n")
    return overall


if __name__ == "__main__":
    ok = run_healthcheck()
    sys.exit(0 if ok else 1)
