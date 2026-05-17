"""Telegram alert bot.

Queries PostgreSQL for posts with high negative sentiment that haven't been
alerted yet (no 'alerted_at' timestamp), and sends a formatted Telegram message.
"""
import logging
from datetime import datetime, timezone

import requests
from sqlalchemy import create_engine, text

from src.config import (
    POSTGRES_URI,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    ALERT_NEGATIVE_THRESHOLD,
)

logger = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


def _send_message(message: str) -> bool:
    """Send a single Telegram message. Returns True on success."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram credentials not set — skipping alert.")
        return False
    try:
        url = TELEGRAM_API.format(token=TELEGRAM_BOT_TOKEN)
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        return True
    except Exception as e:
        logger.error("Telegram send failed: %s", e)
        return False


def _format_alert(row: dict) -> str:
    source_emoji = {"youtube": "▶️", "reddit": "🤖", "google_news": "📰"}.get(
        row.get("source", ""), "📢"
    )
    brand = row.get("brand", "Unknown")
    sentiment_score = row.get("sentiment_score", 0)
    title = (row.get("title") or row.get("content") or "")[:200]
    url = row.get("url", "")
    author = row.get("author", "Unknown")

    return (
        f"🚨 <b>CẢNH BÁO — Nội dung tiêu cực về {brand}</b>\n\n"
        f"{source_emoji} <b>Nguồn:</b> {row.get('source', '').upper()}\n"
        f"👤 <b>Tác giả:</b> {author}\n"
        f"📉 <b>Điểm tiêu cực:</b> {sentiment_score:.0%}\n\n"
        f"📝 <i>{title}</i>\n\n"
        f'🔗 <a href="{url}">Xem bài viết</a>'
    )


def send_negative_alerts() -> dict:
    """Find unalerted negative posts and send Telegram messages. Returns stats."""
    engine = create_engine(POSTGRES_URI, pool_pre_ping=True)
    stats = {"sent": 0, "skipped": 0, "errors": 0}

    with engine.begin() as conn:
        rows = conn.execute(
            text("""
                SELECT id, source, brand, title, content, url, author,
                       sentiment_score
                FROM posts
                WHERE sentiment = 'negative'
                  AND sentiment_score >= :threshold
                  AND alerted_at IS NULL
                ORDER BY sentiment_score DESC
                LIMIT 20
            """),
            {"threshold": ALERT_NEGATIVE_THRESHOLD},
        ).mappings().all()

        for row in rows:
            row_dict = dict(row)
            message = _format_alert(row_dict)
            success = _send_message(message)
            if success:
                # Mark as alerted
                conn.execute(
                    text("UPDATE posts SET alerted_at = NOW() WHERE id = :id"),
                    {"id": row_dict["id"]},
                )
                stats["sent"] += 1
            else:
                stats["skipped"] += 1

    logger.info("Alert stats: %s", stats)
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(send_negative_alerts())
