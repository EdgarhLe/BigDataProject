"""Google News ingestion via SerpAPI.

Fetches news articles matching tracked keywords using Google's news search.
Stores article metadata in MongoDB collection 'google_news_raw'.
"""
import logging
import time

import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config import SERP_API_KEY, TRACK_KEYWORDS, SERP_MAX_PAGES
from src.utils import detect_brand, make_doc_id, upsert_raw, utcnow
from src.ingestion.kafka_producer import publish_doc

logger = logging.getLogger(__name__)

SERP_ENDPOINT = "https://serpapi.com/search"

# Mỗi query tập trung vào 1 cụm thương hiệu xe máy điện
SEARCH_QUERIES = [
    "VinFast xe máy điện",
    "Dat Bike Weaver Quantum Việt Nam",
    "Selex Yadea Dibao xe máy điện",
    "Honda Icon e CUV e UC3 xe điện Việt Nam",
    "xe máy điện Việt Nam 2025",
]


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=3, max=15))
def _fetch_news_page(query: str, page: int) -> list[dict]:
    params = {
        "engine": "google",
        "q": query,
        "tbm": "nws",          # news tab
        "gl": "vn",            # country: Vietnam
        "hl": "vi",            # language: Vietnamese
        "num": 10,
        "start": page * 10,
        "api_key": SERP_API_KEY,
    }
    response = requests.get(SERP_ENDPOINT, params=params, timeout=15)
    response.raise_for_status()
    data = response.json()
    return data.get("news_results", [])


def run_google_news_ingestion() -> dict:
    """Main entry point. Returns ingestion stats."""
    if not SERP_API_KEY:
        logger.warning("SERP_API_KEY not set — skipping Google News ingestion.")
        return {"source": "google_news", "new": 0, "duplicate": 0, "error": "no api key"}

    stats = {"source": "google_news", "new": 0, "duplicate": 0}

    for query in SEARCH_QUERIES:
        for page in range(SERP_MAX_PAGES):
            try:
                articles = _fetch_news_page(query, page)
                if not articles:
                    break  # No more results for this query

                for article in articles:
                    url = article.get("link", "")
                    if not url:
                        continue
                    title = article.get("title", "")
                    snippet = article.get("snippet", "")
                    text = f"{title} {snippet}"

                    doc = {
                        "doc_id": make_doc_id(url),
                        "source": "google_news",
                        "content_type": "article",
                        "query": query,
                        "title": title,
                        "content": snippet,
                        "url": url,
                        "author": article.get("source", ""),
                        "published_at": article.get("date"),
                        "thumbnail": article.get("thumbnail"),
                        "brand": detect_brand(text),
                        "crawled_at": utcnow().isoformat(),
                    }
                    if upsert_raw("google_news_raw", doc):
                        stats["new"] += 1
                        publish_doc(doc, "google_news_raw")
                    else:
                        stats["duplicate"] += 1

                # Polite delay between pages to respect rate limits
                time.sleep(1)

            except Exception as e:
                logger.error("Google News fetch failed (query='%s', page=%d): %s", query, page, e)
                break  # Move to next query on error

    logger.info("Google News ingestion done: %s", stats)
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(run_google_news_ingestion())
