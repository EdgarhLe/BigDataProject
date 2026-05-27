"""Tuổi Trẻ Online scraper — bài viết từ RSS + trang tìm kiếm.

Luồng hoạt động:
    1. Đọc RSS feed chuyên mục Xe & Kinh doanh của Tuổi Trẻ
    2. Lọc bài theo từ khóa đang cấu hình (không mặc định giới hạn một chủ đề)
    3. Lưu article metadata vào MongoDB collection 'tuoitre_raw'

Lưu ý: Tuổi Trẻ không có public comment API → chỉ thu thập bài viết + mô tả.
"""
import logging
import time
import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config import SERP_API_KEY
from src.utils import detect_brand, make_doc_id, upsert_raw, utcnow, get_track_keywords
from src.ingestion.kafka_producer import publish_doc

logger = logging.getLogger(__name__)

# RSS feeds liên quan
RSS_FEEDS = [
    "https://tuoitre.vn/rss/xe.rss",
    "https://tuoitre.vn/rss/kinh-doanh.rss",
    "https://tuoitre.vn/rss/tin-moi-nhat.rss",
]

# Tìm kiếm bổ sung (rỗng — sử dụng từ khóa động từ TRACK_KEYWORDS nếu cần)
SEARCH_QUERIES = []

SEARCH_URL  = "https://tuoitre.vn/tim-kiem.htm"
MAX_RESULTS = 20

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "vi-VN,vi;q=0.9",
}

# Từ khóa lọc bài liên quan
def get_filter_keywords():
    # Rely solely on dynamic tracking keywords stored in config / DB
    return [kw.lower() for kw in get_track_keywords()]


def _is_relevant(text: str) -> bool:
    text_lower = text.lower()
    return any(kw in text_lower for kw in get_filter_keywords())


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _fetch_rss(url: str) -> list[dict]:
    """Đọc RSS feed, trả về list bài viết."""
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()

    articles = []
    try:
        root = ET.fromstring(resp.content)
        ns   = {"media": "http://search.yahoo.com/mrss/"}
        channel = root.find("channel")
        if channel is None:
            return []

        for item in channel.findall("item")[:MAX_RESULTS]:
            title    = (item.findtext("title") or "").strip()
            link     = (item.findtext("link") or "").strip()
            desc_raw = (item.findtext("description") or "").strip()
            pub_date = (item.findtext("pubDate") or "").strip()

            # Strip HTML từ description
            desc = BeautifulSoup(desc_raw, "lxml").get_text(strip=True) if desc_raw else ""

            combined = f"{title} {desc}"
            if link and title and _is_relevant(combined):
                articles.append({
                    "url":   link,
                    "title": title,
                    "desc":  desc,
                    "date":  pub_date,
                })
    except ET.ParseError as e:
        logger.warning("RSS parse error (%s): %s", url, e)

    return articles


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _search_tuoitre(query: str) -> list[dict]:
    """Tìm kiếm bài viết trên trang tìm kiếm Tuổi Trẻ."""
    params = {"keywords": query}
    resp = requests.get(SEARCH_URL, params=params, headers=HEADERS, timeout=15)
    resp.raise_for_status()

    soup     = BeautifulSoup(resp.text, "lxml")
    articles = []

    for item in soup.select("li.news-item, div.news-item, article")[:MAX_RESULTS]:
        a_tag = item.select_one("h3 a, h2 a, .title a")
        if not a_tag:
            continue
        url   = a_tag.get("href", "")
        title = a_tag.get_text(strip=True)

        if not url.startswith("http"):
            url = "https://tuoitre.vn" + url

        desc_tag = item.select_one("p, .sapo, .description")
        desc = desc_tag.get_text(strip=True) if desc_tag else ""

        if title and _is_relevant(f"{title} {desc}"):
            articles.append({"url": url, "title": title, "desc": desc, "date": ""})

    return articles


def _store_article(article: dict, query: str, stats: dict) -> None:
    url   = article["url"]
    title = article["title"]
    desc  = article["desc"]
    text  = f"{title} {desc}"

    doc = {
        "doc_id":       make_doc_id(url),
        "source":       "tuoitre",
        "content_type": "article",
        "query":        query,
        "title":        title,
        "content":      desc,
        "url":          url,
        "author":       "Tuổi Trẻ",
        "published_at": article.get("date") or None,
        "brand":        detect_brand(text),
        "crawled_at":   utcnow().isoformat(),
    }
    if upsert_raw("tuoitre_raw", doc):
        stats["new"] += 1
        publish_doc(doc, "tuoitre_raw")
    else:
        stats["duplicate"] += 1


def run_tuoitre_ingestion() -> dict:
    """Entry point chính. Trả về thống kê ingestion."""
    stats = {"source": "tuoitre", "new": 0, "duplicate": 0}

    # 1. Thu thập từ RSS
    for feed_url in RSS_FEEDS:
        try:
            articles = _fetch_rss(feed_url)
            logger.info("Tuổi Trẻ RSS '%s' → %d bài liên quan", feed_url, len(articles))
            for article in articles:
                _store_article(article, feed_url, stats)
        except Exception as e:
            logger.error("Tuổi Trẻ RSS fetch failed (%s): %s", feed_url, e)
        time.sleep(0.5)

    # 2. Thu thập từ trang tìm kiếm
    for query in SEARCH_QUERIES:
        try:
            articles = _search_tuoitre(query)
            logger.info("Tuổi Trẻ search '%s' → %d bài", query, len(articles))
            for article in articles:
                _store_article(article, query, stats)
        except Exception as e:
            logger.error("Tuổi Trẻ search failed ('%s'): %s", query, e)
        time.sleep(0.5)

    logger.info("Tuổi Trẻ ingestion done: %s", stats)
    return stats
