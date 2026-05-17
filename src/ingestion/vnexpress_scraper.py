"""VnExpress scraper — bài viết + bình luận.

Luồng hoạt động:
  1. Tìm bài viết theo keyword qua trang tìm kiếm VnExpress
  2. Trích xuất article ID từ URL (số cuối trước .html)
  3. Lấy bình luận qua API công khai của VnExpress
  4. Lưu vào MongoDB collection 'vnexpress_raw'
"""
import logging
import re
import time

import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

from src.utils import detect_brand, make_doc_id, upsert_raw, utcnow

logger = logging.getLogger(__name__)

SEARCH_QUERIES = [
    "VinFast xe điện",
    "BYD Việt Nam",
    "Xiaomi Auto SU7",
    "xe điện 2024 2025",
]

SEARCH_URL   = "https://timkiem.vnexpress.net/"
COMMENT_API  = "https://usi-saas.vnexpress.net/index/get"
MAX_ARTICLES = 10   # số bài mỗi keyword
MAX_COMMENTS = 50   # số bình luận mỗi bài

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "vi-VN,vi;q=0.9",
}


def _extract_article_id(url: str) -> str | None:
    """Trích ID bài viết từ URL VnExpress. VD: ...bai-viet-4XXXXXX.html → 4XXXXXX"""
    match = re.search(r"-(\d{7,})\.html$", url)
    return match.group(1) if match else None


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _search_articles(query: str) -> list[dict]:
    """Tìm kiếm bài viết trên VnExpress, trả về list {url, title, description, date}."""
    params = {"q": query, "media_type": "article"}
    resp = requests.get(SEARCH_URL, params=params, headers=HEADERS, timeout=15)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")
    articles = []

    for item in soup.select("article.item-news, div.item-news")[:MAX_ARTICLES]:
        a_tag = item.select_one("h3.title-news a, h2.title-news a")
        if not a_tag:
            continue
        url   = a_tag.get("href", "")
        title = a_tag.get_text(strip=True)

        desc_tag = item.select_one("p.description")
        desc = desc_tag.get_text(strip=True) if desc_tag else ""

        date_tag = item.select_one("span.time-count, span.time_ago")
        date_str = date_tag.get_text(strip=True) if date_tag else ""

        if url and title:
            articles.append({"url": url, "title": title, "description": desc, "date": date_str})

    return articles


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _fetch_comments(article_id: str) -> list[dict]:
    """Gọi API bình luận công khai của VnExpress."""
    params = {
        "offset": 0,
        "limit": MAX_COMMENTS,
        "objectid": article_id,
        "objecttype": 1,
        "siteid": 1000000,
    }
    resp = requests.get(COMMENT_API, params=params, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    return data.get("data", {}).get("items", [])


def run_vnexpress_ingestion() -> dict:
    """Entry point chính. Trả về thống kê ingestion."""
    stats = {"source": "vnexpress", "new": 0, "duplicate": 0}

    for query in SEARCH_QUERIES:
        try:
            articles = _search_articles(query)
            logger.info("VnExpress query '%s' → %d bài", query, len(articles))
        except Exception as e:
            logger.error("VnExpress search failed for '%s': %s", query, e)
            continue

        for article in articles:
            url        = article["url"]
            title      = article["title"]
            desc       = article["description"]
            article_id = _extract_article_id(url)
            text       = f"{title} {desc}"

            # Lưu bài viết
            doc = {
                "doc_id":       make_doc_id(url),
                "source":       "vnexpress",
                "content_type": "article",
                "query":        query,
                "title":        title,
                "content":      desc,
                "url":          url,
                "author":       "VnExpress",
                "published_at": article.get("date"),
                "brand":        detect_brand(text),
                "crawled_at":   utcnow().isoformat(),
            }
            if upsert_raw("vnexpress_raw", doc):
                stats["new"] += 1
            else:
                stats["duplicate"] += 1

            # Lấy bình luận nếu có article_id
            if article_id:
                try:
                    comments = _fetch_comments(article_id)
                    for cmt in comments:
                        cmt_content = cmt.get("content", "") or ""
                        cmt_id      = str(cmt.get("comment_id", ""))
                        if not cmt_content or not cmt_id:
                            continue
                        cmt_doc = {
                            "doc_id":       make_doc_id(f"vnexpress_comment_{cmt_id}"),
                            "source":       "vnexpress",
                            "content_type": "comment",
                            "article_id":   article_id,
                            "title":        "",
                            "content":      cmt_content,
                            "url":          url,
                            "author":       cmt.get("full_name", "Ẩn danh"),
                            "published_at": None,
                            "brand":        detect_brand(f"{title} {cmt_content}"),
                            "crawled_at":   utcnow().isoformat(),
                        }
                        if upsert_raw("vnexpress_raw", cmt_doc):
                            stats["new"] += 1
                        else:
                            stats["duplicate"] += 1
                except Exception as e:
                    logger.warning("Lấy bình luận thất bại (article %s): %s", article_id, e)

            time.sleep(0.5)  # tránh spam request

    logger.info("VnExpress ingestion done: %s", stats)
    return stats
