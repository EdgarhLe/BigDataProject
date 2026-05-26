"""YouTube Data API v3 ingestion.

Fetches videos matching tracked keywords, then fetches their top comments.
Each comment is stored as a raw document in MongoDB collection 'youtube_raw'.
"""
import logging
from datetime import datetime, timezone

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config import YOUTUBE_API_KEY, YOUTUBE_MAX_RESULTS
from src.utils import detect_brand, make_doc_id, upsert_raw, utcnow, get_track_keywords
from src.ingestion.kafka_producer import publish_doc

logger = logging.getLogger(__name__)

def get_brand_queries():
    base_keywords = get_track_keywords()
    if not base_keywords:
        return ["xe máy điện"]
    return base_keywords  # search directly with the keyword

def _build_service():
    return build("youtube", "v3", developerKey=YOUTUBE_API_KEY, cache_discovery=False)


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _search_videos(service, query: str, max_results: int) -> list[dict]:
    response = (
        service.search()
        .list(
            q=query,
            part="id,snippet",
            type="video",
            maxResults=min(max_results, 50),
            relevanceLanguage="vi",
            order="date",
        )
        .execute()
    )
    return response.get("items", [])


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _fetch_comments(service, video_id: str, max_results: int = 100) -> list[dict]:
    try:
        response = (
            service.commentThreads()
            .list(
                part="snippet",
                videoId=video_id,
                maxResults=min(max_results, 100),
                order="relevance",
                textFormat="plainText",
            )
            .execute()
        )
        return response.get("items", [])
    except HttpError as e:
        if e.resp.status in (403, 404):
            # Comments disabled or video not found
            return []
        raise


def run_youtube_ingestion() -> dict:
    """Main entry point. Returns ingestion stats."""
    if not YOUTUBE_API_KEY:
        logger.warning("YOUTUBE_API_KEY not set — skipping YouTube ingestion.")
        return {"source": "youtube", "new": 0, "duplicate": 0, "error": "no api key"}

    service = _build_service()
    stats = {"source": "youtube", "new": 0, "duplicate": 0}

    seen_video_ids: set[str] = set()
    all_videos: list[dict] = []
    brand_queries = get_brand_queries()
    for query in brand_queries:
        try:
            results = _search_videos(service, query, max(YOUTUBE_MAX_RESULTS // len(brand_queries), 10))
            logger.info("Found %d videos for query '%s'", len(results), query)
            all_videos.extend(results)
        except HttpError as e:
            logger.error("YouTube search failed for '%s': %s", query, e)

    for video_item in all_videos:
        video_id = video_item["id"].get("videoId")
        if not video_id or video_id in seen_video_ids:
            continue
        seen_video_ids.add(video_id)

        snippet = video_item["snippet"]
        # Store the video itself
        video_doc = {
            "doc_id": make_doc_id(f"yt_video_{video_id}"),
            "source": "youtube",
            "content_type": "video",
            "external_id": video_id,
            "title": snippet.get("title", ""),
            "content": snippet.get("description", ""),
            "url": f"https://www.youtube.com/watch?v={video_id}",
            "author": snippet.get("channelTitle", ""),
            "published_at": snippet.get("publishedAt"),
            "brand": detect_brand(snippet.get("title", "") + " " + snippet.get("description", "")),
            "crawled_at": utcnow().isoformat(),
        }
        is_new = upsert_raw("youtube_raw", video_doc)
        if is_new:
            stats["new"] += 1
            publish_doc(video_doc, "youtube_raw")
        else:
            stats["duplicate"] += 1

        # Fetch comments for this video
        comments = _fetch_comments(service, video_id)
        for thread in comments:
            top = thread["snippet"]["topLevelComment"]["snippet"]
            text = top.get("textDisplay", "")
            comment_id = thread["id"]
            doc = {
                "doc_id": make_doc_id(f"yt_comment_{comment_id}"),
                "source": "youtube",
                "content_type": "comment",
                "external_id": comment_id,
                "video_id": video_id,
                "title": "",
                "content": text,
                "url": f"https://www.youtube.com/watch?v={video_id}&lc={comment_id}",
                "author": top.get("authorDisplayName", ""),
                "published_at": top.get("publishedAt"),
                "brand": detect_brand(text),
                "crawled_at": utcnow().isoformat(),
            }
            is_new = upsert_raw("youtube_raw", doc)
            if is_new:
                stats["new"] += 1
                publish_doc(doc, "youtube_raw")
            else:
                stats["duplicate"] += 1

    logger.info("YouTube ingestion done: %s", stats)
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(run_youtube_ingestion())
