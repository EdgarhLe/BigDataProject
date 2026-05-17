"""Reddit API ingestion using PRAW.

Searches across multiple subreddits for posts related to tracked keywords.
Stores posts + their top comments in MongoDB collection 'reddit_raw'.
"""
import logging

import praw
from tenacity import retry, stop_after_attempt, wait_exponential

from src.config import (
    REDDIT_CLIENT_ID,
    REDDIT_CLIENT_SECRET,
    REDDIT_USER_AGENT,
    TRACK_KEYWORDS,
    REDDIT_MAX_POSTS,
)
from src.utils import detect_brand, make_doc_id, upsert_raw, utcnow

logger = logging.getLogger(__name__)

SUBREDDITS = [
    "vietnam",
    "TroChuyenLinhTinh",
    "electricvehicles",
    "cars",
    "asean",
]

# Use top 8 keywords (shorter ones for better Reddit search results)
SEARCH_TERMS = [kw for kw in TRACK_KEYWORDS if len(kw) <= 12][:8]


def _build_reddit() -> praw.Reddit:
    return praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT,
        read_only=True,
    )


def _store_submission(submission, stats: dict) -> None:
    text = f"{submission.title} {submission.selftext or ''}"
    doc = {
        "doc_id": make_doc_id(f"reddit_post_{submission.id}"),
        "source": "reddit",
        "content_type": "post",
        "external_id": submission.id,
        "subreddit": str(submission.subreddit),
        "title": submission.title,
        "content": submission.selftext or "",
        "url": f"https://www.reddit.com{submission.permalink}",
        "author": str(submission.author) if submission.author else "[deleted]",
        "published_at": None if not submission.created_utc else
            __import__("datetime").datetime.fromtimestamp(
                submission.created_utc,
                tz=__import__("datetime").timezone.utc,
            ).isoformat(),
        "score": submission.score,
        "brand": detect_brand(text),
        "crawled_at": utcnow().isoformat(),
    }
    if upsert_raw("reddit_raw", doc):
        stats["new"] += 1
    else:
        stats["duplicate"] += 1


def _store_comments(submission, stats: dict, top_n: int = 20) -> None:
    try:
        submission.comments.replace_more(limit=0)
        for comment in list(submission.comments)[:top_n]:
            text = comment.body or ""
            doc = {
                "doc_id": make_doc_id(f"reddit_comment_{comment.id}"),
                "source": "reddit",
                "content_type": "comment",
                "external_id": comment.id,
                "post_id": submission.id,
                "subreddit": str(submission.subreddit),
                "title": "",
                "content": text,
                "url": f"https://www.reddit.com{comment.permalink}",
                "author": str(comment.author) if comment.author else "[deleted]",
                "published_at": None if not comment.created_utc else
                    __import__("datetime").datetime.fromtimestamp(
                        comment.created_utc,
                        tz=__import__("datetime").timezone.utc,
                    ).isoformat(),
                "score": comment.score,
                "brand": detect_brand(text),
                "crawled_at": utcnow().isoformat(),
            }
            if upsert_raw("reddit_raw", doc):
                stats["new"] += 1
            else:
                stats["duplicate"] += 1
    except Exception as e:
        logger.warning("Failed to fetch comments for %s: %s", submission.id, e)


def run_reddit_ingestion() -> dict:
    """Main entry point. Returns ingestion stats."""
    if not REDDIT_CLIENT_ID or not REDDIT_CLIENT_SECRET:
        logger.warning("Reddit credentials not set — skipping Reddit ingestion.")
        return {"source": "reddit", "new": 0, "duplicate": 0, "error": "no credentials"}

    reddit = _build_reddit()
    stats = {"source": "reddit", "new": 0, "duplicate": 0}

    for subreddit_name in SUBREDDITS:
        subreddit = reddit.subreddit(subreddit_name)
        for term in SEARCH_TERMS:
            try:
                results = subreddit.search(
                    term,
                    sort="new",
                    time_filter="week",
                    limit=max(10, REDDIT_MAX_POSTS // len(SEARCH_TERMS)),
                )
                for submission in results:
                    _store_submission(submission, stats)
                    _store_comments(submission, stats)
            except Exception as e:
                logger.error("Reddit search failed (%s / %s): %s", subreddit_name, term, e)

    logger.info("Reddit ingestion done: %s", stats)
    return stats


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(run_reddit_ingestion())
