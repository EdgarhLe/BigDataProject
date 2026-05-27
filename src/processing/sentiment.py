"""Sentiment analysis module.

Priority:
  1. OpenAI API (if OPENAI_API_KEY set) — best accuracy for Vietnamese text
  2. underthesea (offline fallback) — free, no API key required
  3. Keyword-based Vietnamese/English fallback — always works, no dependencies

Returns a dict: {"label": "positive"|"negative"|"neutral", "score": float 0-1}
"""
import logging

from src.config import OPENAI_API_KEY

logger = logging.getLogger(__name__)

# ── OpenAI-based sentiment ─────────────────────────────────────────────────────

_SYSTEM_PROMPT = """Bạn là một chuyên gia phân tích cảm xúc văn bản tiếng Việt và tiếng Anh.
Phân tích cảm xúc của văn bản sau.
Trả về JSON theo định dạng:
{
  "label": "positive"|"negative"|"neutral",
  "score": <0.0-1.0>,
  "emotion": "Vui vẻ"|"Phẫn nộ"|"Buồn bã"|"Ngạc nhiên"|"Bình thường",
  "aspects": {"Tên_khía_cạnh_vd_Pin": "positive"|"negative"|"neutral"}
}
Chỉ trả về JSON hợp lệ."""

def _sentiment_openai(text: str) -> dict:
    import json
    from openai import OpenAI

    client = OpenAI(api_key=OPENAI_API_KEY)
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": text[:1500]},  # cap to save tokens
        ],
        temperature=0,
        max_tokens=200,
        response_format={"type": "json_object"},
    )
    raw = response.choices[0].message.content or "{}"
    result = json.loads(raw)
    
    # Ensure default fields are present
    if "label" not in result: result["label"] = "neutral"
    if "score" not in result: result["score"] = 0.5
    if "emotion" not in result: result["emotion"] = "Bình thường"
    if "aspects" not in result: result["aspects"] = {}
    
    return result


# ── underthesea-based sentiment (offline) ─────────────────────────────────────

def _sentiment_underthesea(text: str) -> dict:
    from underthesea import sentiment

    label_raw = sentiment(text)
    mapping = {
        "positive": ("positive", 0.80),
        "negative": ("negative", 0.80),
        "neutral": ("neutral", 0.60),
    }
    label, score = mapping.get(str(label_raw).lower(), ("neutral", 0.50))
    return {"label": label, "score": score, "emotion": "Bình thường", "aspects": {}}


# ── Keyword-based Vietnamese/English fallback ─────────────────────────────────

_POS_WORDS = [
    # Tiếng Việt
    "tốt", "hay", "tuyệt", "xuất sắc", "thích", "yêu", "ổn", "ok",
    "nhanh", "mượt", "đẹp", "hài lòng", "chất lượng", "đỉnh", "ngon",
    "tiết kiệm", "bền", "xịn", "hiện đại", "mạnh", "an toàn",
    "recommend", "khuyên dùng", "đáng mua", "đáng tiền", "hợp lý",
    "ấn tượng", "thú vị", "sáng tạo", "đột phá", "tiên tiến",
    # English
    "good", "great", "excellent", "amazing", "love", "nice", "fast",
    "smooth", "beautiful", "quality", "recommend", "impressive",
    "innovative", "reliable", "affordable", "worth", "best",
]

_NEG_WORDS = [
    # Tiếng Việt
    "tệ", "kém", "dở", "xấu", "chậm", "lỗi", "hỏng", "vỡ", "chán",
    "thất vọng", "không tốt", "không ổn", "ồn", "pin yếu", "đắt",
    "rởm", "giả", "fake", "nhám", "lag", "treo", "nóng máy",
    "sự cố", "tai nạn", "nguy hiểm", "lo ngại", "lo lắng", "phàn nàn",
    "bức xúc", "mất tiền", "lãng phí", "thất bại", "kém chất lượng",
    # English
    "bad", "poor", "terrible", "worst", "awful", "horrible", "broken",
    "slow", "expensive", "fake", "dangerous", "unsafe", "failure",
    "disappointed", "trash", "useless", "overpriced", "scam",
]


def _sentiment_keywords(text: str) -> dict:
    """Keyword-based Vietnamese/English sentiment — no external dependencies."""
    text_lower = text.lower()
    pos = sum(1 for w in _POS_WORDS if w in text_lower)
    neg = sum(1 for w in _NEG_WORDS if w in text_lower)

    if pos > neg:
        score = min(0.55 + pos * 0.08, 0.92)
        return {"label": "positive", "score": round(score, 4), "emotion": "Bình thường", "aspects": {}}
    elif neg > pos:
        score = min(0.55 + neg * 0.08, 0.92)
        return {"label": "negative", "score": round(score, 4), "emotion": "Bình thường", "aspects": {}}
    return {"label": "neutral", "score": 0.55, "emotion": "Bình thường", "aspects": {}}


# ── Public API ─────────────────────────────────────────────────────────────────

_openai_disabled = False     # circuit breaker: skip after first quota error
_underthesea_disabled = False  # circuit breaker: skip if model not loaded


def analyze_sentiment(text: str) -> dict:
    """Return sentiment dict for given text. Falls back gracefully."""
    global _openai_disabled, _underthesea_disabled
    if not text or not text.strip():
        return {"label": "neutral", "score": 0.5}

    if OPENAI_API_KEY and not _openai_disabled:
        try:
            return _sentiment_openai(text)
        except Exception as e:
            err_str = str(e)
            if "429" in err_str or "quota" in err_str.lower() or "insufficient_quota" in err_str:
                _openai_disabled = True
                logger.warning("OpenAI quota exceeded — disabling for this session")
            else:
                logger.warning("OpenAI sentiment failed, falling back: %s", e)

    if not _underthesea_disabled:
        try:
            return _sentiment_underthesea(text)
        except Exception as e:
            _underthesea_disabled = True
            logger.warning("underthesea disabled for this session: %s", e)

    return _sentiment_keywords(text)
