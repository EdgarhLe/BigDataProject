"""Shared utilities: MongoDB client, deduplication, brand mapping."""
import hashlib
from datetime import datetime, timezone
from pymongo import MongoClient, ASCENDING
from pymongo.collection import Collection
from src.config import MONGO_URI, MONGO_DB

# ── Brand disambiguation dictionary ───────────────────────────────────────────
BRAND_MAP: dict[str, str] = {
    # VinFast (xe máy điện)
    "vinfast": "VinFast",
    "evo200": "VinFast",   "evo 200": "VinFast",
    "feliz s": "VinFast", "feliz": "VinFast",
    "klara s": "VinFast", "klara": "VinFast",
    "vento s": "VinFast", "vento": "VinFast",
    "theon s": "VinFast", "theon": "VinFast",
    "rasad": "VinFast",
    "sadie": "VinFast",
    "saxil": "VinFast",
    # Dat Bike
    "dat bike": "Dat Bike",
    "datbike": "Dat Bike",
    "weaver++": "Dat Bike", "weaver": "Dat Bike",
    "dat bike quantum": "Dat Bike",
    # Selex Motors (substring dài trước để tránh match nhầm)
    "selex motors": "Selex Motors",
    "selex camel": "Selex Motors",
    "selex": "Selex Motors",
    # Yadea
    "yadea": "Yadea",
    # Dibao
    "dibao": "Dibao",
    # Honda EV (substring dài trước ngắn)
    "honda icon e": "Honda",
    "icon e:": "Honda",  "icon e": "Honda",
    "cuv e:": "Honda",   "cuv e": "Honda",
    "honda uc3": "Honda", "uc3": "Honda",
    "honda": "Honda",
    # Generic
    "xe máy điện": "General E-Scooter",
    "scooter điện": "General E-Scooter",
    "xe điện": "General E-Scooter",
    "electric scooter": "General E-Scooter",
    "e-scooter": "General E-Scooter",
}


def detect_brand(text: str) -> str:
    """Return the primary brand detected in text, else 'Other'."""
    text_lower = text.lower()
    
    # Dynamically check current track keywords first
    try:
        dynamic_kws = get_track_keywords()
        for kw in dynamic_kws:
            if kw.lower() in text_lower:
                return kw # Return exact case of the keyword
    except Exception:
        pass

    # Fallback to hardcoded BRAND_MAP mapping
    for keyword, brand in BRAND_MAP.items():
        if keyword in text_lower:
            return brand
    return "Other"


def make_doc_id(unique_str: str) -> str:
    """Create a stable MD5-based deduplication ID from a URL or external ID."""
    return hashlib.md5(unique_str.encode("utf-8")).hexdigest()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ── MongoDB helpers ────────────────────────────────────────────────────────────
_client: MongoClient | None = None


def get_mongo_client() -> MongoClient:
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    return _client


def get_collection(name: str) -> Collection:
    client = get_mongo_client()
    db = client[MONGO_DB]
    col = db[name]
    # Ensure unique index on doc_id to prevent duplicate inserts
    if name != "tracking_configs":
        col.create_index([("doc_id", ASCENDING)], unique=True)
    return col


def get_track_keywords() -> list[str]:
    """Retrieve dynamic keywords from MongoDB, fallback to .env."""
    from src.config import TRACK_KEYWORDS
    try:
        col = get_collection("tracking_configs")
        keywords = list(col.find({}))
        if keywords:
            return [k.get("keyword") for k in keywords if k.get("keyword")]
    except Exception:
        pass
    
    return TRACK_KEYWORDS


def upsert_raw(collection_name: str, doc: dict) -> bool:
    """Insert doc into MongoDB. Returns True if new, False if duplicate."""
    col = get_collection(collection_name)
    try:
        col.insert_one(doc)
        return True
    except Exception:
        # Duplicate key → already exists
        return False
