"""Shared configuration loaded from environment variables."""
import os
from dotenv import load_dotenv

load_dotenv()

# ── API Keys ──────────────────────────────────────────────────
YOUTUBE_API_KEY     = os.getenv("YOUTUBE_API_KEY", "")
REDDIT_CLIENT_ID    = os.getenv("REDDIT_CLIENT_ID", "")
REDDIT_CLIENT_SECRET = os.getenv("REDDIT_CLIENT_SECRET", "")
REDDIT_USER_AGENT   = os.getenv("REDDIT_USER_AGENT", "social-listening-bot/1.0")
SERP_API_KEY        = os.getenv("SERP_API_KEY", "")
OPENAI_API_KEY      = os.getenv("OPENAI_API_KEY", "")
TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "")

# ── Database ──────────────────────────────────────────────────
MONGO_URI = (
    f"mongodb://{os.getenv('MONGO_USER','admin')}:{os.getenv('MONGO_PASSWORD','secret')}"
    f"@{os.getenv('MONGO_HOST','localhost')}:{os.getenv('MONGO_PORT','27017')}"
    f"/{os.getenv('MONGO_DB','social_listening')}?authSource=admin"
)
MONGO_DB  = os.getenv("MONGO_DB", "social_listening")

POSTGRES_URI = (
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER','admin')}:{os.getenv('POSTGRES_PASSWORD','secret')}"
    f"@{os.getenv('POSTGRES_HOST','localhost')}:{os.getenv('POSTGRES_PORT','5432')}"
    f"/{os.getenv('POSTGRES_DB','social_listening')}"
)

# ── Crawl Settings ────────────────────────────────────────────
RAW_KEYWORDS = os.getenv(
    "TRACK_KEYWORDS",
    "VinFast,VF3,VF5,VF6,VF7,VF8,VF9,BYD,Atto 3,Dolphin,Seal,Xiaomi Auto,SU7,xe điện,EV Việt Nam"
)
TRACK_KEYWORDS = [k.strip() for k in RAW_KEYWORDS.split(",") if k.strip()]

YOUTUBE_MAX_RESULTS     = int(os.getenv("YOUTUBE_MAX_RESULTS", 50))
REDDIT_MAX_POSTS        = int(os.getenv("REDDIT_MAX_POSTS", 100))
SERP_MAX_PAGES          = int(os.getenv("SERP_MAX_PAGES", 3))
ALERT_NEGATIVE_THRESHOLD = float(os.getenv("ALERT_NEGATIVE_THRESHOLD", 0.75))
