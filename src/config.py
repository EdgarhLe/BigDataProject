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

# ── Kafka ─────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_RAW         = os.getenv("KAFKA_TOPIC_RAW", "social_raw_posts")

# ── Spark / MLlib ─────────────────────────────────────────────
MODEL_PATH = os.getenv("MODEL_PATH", "./models/sentiment_pipeline")

# ── Crawl Settings ────────────────────────────────────────────
RAW_KEYWORDS = os.getenv(
    "TRACK_KEYWORDS",
    "VinFast,Evo200,Feliz S,Klara S,Vento S,Theon S,Rasad,Sadie,Saxil,"
    "Dat Bike,Weaver,Dat Bike Quantum,"
    "Selex,Selex Camel,"
    "Yadea,Dibao,"
    "Honda Icon e,CUV e,Honda UC3,"
    "xe máy điện,scooter điện,electric scooter"
)
TRACK_KEYWORDS = [k.strip() for k in RAW_KEYWORDS.split(",") if k.strip()]

YOUTUBE_MAX_RESULTS     = int(os.getenv("YOUTUBE_MAX_RESULTS", 50))
REDDIT_MAX_POSTS        = int(os.getenv("REDDIT_MAX_POSTS", 100))
SERP_MAX_PAGES          = int(os.getenv("SERP_MAX_PAGES", 3))
ALERT_NEGATIVE_THRESHOLD = float(os.getenv("ALERT_NEGATIVE_THRESHOLD", 0.75))
