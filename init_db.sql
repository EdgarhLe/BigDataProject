-- Social Listening DB schema for PostgreSQL

CREATE TABLE IF NOT EXISTS posts (
    id          SERIAL PRIMARY KEY,
    doc_id      VARCHAR(64) UNIQUE NOT NULL,   -- MD5 hash of source_url or external_id
    source      VARCHAR(32) NOT NULL,           -- youtube | reddit | google_news
    brand       VARCHAR(64),                    -- VinFast | BYD | Xiaomi Auto | Other
    title       TEXT,
    content     TEXT,
    url         TEXT,
    author      VARCHAR(255),
    published_at TIMESTAMPTZ,
    crawled_at  TIMESTAMPTZ DEFAULT NOW(),
    sentiment   VARCHAR(16),                    -- positive | negative | neutral
    sentiment_score NUMERIC(5,4),
    language    VARCHAR(8) DEFAULT 'vi',
    raw_mongo_id VARCHAR(64),
    alerted_at  TIMESTAMPTZ                     -- NULL until Telegram alert sent
);

CREATE INDEX IF NOT EXISTS idx_posts_brand     ON posts(brand);
CREATE INDEX IF NOT EXISTS idx_posts_sentiment ON posts(sentiment);
CREATE INDEX IF NOT EXISTS idx_posts_source    ON posts(source);
CREATE INDEX IF NOT EXISTS idx_posts_published ON posts(published_at);
CREATE INDEX IF NOT EXISTS idx_posts_alerted   ON posts(alerted_at) WHERE alerted_at IS NULL;

CREATE TABLE IF NOT EXISTS model_evaluations (
    id              SERIAL PRIMARY KEY,
    trained_at      TIMESTAMPTZ DEFAULT NOW(),
    model_version   VARCHAR(64) NOT NULL,
    num_train       INTEGER,
    num_test        INTEGER,
    accuracy        NUMERIC(6,4),
    precision_score NUMERIC(6,4),
    recall_score    NUMERIC(6,4),
    f1_score        NUMERIC(6,4),
    auc_roc         NUMERIC(6,4)
);

CREATE TABLE IF NOT EXISTS daily_summary (
    id          SERIAL PRIMARY KEY,
    date        DATE NOT NULL,
    brand       VARCHAR(64) NOT NULL,
    source      VARCHAR(32) NOT NULL,
    total_mentions  INTEGER DEFAULT 0,
    positive_count  INTEGER DEFAULT 0,
    negative_count  INTEGER DEFAULT 0,
    neutral_count   INTEGER DEFAULT 0,
    UNIQUE(date, brand, source)
);
