# 📡 Social Listening Dashboard

Hệ thống **Social Listening** theo dõi & phân tích cảm xúc thương hiệu theo thời gian thực từ nhiều nguồn dữ liệu (YouTube, Google News, VnExpress, Tuổi Trẻ). Kết quả được hiển thị trên dashboard tương tác với biểu đồ Share of Voice, Sentiment, Emotion và Aspects.

---

## 🏗️ Kiến trúc hệ thống

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│   YouTube API    Google News (SerpAPI)   VnExpress   Tuổi Trẻ  │
└──────────────────────────┬──────────────────────────────────────┘
                           │ Thu thập (Ingestion)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                  MONGODB (Data Lake - Raw)                      │
│   youtube_raw │ google_news_raw │ vnexpress_raw │ tuoitre_raw   │
│   tracking_configs (dynamic keywords)                           │
└──────────────────────────┬──────────────────────────────────────┘
                           │ Kafka Topic: social_raw_posts
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│               SPARK ETL  (fallback: Python ETL)                 │
│   Spam filter → Brand detection → Sentiment AI (OpenAI / NLP)  │
│   Emotion + Aspects extraction                                  │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│               POSTGRESQL (Data Warehouse)                       │
│   posts │ daily_summary │ model_evaluations                     │
└──────────┬───────────────────────────────────┬──────────────────┘
           │                                   │
           ▼                                   ▼
┌──────────────────────┐           ┌───────────────────────────┐
│  STREAMLIT DASHBOARD │           │   TELEGRAM ALERT BOT      │
│  localhost:8501      │           │  Cảnh báo sentiment tiêu  │
│  - Tổng quan         │           │  cực nghiêm trọng         │
│  - Cảm xúc           │           └───────────────────────────┘
│  - Raw Feed          │
│  - Cài đặt           │
└──────────────────────┘

         Toàn bộ được điều phối bởi DAGSTER (localhost:3000)
```

---

## 🛠️ Công nghệ sử dụng

| Layer | Công nghệ | Phiên bản | Vai trò |
|---|---|---|---|
| **Containerization** | Docker + Docker Compose | - | Đóng gói toàn bộ hệ thống |
| **Message Queue** | Apache Kafka | 3.7 | Buffer dữ liệu giữa Ingestion và ETL |
| **Data Lake** | MongoDB | 7.0 | Lưu raw data, cấu hình dynamic keyword |
| **Data Warehouse** | PostgreSQL | 16 | Lưu dữ liệu đã xử lý cho Dashboard |
| **Orchestration** | Dagster | 1.7 | Điều phối pipeline, lập lịch tự động |
| **Big Data** | Apache Spark (PySpark) | 3.5 | Xử lý batch song song (fallback: Python ETL) |
| **AI / NLP** | OpenAI gpt-4o-mini | - | Phân tích Sentiment + Emotion + Aspects |
| **NLP Offline** | underthesea | 6.8 | Fallback khi OpenAI hết quota |
| **Dashboard** | Streamlit | 1.35 | Web app tương tác |
| **Charts** | Plotly | 5.22 | Biểu đồ động |
| **Scraping** | BeautifulSoup4 + lxml | - | Cào báo VnExpress, Tuổi Trẻ |
| **YouTube** | google-api-python-client | 2.131 | YouTube Data API v3 |
| **Google News** | requests + SerpAPI | - | Tìm kiếm bài báo |
| **Alerts** | Telegram Bot API | - | Cảnh báo sentiment tiêu cực |
| **ORM** | SQLAlchemy | 2.0 | Kết nối Python ↔ PostgreSQL |
| **Retry** | tenacity | 8.3 | Tự động retry khi API lỗi |

---

## 📂 Cấu trúc thư mục

```
SocialLearning/
├── docker-compose.yml          # Định nghĩa 5 service: Kafka, MongoDB, PostgreSQL, Dagster, Streamlit
├── Dockerfile.dagster          # Image cho Dagster + PySpark (có Java 21)
├── Dockerfile.streamlit        # Image cho Streamlit Dashboard
├── init_db.sql                 # Schema PostgreSQL (posts, daily_summary, model_evaluations)
├── requirements.txt            # Python dependencies
├── workspace.yaml              # Dagster workspace config
├── dagster.yaml                # Dagster home config
├── start.ps1                   # Script khởi động nhanh (Windows PowerShell)
│
├── src/
│   ├── config.py               # Đọc biến môi trường (.env)
│   ├── utils.py                # MongoDB helpers, brand detection, deduplication
│   │
│   ├── ingestion/
│   │   ├── youtube_api.py      # Thu thập video + comment từ YouTube Data API
│   │   ├── google_news.py      # Thu thập bài báo từ Google News (SerpAPI)
│   │   ├── vnexpress_scraper.py# Cào bài viết từ VnExpress
│   │   ├── tuoitre_scraper.py  # Cào bài viết từ Tuổi Trẻ (RSS + search)
│   │   └── kafka_producer.py   # Đẩy raw doc vào Kafka topic
│   │
│   ├── processing/
│   │   ├── etl.py              # ETL chính: MongoDB → Sentiment → PostgreSQL
│   │   ├── sentiment.py        # Phân tích cảm xúc (OpenAI → underthesea → keyword)
│   │   ├── spark_etl.py        # Spark batch ETL (xử lý song song)
│   │   └── spark_train.py      # Huấn luyện MLlib model (chạy hàng tuần)
│   │
│   ├── orchestration/
│   │   └── dagster_pipeline.py # Định nghĩa Dagster Assets + Schedules
│   │
│   ├── dashboard/
│   │   └── app.py              # Streamlit UI (4 tab: Tổng quan, Cảm xúc, Raw Feed, Cài đặt)
│   │
│   ├── alerts/
│   │   └── telegram_bot.py     # Gửi cảnh báo Telegram khi có sentiment tiêu cực
│   │
│   └── monitoring/
│       ├── healthcheck.py      # Kiểm tra trạng thái các service
│       └── volume_stats.py     # Thống kê khối lượng dữ liệu
│
└── scripts/
    ├── run_pipeline.py         # Chạy pipeline thủ công (--step ingest|etl|alert|all)
    └── reset_mongo.js          # Script reset MongoDB
```

---

## 🔄 Luồng dữ liệu chi tiết

### 1. Thu thập dữ liệu (Ingestion)
- Đọc **dynamic keywords** từ MongoDB `tracking_configs` (quản lý qua Dashboard tab Cài đặt)
- Gọi API / cào web tương ứng → tạo `doc_id` (MD5 hash URL) để **deduplicate**
- Lưu raw document vào MongoDB + đẩy vào **Kafka topic** `social_raw_posts`

### 2. ETL & AI Analysis
- **Spark ETL** (khi Java khả dụng): đọc Kafka batch → xử lý song song → bulk write PostgreSQL
- **Python ETL** (fallback): xử lý từng bài, ưu tiên khi Spark không chạy được
- **Spam filter heuristic**: lọc bài có > 5 hashtag, link rút gọn, từ khóa seeding
- **Brand detection**: so khớp keyword động (MongoDB) trước, fallback sang `BRAND_MAP` cố định
- **Sentiment 3 tầng**:
  1. OpenAI `gpt-4o-mini` với JSON mode → `label` + `score` + `emotion` + `aspects`
  2. `underthesea` (offline NLP tiếng Việt)
  3. Keyword-based fallback (luôn hoạt động)

### 3. Dashboard (Streamlit)
- **Tab Tổng quan**: Share of Voice pie chart, lượng nhắc đến theo ngày, phân bổ theo nguồn
- **Tab Cảm xúc**: KPI metrics, stacked bar theo brand, trend line, emotion chart, aspect chart
- **Tab Raw Feed**: Bảng bài viết có thể lọc
- **Tab Cài đặt**: Thêm/xóa keyword (checkbox), nút "Thu thập dữ liệu ngay" tự động crawl & reload

### 4. Cảnh báo Telegram
- Sau mỗi lần ETL, tìm bài `sentiment = negative` và `sentiment_score > threshold`
- Gửi tin nhắn HTML qua **Telegram Bot API**

---

## ⚙️ Cài đặt & Chạy

### Yêu cầu
- Docker Desktop (Windows/Mac/Linux)
- File `.env` với các API keys

### File `.env` mẫu
```env
# YouTube Data API v3
YOUTUBE_API_KEY=your_youtube_api_key

# SerpAPI (Google News)
SERP_API_KEY=your_serp_api_key

# MongoDB
MONGO_USER=admin
MONGO_PASSWORD=secret
MONGO_URI=mongodb://admin:secret@mongodb:27017/social_listening?authSource=admin
MONGO_DB=social_listening

# PostgreSQL
POSTGRES_USER=admin
POSTGRES_PASSWORD=secret
POSTGRES_URI=postgresql://admin:secret@postgres:5432/social_listening

# OpenAI
OPENAI_API_KEY=your_openai_api_key

# Telegram Bot
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
ALERT_NEGATIVE_THRESHOLD=0.8

# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
```

### Khởi động
```powershell
# Khởi động toàn bộ hệ thống
docker compose up -d

# Chạy pipeline thủ công
docker exec social_dagster python scripts/run_pipeline.py --step all

# Chỉ chạy thu thập dữ liệu
docker exec social_dagster python scripts/run_pipeline.py --step ingest

# Chỉ chạy ETL
docker exec social_dagster python scripts/run_pipeline.py --step etl
```

### Truy cập
| Service | URL |
|---|---|
| **Dashboard (Streamlit)** | http://localhost:8501 |
| **Dagster UI** | http://localhost:3000 |
| **MongoDB** | localhost:27017 |
| **PostgreSQL** | localhost:5432 |
| **Kafka** | localhost:9092 |

---

## 📊 Lịch chạy tự động (Dagster Schedules)

| Schedule | Cron | Công việc |
|---|---|---|
| `hourly_social_listening` | `0 * * * *` | Ingest → ETL → Alert (mỗi giờ) |
| `weekly_model_retrain` | `0 2 * * 0` | Huấn luyện lại MLlib model (Chủ nhật 2:00) |

---

## 🗄️ Schema PostgreSQL

### Bảng `posts` (dữ liệu chính)
| Cột | Kiểu | Mô tả |
|---|---|---|
| `doc_id` | VARCHAR(64) | MD5 hash URL để deduplicate |
| `source` | VARCHAR(32) | youtube \| google_news \| vnexpress \| tuoitre |
| `brand` | VARCHAR(64) | Thương hiệu được nhắc đến |
| `title` | TEXT | Tiêu đề bài viết / video |
| `sentiment` | VARCHAR(16) | positive \| negative \| neutral |
| `sentiment_score` | NUMERIC | Độ tin cậy 0.0 – 1.0 |
| `emotion` | VARCHAR(64) | Vui vẻ \| Phẫn nộ \| Buồn bã \| Ngạc nhiên \| Bình thường |
| `aspects` | JSONB | `{"Pin": "negative", "Giá": "positive", ...}` |
| `alerted_at` | TIMESTAMPTZ | Thời điểm gửi cảnh báo Telegram |

### Bảng `daily_summary` (tổng hợp theo ngày)
Dùng để vẽ biểu đồ trend nhanh, tránh query toàn bộ `posts`.

### Bảng `model_evaluations` (đánh giá ML)
Lưu kết quả accuracy, F1, AUC-ROC sau mỗi lần retrain MLlib.

---

## 🔑 Tính năng nổi bật

- ✅ **Dynamic Keywords**: Thêm/xóa từ khóa theo dõi trực tiếp trên Dashboard, pipeline tự động dùng danh sách mới
- ✅ **Auto-crawl**: Nhấn một nút trên Dashboard là tự động cào + phân tích + reload biểu đồ
- ✅ **Spam Filter**: Lọc heuristic trước khi gọi AI để tiết kiệm token
- ✅ **3-tier Sentiment**: OpenAI → underthesea → keyword fallback, không bao giờ bị lỗi
- ✅ **Emotion & Aspects**: Phân tích sâu hơn sentiment đơn giản
- ✅ **Deduplication**: MD5 hash URL ngăn lưu trùng dữ liệu
- ✅ **Global Filter**: Bộ lọc Brand/Sentiment/Nguồn áp dụng đồng bộ trên tất cả Tab
- ✅ **Telegram Alerts**: Cảnh báo tức thì khi xuất hiện nội dung tiêu cực nghiêm trọng
- ✅ **Dagster Retry**: Tự động thử lại 2 lần khi API bị lỗi tạm thời
