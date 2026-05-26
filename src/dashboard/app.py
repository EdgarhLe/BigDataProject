"""Streamlit dashboard for Social Listening — Xe Máy Điện Race in Vietnam.

Pages:
  1. Overview    — Share of Voice pie chart, daily mentions trend
  2. Sentiment   — Sentiment breakdown per brand + time series
  3. Raw Feed    — Filterable table of recent posts
"""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text

from src.config import POSTGRES_URI

st.set_page_config(
    page_title="Xe Máy Điện Social Listening — Vietnam",
    page_icon="🛵",
    layout="wide",
)

# ── Brand colour palette ────────────────────────────────────────────────
BRAND_COLORS: dict[str, str] = {
    "VinFast":           "#002060",
    "Dat Bike":          "#E63946",
    "Selex Motors":      "#2DC653",
    "Yadea":             "#F77F00",
    "Dibao":             "#9B59B6",
    "Honda":             "#CC0000",
    "General E-Scooter": "#00A8CC",
}

# ── DB connection ──────────────────────────────────────────────────────────────

@st.cache_resource
def get_engine():
    return create_engine(POSTGRES_URI, pool_pre_ping=True)


@st.cache_data(ttl=300)
def load_posts(days: int = 30) -> pd.DataFrame:
    engine = get_engine()
    query = text("""
        SELECT source, brand, title, content, url, author,
               COALESCE(published_at, crawled_at) AS published_at,
               sentiment, sentiment_score, emotion,
               aspects
        FROM posts
        WHERE COALESCE(published_at, crawled_at) >= NOW() - INTERVAL ':days days'
          AND brand != 'Other'
        ORDER BY COALESCE(published_at, crawled_at) DESC
        LIMIT 5000
    """).bindparams(days=days)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, parse_dates=["published_at"])
    return df


@st.cache_data(ttl=300)
def load_daily_summary(days: int = 30) -> pd.DataFrame:
    engine = get_engine()
    query = text("""
        SELECT date, brand, source,
               total_mentions, positive_count, negative_count, neutral_count
        FROM daily_summary
        WHERE date >= CURRENT_DATE - :days
        ORDER BY date
    """).bindparams(days=days)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, parse_dates=["date"])
    return df


# ── Sidebar ────────────────────────────────────────────────────────────────────

st.sidebar.title("🛵 Xe Máy Điện Social Listening")
st.sidebar.markdown("**Vietnam E-Scooter Race**\nVinFast · Dat Bike · Selex · Yadea · Dibao · Honda")

days = st.sidebar.slider("Khoảng thời gian (ngày)", min_value=7, max_value=90, value=30, step=7)

# ── Load data ─────────────────────────────────────────────────────────────────

try:
    df_posts_raw = load_posts(days)
    df_summary_raw = load_daily_summary(days)
    data_ok = True
except Exception as e:
    st.error(f"Không kết nối được database: {e}")
    data_ok = False

if data_ok and len(df_posts_raw):
    # Global filters at the top of the page
    st.markdown("### 🎛️ Bộ lọc dữ liệu chung")
    f_col1, f_col2, f_col3 = st.columns(3)
    selected_brands = f_col1.multiselect(
        "Thương hiệu",
        options=df_posts_raw["brand"].unique(),
        default=list(df_posts_raw["brand"].unique())
    )
    selected_sents = f_col2.multiselect(
        "Cảm xúc",
        options=["positive", "neutral", "negative"],
        default=["positive", "neutral", "negative"]
    )
    selected_sources = f_col3.multiselect(
        "Nguồn dữ liệu",
        options=df_posts_raw["source"].unique(),
        default=list(df_posts_raw["source"].unique())
    )

    df_posts = df_posts_raw[
        df_posts_raw["brand"].isin(selected_brands) &
        df_posts_raw["sentiment"].isin(selected_sents) &
        df_posts_raw["source"].isin(selected_sources)
    ]
    df_summary = df_summary_raw[
        df_summary_raw["brand"].isin(selected_brands) &
        df_summary_raw["source"].isin(selected_sources)
    ]
else:
    df_posts = pd.DataFrame()
    df_summary = pd.DataFrame()

st.markdown("---")

# ── Page tabs ─────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4 = st.tabs(["📊 Tổng quan", "😊 Cảm xúc", "📰 Raw Feed", "⚙️ Cài đặt"])

# ────────────────────────────── Tab 1: Tổng quan ──────────────────────────────
with tab1:
    st.header("📊 Tổng quan — Share of Voice (Xe Máy Điện)")

    if not data_ok or df_posts.empty:
        st.info("Chưa có dữ liệu. Hãy chạy pipeline thu thập dữ liệu.")
    else:
        col1, col2, col3 = st.columns(3)
        col1.metric("Tổng bài viết", f"{len(df_posts):,}")
        col2.metric("Thương hiệu được nhắc", df_posts["brand"].nunique())
        col3.metric("Nguồn dữ liệu", df_posts["source"].nunique())

        st.markdown("---")

        # Share of Voice Pie
        sov = df_posts["brand"].value_counts().reset_index()
        sov.columns = ["brand", "count"]

        col_pie, col_bar = st.columns(2)
        with col_pie:
            st.subheader("Presence Score (Share of Voice)")
            fig_pie = px.pie(
                sov, values="count", names="brand",
                color_discrete_map=BRAND_COLORS,
                hole=0.35,
            )
            fig_pie.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(fig_pie, use_container_width=True)

        with col_bar:
            st.subheader("Lượng nhắc đến theo ngày")
            if not df_summary.empty:
                daily_brand = df_summary.groupby(["date", "brand"])["total_mentions"].sum().reset_index()
                fig_line = px.line(
                    daily_brand, x="date", y="total_mentions", color="brand",
                    color_discrete_map=BRAND_COLORS,
                    labels={"total_mentions": "Số lượt nhắc", "date": "Ngày"},
                )
                st.plotly_chart(fig_line, use_container_width=True)
            else:
                st.info("Chưa có dữ liệu tổng hợp theo ngày.")

        # Breakdown by source
        st.subheader("Phân bổ theo nguồn dữ liệu")
        src_brand = df_posts.groupby(["source", "brand"]).size().reset_index(name="count")
        fig_src = px.bar(
            src_brand, x="brand", y="count", color="source", barmode="group",
            labels={"count": "Số lượt nhắc", "brand": "Thương hiệu", "source": "Nguồn"},
        )
        st.plotly_chart(fig_src, use_container_width=True)


# ───────────────────────────── Tab 2: Sentiment ───────────────────────────────
with tab2:
    st.header("😊 Phân tích Cảm xúc (Sentiment Analysis)")

    if not data_ok or df_posts.empty:
        st.info("Chưa có dữ liệu.")
    else:
        sentiment_map = {"positive": "🟢 Tích cực", "neutral": "🟡 Trung lập", "negative": "🔴 Tiêu cực"}

        # KPI metrics
        total = len(df_posts)
        pos = (df_posts["sentiment"] == "positive").sum()
        neg = (df_posts["sentiment"] == "negative").sum()
        neu = total - pos - neg

        k1, k2, k3 = st.columns(3)
        k1.metric("🟢 Tích cực", f"{pos:,}", f"{pos/total*100:.1f}%")
        k2.metric("🟡 Trung lập", f"{neu:,}", f"{neu/total*100:.1f}%")
        k3.metric("🔴 Tiêu cực", f"{neg:,}", f"{neg/total*100:.1f}%")

        st.markdown("---")

        # Stacked bar per brand
        sent_brand = (
            df_posts.groupby(["brand", "sentiment"]).size()
            .reset_index(name="count")
        )
        fig_sent = px.bar(
            sent_brand, x="brand", y="count", color="sentiment",
            barmode="stack",
            color_discrete_map={"positive": "#27ae60", "neutral": "#f39c12", "negative": "#e74c3c"},
            labels={"count": "Số lượt", "brand": "Thương hiệu"},
            title="Cảm xúc theo thương hiệu",
        )
        st.plotly_chart(fig_sent, use_container_width=True)

        # Sentiment trend over time
        if not df_posts["published_at"].isna().all():
            df_trend = df_posts.copy()
            df_trend["day"] = df_trend["published_at"].dt.date
            trend = df_trend.groupby(["day", "sentiment"]).size().reset_index(name="count")
            fig_trend = px.line(
                trend, x="day", y="count", color="sentiment",
                color_discrete_map={"positive": "#27ae60", "neutral": "#f39c12", "negative": "#e74c3c"},
                labels={"count": "Số lượt", "day": "Ngày"},
                title="Xu hướng cảm xúc theo thời gian",
            )
            st.plotly_chart(fig_trend, use_container_width=True)

        st.markdown("---")
        st.subheader("Cảm xúc chi tiết (Emotion)")
        if "emotion" in df_posts.columns and not df_posts["emotion"].isna().all():
            emo_brand = df_posts[df_posts["emotion"] != "Bình thường"].groupby(["brand", "emotion"]).size().reset_index(name="count")
            if not emo_brand.empty:
                fig_emo = px.bar(
                    emo_brand, x="brand", y="count", color="emotion",
                    barmode="group",
                    title="Phân loại Cảm xúc theo Thương hiệu",
                )
                st.plotly_chart(fig_emo, use_container_width=True)
            else:
                st.info("Chưa có dữ liệu cảm xúc chi tiết.")
                
        st.subheader("Đánh giá theo khía cạnh (Aspects)")
        if "aspects" in df_posts.columns:
            # Flatten aspects json
            aspect_records = []
            for _, row in df_posts.dropna(subset=["aspects"]).iterrows():
                try:
                    import json
                    if isinstance(row["aspects"], str):
                        aspects_dict = json.loads(row["aspects"])
                    else:
                        aspects_dict = row["aspects"]
                    if isinstance(aspects_dict, dict):
                        for aspect_name, aspect_sent in aspects_dict.items():
                            aspect_records.append({
                                "brand": row["brand"],
                                "aspect": aspect_name.capitalize(),
                                "sentiment": aspect_sent
                            })
                except Exception:
                    pass
            
            if aspect_records:
                df_aspects = pd.DataFrame(aspect_records)
                aspect_counts = df_aspects.groupby(["brand", "aspect", "sentiment"]).size().reset_index(name="count")
                fig_aspect = px.bar(
                    aspect_counts, x="aspect", y="count", color="sentiment", facet_col="brand",
                    color_discrete_map={"positive": "#27ae60", "neutral": "#f39c12", "negative": "#e74c3c"},
                    title="Khía cạnh được nhắc đến nhiều nhất"
                )
                st.plotly_chart(fig_aspect, use_container_width=True)
            else:
                st.info("Chưa có dữ liệu khía cạnh.")


# ───────────────────────────── Tab 3: Raw Feed ────────────────────────────────
with tab3:
    st.header("📰 Raw Feed — Bài viết gần đây")

    if not data_ok or df_posts.empty:
        st.info("Chưa có dữ liệu phù hợp với bộ lọc hiện tại.")
    else:
        st.markdown(f"Hiển thị **{len(df_posts):,}** bài viết (đã lọc qua Sidebar)")

        display_cols = ["published_at", "source", "brand", "sentiment", "emotion", "sentiment_score", "title", "author", "url"]
        st.dataframe(
            df_posts[display_cols].head(500),
            use_container_width=True,
            column_config={
                "url": st.column_config.LinkColumn("Link"),
                "sentiment_score": st.column_config.NumberColumn("Score", format="%.2f"),
            },
        )

# ───────────────────────────── Tab 4: Cài đặt Từ khóa ─────────────────────────
with tab4:
    st.header("⚙️ Cài đặt Từ khóa (Dynamic Query)")
    st.markdown("Thêm hoặc xóa các từ khóa cần theo dõi. Pipeline cào dữ liệu sẽ tự động lấy từ danh sách này.")
    
    from src.utils import get_collection
    
    try:
        col = get_collection("tracking_configs")
        keywords_doc = list(col.find({}))
        
        st.subheader("Danh sách hiện tại")
        if keywords_doc:
            df_kw = pd.DataFrame(keywords_doc)
            kw_list = [doc.get("keyword") for doc in keywords_doc if doc.get("keyword")]
            
            st.markdown("Chọn từ khóa muốn **xóa** rồi nhấn nút bên dưới:")
            to_delete = []
            for kw in kw_list:
                if st.checkbox(kw, key=f"chk_{kw}"):
                    to_delete.append(kw)
            
            if to_delete:
                if st.button(f"🗑️ Xóa {len(to_delete)} từ khóa đã chọn", type="primary"):
                    for kw in to_delete:
                        col.delete_one({"keyword": kw})
                    st.success(f"Đã xóa: {', '.join(to_delete)}")
                    st.cache_data.clear()
                    import time; time.sleep(1)
                    st.rerun()
        else:
            st.info("Chưa có từ khóa nào được thiết lập. Sẽ dùng mặc định trong .env")
            
        with st.form("add_keyword_form"):
            new_kw = st.text_input("Từ khóa mới (VD: VinFast, Dat Bike)")
            submitted = st.form_submit_button("Thêm từ khóa và Tự động Cào dữ liệu")
            if submitted and new_kw:
                # Check if exists
                if not col.find_one({"keyword": new_kw.strip()}):
                    col.insert_one({"keyword": new_kw.strip()})
                    st.success(f"Đã thêm '{new_kw}'! Hệ thống đang tiến hành cào dữ liệu mới, vui lòng chờ vài phút...")
                    
                    # Tự động crawl
                    with st.spinner("Đang tự động thu thập từ YouTube, Báo chí và Phân tích Cảm xúc..."):
                        try:
                            from src.ingestion.youtube_api import run_youtube_ingestion
                            from src.ingestion.google_news import run_google_news_ingestion
                            from src.ingestion.vnexpress_scraper import run_vnexpress_ingestion
                            from src.ingestion.tuoitre_scraper import run_tuoitre_ingestion
                            from src.processing.etl import run_etl
                            
                            run_youtube_ingestion()
                            run_google_news_ingestion()
                            run_vnexpress_ingestion()
                            run_tuoitre_ingestion()
                            run_etl()
                            
                            # Xóa cache để dashboard lấy dữ liệu mới từ CSDL
                            st.cache_data.clear()
                            
                            st.success("Thu thập dữ liệu thành công! Đang tải lại trang...")
                        except Exception as ex:
                            st.error(f"Lỗi khi cào dữ liệu: {ex}")
                    import time
                    time.sleep(2)
                    st.rerun()
                else:
                    st.warning("Từ khóa đã tồn tại!")
                    
        with st.form("del_keyword_form"):
            del_kw = st.text_input("Hoặc nhập tên từ khóa để xóa nhanh")
            deleted = st.form_submit_button("Xóa nhanh")
            if deleted and del_kw:
                result = col.delete_one({"keyword": del_kw.strip()})
                if result.deleted_count > 0:
                    st.success(f"Đã xóa '{del_kw}'!")
                    st.rerun()
                else:
                    st.warning("Không tìm thấy từ khóa!")
                    
        st.markdown("---")
        st.subheader("🚀 Chạy thu thập dữ liệu thủ công")
        st.markdown("Hệ thống sẽ chạy một lượt thu thập toàn bộ dữ liệu mới nhất (Youtube, Báo chí, v.v...) và phân tích cảm xúc ngay lập tức.")
        if st.button("Thu thập dữ liệu ngay (Run Pipeline)"):
            with st.spinner("Đang chạy thu thập dữ liệu. Vui lòng chờ vài phút..."):
                try:
                    from src.ingestion.youtube_api import run_youtube_ingestion
                    from src.ingestion.google_news import run_google_news_ingestion
                    from src.ingestion.vnexpress_scraper import run_vnexpress_ingestion
                    from src.ingestion.tuoitre_scraper import run_tuoitre_ingestion
                    from src.processing.etl import run_etl
                    
                    st.toast("Đang thu thập YouTube...")
                    yt = run_youtube_ingestion()
                    st.toast("Đang thu thập Google News...")
                    gn = run_google_news_ingestion()
                    st.toast("Đang thu thập VnExpress...")
                    vne = run_vnexpress_ingestion()
                    st.toast("Đang thu thập Tuổi Trẻ...")
                    tt = run_tuoitre_ingestion()
                    
                    st.toast("Đang phân tích AI & chuyển dữ liệu (ETL)...")
                    etl = run_etl()
                    
                    # Xóa bộ nhớ đệm cache để Streamlit làm mới lại các tab
                    st.cache_data.clear()
                    
                    total_new = yt.get('new', 0) + gn.get('new', 0) + vne.get('new', 0) + tt.get('new', 0)
                    st.success(f"Hoàn thành! Thu thập được {total_new} bài viết mới. Xử lý & Phân tích thành công {etl.get('processed', 0)} bài.")
                    
                    import time
                    time.sleep(3)
                    st.rerun()
                except Exception as ex:
                    st.error(f"Có lỗi khi chạy Pipeline: {ex}")

    except Exception as e:
        st.error(f"Lỗi: {e}")
