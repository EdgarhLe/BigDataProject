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
               sentiment, sentiment_score
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
selected_sources = st.sidebar.multiselect(
    "Nguồn dữ liệu",
    options=["youtube", "google_news", "vnexpress", "tuoitre"],
    default=["youtube", "google_news", "vnexpress", "tuoitre"],
)

# ── Load data ─────────────────────────────────────────────────────────────────

try:
    df_posts = load_posts(days)
    df_summary = load_daily_summary(days)
    data_ok = True
except Exception as e:
    st.error(f"Không kết nối được database: {e}")
    data_ok = False

if data_ok and len(df_posts):
    if selected_sources:
        df_posts = df_posts[df_posts["source"].isin(selected_sources)]
        df_summary = df_summary[df_summary["source"].isin(selected_sources)]

# ── Page tabs ─────────────────────────────────────────────────────────────────

tab1, tab2, tab3 = st.tabs(["📊 Tổng quan", "😊 Cảm xúc", "📰 Raw Feed"])

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


# ───────────────────────────── Tab 3: Raw Feed ────────────────────────────────
with tab3:
    st.header("📰 Raw Feed — Bài viết gần đây")

    if not data_ok or df_posts.empty:
        st.info("Chưa có dữ liệu.")
    else:
        col_f1, col_f2, col_f3 = st.columns(3)
        brand_filter = col_f1.multiselect("Thương hiệu", df_posts["brand"].unique(),
                                          default=list(df_posts["brand"].unique()))
        sent_filter  = col_f2.multiselect("Cảm xúc", ["positive", "neutral", "negative"],
                                          default=["positive", "neutral", "negative"])
        src_filter   = col_f3.multiselect("Nguồn", df_posts["source"].unique(),
                                          default=list(df_posts["source"].unique()))

        filtered = df_posts[
            df_posts["brand"].isin(brand_filter) &
            df_posts["sentiment"].isin(sent_filter) &
            df_posts["source"].isin(src_filter)
        ]

        st.markdown(f"Hiển thị **{len(filtered):,}** bài viết")

        display_cols = ["published_at", "source", "brand", "sentiment", "sentiment_score", "title", "author", "url"]
        st.dataframe(
            filtered[display_cols].head(500),
            use_container_width=True,
            column_config={
                "url": st.column_config.LinkColumn("Link"),
                "sentiment_score": st.column_config.NumberColumn("Score", format="%.2f"),
            },
        )
