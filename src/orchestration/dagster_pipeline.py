"""Dagster pipeline: orchestrates ingestion + ETL on a schedule.

Topology:
    [youtube_asset]   ──┐
    [news_asset]      ──┼──► [etl_asset] ──► [alert_asset]
    [vnexpress_asset] ──┤
    [tuoitre_asset]   ──┘

Schedule: runs every hour.
"""
import logging

from dagster import (
    AssetExecutionContext,
    Definitions,
    ScheduleDefinition,
    asset,
    define_asset_job,
    RetryPolicy,
)

from src.ingestion.youtube_api import run_youtube_ingestion
from src.ingestion.google_news import run_google_news_ingestion
from src.ingestion.vnexpress_scraper import run_vnexpress_ingestion
from src.ingestion.tuoitre_scraper import run_tuoitre_ingestion
from src.processing.etl import run_etl
from src.alerts.telegram_bot import send_negative_alerts

logger = logging.getLogger(__name__)

_retry = RetryPolicy(max_retries=2, delay=30)


# ── Ingestion assets ───────────────────────────────────────────────────────────

@asset(retry_policy=_retry, group_name="ingestion", description="Fetch YouTube videos & comments")
def youtube_asset(context: AssetExecutionContext) -> dict:
    stats = run_youtube_ingestion()
    context.log.info("YouTube stats: %s", stats)
    return stats


@asset(retry_policy=_retry, group_name="ingestion", description="Scrape VnExpress articles & comments")
def vnexpress_asset(context: AssetExecutionContext) -> dict:
    stats = run_vnexpress_ingestion()
    context.log.info("VnExpress stats: %s", stats)
    return stats


@asset(retry_policy=_retry, group_name="ingestion", description="Scrape Tuổi Trẻ articles via RSS + search")
def tuoitre_asset(context: AssetExecutionContext) -> dict:
    stats = run_tuoitre_ingestion()
    context.log.info("Tuổi Trẻ stats: %s", stats)
    return stats


@asset(retry_policy=_retry, group_name="ingestion", description="Fetch Google News articles via SerpAPI")
def news_asset(context: AssetExecutionContext) -> dict:
    stats = run_google_news_ingestion()
    context.log.info("Google News stats: %s", stats)
    return stats


# ── Processing asset (depends on all ingestion assets) ────────────────────────

@asset(
    deps=[youtube_asset, news_asset, vnexpress_asset, tuoitre_asset],
    retry_policy=_retry,
    group_name="processing",
    description="Run ETL: sentiment analysis + load into PostgreSQL",
)
def etl_asset(context: AssetExecutionContext) -> dict:
    stats = run_etl()
    context.log.info("ETL stats: %s", stats)
    return stats


# ── Alert asset (depends on ETL being complete) ───────────────────────────────

@asset(
    deps=[etl_asset],
    retry_policy=RetryPolicy(max_retries=1, delay=10),
    group_name="alerts",
    description="Send Telegram alerts for highly negative posts",
)
def alert_asset(context: AssetExecutionContext) -> dict:
    stats = send_negative_alerts()
    context.log.info("Alert stats: %s", stats)
    return stats


# ── Job + Schedule ─────────────────────────────────────────────────────────────

social_listening_job = define_asset_job(
    name="social_listening_job",
    selection=[youtube_asset, news_asset, vnexpress_asset, tuoitre_asset, etl_asset, alert_asset],
)

hourly_schedule = ScheduleDefinition(
    job=social_listening_job,
    cron_schedule="0 * * * *",   # every hour
    name="hourly_social_listening",
)

# ── Dagster Definitions (workspace entry-point) ────────────────────────────────

defs = Definitions(
    assets=[youtube_asset, news_asset, vnexpress_asset, tuoitre_asset, etl_asset, alert_asset],
    jobs=[social_listening_job],
    schedules=[hourly_schedule],
)
