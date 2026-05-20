"""Dagster pipeline: orchestrates ingestion + ETL + MLlib training on schedules.

Topology:
    [youtube_asset]   ──┐
    [news_asset]      ──┼──► [etl_asset] ──► [alert_asset]
    [vnexpress_asset] ──┤
    [tuoitre_asset]   ──┘

    [spark_training_asset] ──► [model_eval_asset]   (weekly, independent)

Schedules:
    hourly_social_listening  – 0 * * * *   (ingest → ETL → alert)
    weekly_model_retrain     – 0 2 * * 0   (Spark MLlib retrain, every Sunday 02:00)
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
    description="Run ETL: Spark Kafka batch (with Python fallback) → PostgreSQL",
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


# ── MLlib training asset (independent, weekly) ────────────────────────────────

@asset(
    retry_policy=RetryPolicy(max_retries=1, delay=60),
    group_name="ml",
    description="Retrain Spark MLlib sentiment pipeline on latest labeled PostgreSQL data",
)
def spark_training_asset(context: AssetExecutionContext) -> dict:
    try:
        from src.processing.spark_train import run_spark_train
        result = run_spark_train()
        context.log.info("Spark training result: %s", result)
        return result
    except Exception as exc:
        context.log.warning("Spark training failed: %s", exc)
        return {"error": str(exc)}


@asset(
    deps=[spark_training_asset],
    retry_policy=RetryPolicy(max_retries=1, delay=10),
    group_name="ml",
    description="Fetch and log the latest model evaluation metrics",
)
def model_eval_asset(context: AssetExecutionContext) -> dict:
    try:
        from sqlalchemy import create_engine, text
        from src.config import POSTGRES_URI

        engine = create_engine(POSTGRES_URI)
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT model_version, accuracy, f1_score, auc_roc, trained_at "
                    "FROM model_evaluations ORDER BY trained_at DESC LIMIT 1"
                )
            ).fetchone()
        if row:
            metrics = dict(row._mapping)
            context.log.info("Latest model metrics: %s", metrics)
            return metrics
        return {}
    except Exception as exc:
        context.log.warning("model_eval_asset failed: %s", exc)
        return {"error": str(exc)}


# ── Jobs ───────────────────────────────────────────────────────────────────────

social_listening_job = define_asset_job(
    name="social_listening_job",
    selection=[youtube_asset, news_asset, vnexpress_asset, tuoitre_asset, etl_asset, alert_asset],
)

model_retrain_job = define_asset_job(
    name="model_retrain_job",
    selection=[spark_training_asset, model_eval_asset],
)

# ── Schedules ──────────────────────────────────────────────────────────────────

hourly_schedule = ScheduleDefinition(
    job=social_listening_job,
    cron_schedule="0 * * * *",
    name="hourly_social_listening",
)

weekly_retrain_schedule = ScheduleDefinition(
    job=model_retrain_job,
    cron_schedule="0 2 * * 0",   # Every Sunday at 02:00 UTC
    name="weekly_model_retrain",
)

# ── Dagster Definitions (workspace entry-point) ────────────────────────────────

defs = Definitions(
    assets=[
        youtube_asset, news_asset, vnexpress_asset, tuoitre_asset,
        etl_asset, alert_asset,
        spark_training_asset, model_eval_asset,
    ],
    jobs=[social_listening_job, model_retrain_job],
    schedules=[hourly_schedule, weekly_retrain_schedule],
)

