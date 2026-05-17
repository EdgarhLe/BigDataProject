"""Manual pipeline runner — for running/testing outside Docker.

Usage:
  python scripts/run_pipeline.py --step all
  python scripts/run_pipeline.py --step ingest
  python scripts/run_pipeline.py --step etl
  python scripts/run_pipeline.py --step alert
  python scripts/run_pipeline.py --step stats
  python scripts/run_pipeline.py --step health
"""
import argparse
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def step_ingest():
    from src.ingestion.youtube_api import run_youtube_ingestion
    from src.ingestion.reddit_api import run_reddit_ingestion
    from src.ingestion.google_news import run_google_news_ingestion

    print("\n>>> Running ingestion...")
    yt   = run_youtube_ingestion()
    rd   = run_reddit_ingestion()
    gn   = run_google_news_ingestion()
    total_new = yt.get("new", 0) + rd.get("new", 0) + gn.get("new", 0)
    total_dup = yt.get("duplicate", 0) + rd.get("duplicate", 0) + gn.get("duplicate", 0)
    print(f"\n    YouTube     : {yt}")
    print(f"    Reddit      : {rd}")
    print(f"    Google News : {gn}")
    print(f"\n    Total new   : {total_new:,}   duplicates blocked: {total_dup:,}")


def step_etl():
    from src.processing.etl import run_etl
    print("\n>>> Running ETL (MongoDB → PostgreSQL) ...")
    stats = run_etl()
    print(f"\n    {stats}")


def step_alert():
    from src.alerts.telegram_bot import send_negative_alerts
    print("\n>>> Sending Telegram alerts ...")
    stats = send_negative_alerts()
    print(f"\n    {stats}")


def step_stats():
    from src.monitoring.volume_stats import print_volume_report
    print_volume_report()


def step_health():
    from src.monitoring.healthcheck import run_healthcheck
    ok = run_healthcheck()
    sys.exit(0 if ok else 1)


STEPS = {
    "ingest": step_ingest,
    "etl":    step_etl,
    "alert":  step_alert,
    "stats":  step_stats,
    "health": step_health,
}


def main():
    parser = argparse.ArgumentParser(description="Social Listening pipeline runner")
    parser.add_argument(
        "--step",
        choices=list(STEPS.keys()) + ["all"],
        default="all",
        help="Which pipeline step to run (default: all)",
    )
    args = parser.parse_args()

    if args.step == "all":
        for name, fn in STEPS.items():
            if name in ("stats", "health"):
                continue  # run stats/health separately
            print(f"\n{'='*50}\nSTEP: {name.upper()}\n{'='*50}")
            fn()
        step_stats()
    else:
        STEPS[args.step]()


if __name__ == "__main__":
    main()
