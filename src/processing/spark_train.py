"""Spark MLlib training script for Vietnamese social media sentiment.

Pipeline: RegexTokenizer → StopWordsRemover → HashingTF → IDF → LogisticRegression

Usage:
    python -m src.processing.spark_train          # train + save + log metrics
    python -m src.processing.spark_train --eval   # evaluate existing model only
"""
import logging
import os
import sys
import time
from datetime import datetime, timezone

# ── Must set BEFORE importing PySpark ──────────────────────────────────────────
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

import psycopg2
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.ml.feature import HashingTF, IDF, RegexTokenizer, StopWordsRemover
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

from src.config import MODEL_PATH, POSTGRES_URI

logger = logging.getLogger(__name__)

# ── Vietnamese stop-words (minimal, extensible) ─────────────────────────────
_VIETNAMESE_STOPWORDS = [
    "và", "của", "là", "có", "được", "không", "cho", "với", "trong", "đã",
    "này", "một", "những", "các", "để", "tôi", "bạn", "anh", "chị", "ông",
    "bà", "nhưng", "hay", "cũng", "rất", "thì", "mà", "nên", "nếu", "vì",
    "khi", "từ", "đến", "về", "theo", "theo", "như", "vậy", "thế", "cả",
    "chỉ", "đây", "đó", "vẫn", "đang", "sẽ", "đều", "lại", "hơn", "sau",
    "trước", "giờ", "ngày", "năm", "tháng", "tuần", "lần", "nhiều", "ít",
]


def _build_spark() -> SparkSession:
    """Create a SparkSession with safe settings for local execution."""
    return (
        SparkSession.builder.master("local[*]")
        .appName("SocialLearning-SentimentTrain")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        # Security fixes (required on Java 17+)
        .config(
            "spark.driver.extraJavaOptions",
            "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
            "-Djava.security.manager=allow",
        )
        .config(
            "spark.executor.extraJavaOptions",
            "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
            "-Djava.security.manager=allow",
        )
        .getOrCreate()
    )


def _load_labeled_data(spark: SparkSession):
    """Read labeled posts from PostgreSQL.

    Rows with sentiment='positive' → label=1, 'negative' → label=0.
    Neutral and NULL are excluded (not useful for binary classifier).
    """
    jdbc_url = POSTGRES_URI.replace("postgresql+psycopg2", "jdbc:postgresql")
    # Strip SQLAlchemy driver prefix for JDBC
    if jdbc_url.startswith("jdbc:postgresql+psycopg2"):
        jdbc_url = jdbc_url.replace("jdbc:postgresql+psycopg2", "jdbc:postgresql")

    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "posts")
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    # Keep only binary-labeled rows with non-empty text
    df = df.filter(col("sentiment").isin("positive", "negative"))
    df = df.filter(col("content").isNotNull() & (col("content") != ""))

    # Combine title + content for richer features
    from pyspark.sql.functions import concat_ws
    df = df.withColumn("text", concat_ws(" ", col("title"), col("content")))
    df = df.withColumn(
        "label",
        when(col("sentiment") == "positive", 1.0).otherwise(0.0),
    )
    return df.select("text", "label")


def _build_pipeline() -> Pipeline:
    tokenizer = RegexTokenizer(
        inputCol="text",
        outputCol="words",
        pattern=r"[^\w\u00C0-\u024F\u1E00-\u1EFF]+",  # keep Latin + Vietnamese chars
        toLowercase=True,
    )
    remover = StopWordsRemover(
        inputCol="words",
        outputCol="filtered",
        stopWords=StopWordsRemover.loadDefaultStopWords("english") + _VIETNAMESE_STOPWORDS,
    )
    hashing_tf = HashingTF(inputCol="filtered", outputCol="raw_features", numFeatures=20_000)
    idf = IDF(inputCol="raw_features", outputCol="features", minDocFreq=5)
    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label",
        maxIter=20,
        regParam=0.01,
        elasticNetParam=0.0,
    )
    return Pipeline(stages=[tokenizer, remover, hashing_tf, idf, lr])


def _evaluate(model, test_df):
    predictions = model.transform(test_df)
    binary_eval = BinaryClassificationEvaluator(
        labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    )
    auc_roc = binary_eval.evaluate(predictions)

    multi_eval = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
    accuracy   = multi_eval.setMetricName("accuracy").evaluate(predictions)
    precision  = multi_eval.setMetricName("weightedPrecision").evaluate(predictions)
    recall     = multi_eval.setMetricName("weightedRecall").evaluate(predictions)
    f1         = multi_eval.setMetricName("f1").evaluate(predictions)

    return {
        "accuracy":        round(accuracy, 4),
        "precision_score": round(precision, 4),
        "recall_score":    round(recall, 4),
        "f1_score":        round(f1, 4),
        "auc_roc":         round(auc_roc, 4),
    }


def _save_metrics(metrics: dict, num_train: int, num_test: int, model_version: str) -> None:
    """Write evaluation row to PostgreSQL model_evaluations table."""
    from sqlalchemy import create_engine, text

    engine = create_engine(POSTGRES_URI)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO model_evaluations
                    (model_version, num_train, num_test,
                     accuracy, precision_score, recall_score, f1_score, auc_roc)
                VALUES
                    (:version, :num_train, :num_test,
                     :accuracy, :precision_score, :recall_score, :f1_score, :auc_roc)
                """
            ),
            {
                "version":        model_version,
                "num_train":      num_train,
                "num_test":       num_test,
                **metrics,
            },
        )
    logger.info("Metrics saved to model_evaluations: %s", metrics)


def run_spark_train() -> dict:
    """Full train → evaluate → save pipeline.  Returns metric dict."""
    spark = _build_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        logger.info("Loading labeled data from PostgreSQL …")
        df = _load_labeled_data(spark)
        total = df.count()
        if total < 50:
            logger.warning("Only %d labeled rows — skipping training.", total)
            return {}

        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        num_train = train_df.count()
        num_test  = test_df.count()
        logger.info("Train=%d  Test=%d", num_train, num_test)

        pipeline = _build_pipeline()
        logger.info("Training …")
        model = pipeline.fit(train_df)

        metrics = _evaluate(model, test_df)
        logger.info(
            "Accuracy=%.4f  Precision=%.4f  Recall=%.4f  F1=%.4f  AUC-ROC=%.4f",
            metrics["accuracy"], metrics["precision_score"],
            metrics["recall_score"], metrics["f1_score"], metrics["auc_roc"],
        )

        # Save model
        model_version = datetime.now(timezone.utc).strftime("v%Y%m%d_%H%M%S")
        model_path    = os.path.join(MODEL_PATH, model_version)
        os.makedirs(MODEL_PATH, exist_ok=True)
        model.write().overwrite().save(model_path)
        logger.info("Model saved to %s", model_path)

        # Write a 'latest' symlink-style marker
        latest_marker = os.path.join(MODEL_PATH, "latest.txt")
        with open(latest_marker, "w") as f:
            f.write(model_path)

        # Persist metrics
        _save_metrics(metrics, num_train, num_test, model_version)

        return {"version": model_version, "path": model_path, **metrics}

    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--eval", action="store_true", help="evaluate existing model only")
    args = parser.parse_args()

    result = run_spark_train()
    print(result)
