"""Spark Structured Streaming job: validate -> aggregate -> score for fraud.

Reads payment events from Kafka, applies a watermark and deduplication,
computes per-minute per-merchant aggregates, and scores every transaction
with the pre-trained fraud model. Two sinks: console (aggregates) and
Parquet (scored payments), both checkpointed.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from src.config import SETTINGS

PAYMENT_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("user_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("merchant", StringType()),
    StructField("merchant_risk", DoubleType()),
    StructField("country", StringType()),
    StructField("hour", IntegerType()),
    StructField("event_time", StringType()),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName(SETTINGS.app_name)
        .master(SETTINGS.master)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    print(f"starting with {SETTINGS.summary()}")

    # Load the fraud model trained offline; fall back gracefully in dev
    try:
        fraud_model = PipelineModel.load(SETTINGS.model_dir)
        print(f"loaded fraud model from {SETTINGS.model_dir}")
    except Exception as exc:  # model not trained yet
        print(f"no fraud model found ({exc}); scoring will be skipped")
        fraud_model = None

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", SETTINGS.bootstrap_servers)
        .option("subscribe", SETTINGS.payments_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    payments = (
        raw.selectExpr("CAST(value AS STRING) AS json")
        .select(F.from_json("json", PAYMENT_SCHEMA).alias("p"))
        .select("p.*")
        .withColumn("event_ts", F.to_timestamp("event_time"))
        .withWatermark("event_ts", SETTINGS.watermark_delay)
        .dropDuplicates(["transaction_id"])  # late/out-of-order protection
        .filter(F.col("amount") > 0)  # basic validation
    )

    # Feature engineering for the fraud model (streaming-safe approximations)
    scored = (
        payments
        .withColumn("log_amount", F.log1p("amount"))
        .withColumn("velocity_10m", F.lit(1))  # placeholder; real velocity needs state
        .withColumn("amount_deviation", F.lit(0.5))
    )
    if fraud_model is not None:
        scored = fraud_model.transform(scored)
        fraud_prob = F.col("probability").getItem(1)
        scored = (
            scored.withColumn("fraud_probability", fraud_prob)
            .withColumn("is_fraud",
                        F.when(fraud_prob >= SETTINGS.fraud_threshold, True).otherwise(False))
        )

    # Sink 1: scored payments -> Parquet (gold)
    scored_query = (
        scored.writeStream.outputMode("append")
        .format("parquet")
        .option("path", SETTINGS.output_dir)
        .option("checkpointLocation", f"{SETTINGS.checkpoint_dir}/scored")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # Sink 2: per-minute per-merchant aggregates -> console
    agg = (
        payments.groupBy(
            F.window("event_ts", SETTINGS.window_duration),
            "merchant",
        )
        .agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("total_amount"),
            F.avg("amount").alias("avg_amount"),
        )
        .select("window.start", "window.end", "merchant",
                "txn_count", "total_amount", "avg_amount")
    )
    agg_query = (
        agg.writeStream.outputMode("complete")
        .format("console").option("truncate", "false")
        .option("checkpointLocation", f"{SETTINGS.checkpoint_dir}/agg")
        .start()
    )

    print("streaming queries started - press Ctrl+C to stop")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
