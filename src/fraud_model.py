"""Fraud classifier: train offline with Spark MLlib, score in streaming.

Trains a LogisticRegression pipeline on synthetic labeled payments and saves
it to disk. The streaming job loads the saved model at startup and applies it
to every micro-batch, so training stays offline while scoring stays real-time.
"""

import argparse
import math
import random

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.config import SETTINGS

FEATURE_COLS = ["log_amount", "hour", "merchant_risk", "velocity_10m", "amount_deviation"]


def build_synthetic_training_data(spark: SparkSession, n: int = 20000):
    """Create labeled synthetic payments with fraud-like patterns."""
    rows = []
    for i in range(n):
        fraud = random.random() < 0.03
        amount = random.uniform(900, 9000) if fraud else math.exp(random.gauss(3.2, 1.1))
        rows.append((
            float(math.log1p(amount)),
            random.choice([1, 2, 3]) if fraud else random.randint(6, 23),
            random.uniform(0.4, 0.9) if fraud else random.uniform(0.0, 0.4),
            random.randint(3, 12) if fraud else random.randint(0, 3),
            random.uniform(1.5, 4.0) if fraud else random.uniform(0.0, 1.0),
            int(fraud),
        ))
    return spark.createDataFrame(rows, FEATURE_COLS + ["label"])


def train(spark: SparkSession, model_dir: str) -> dict:
    df = build_synthetic_training_data(spark)
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    assembler = VectorAssembler(inputCols=FEATURE_COLS, outputCol="features_raw")
    scaler = StandardScaler(inputCol="features_raw", outputCol="features")
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=50)

    pipeline = Pipeline(stages=[assembler, scaler, lr])
    model = pipeline.fit(train_df)
    model.write().overwrite().save(model_dir)

    preds = model.transform(test_df)
    auc = BinaryClassificationEvaluator(labelCol="label").evaluate(preds)
    print(f"saved model to {model_dir} | test AUC={auc:.3f}")
    return {"auc": auc, "model_dir": model_dir}


def main() -> None:
    parser = argparse.ArgumentParser(description="Train the fraud classifier")
    parser.add_argument("--train", action="store_true", help="train and save the model")
    parser.add_argument("--output", default=SETTINGS.model_dir)
    parser.add_argument("--rows", type=int, default=20000)
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName(f"{SETTINGS.app_name}-train")
        .master(SETTINGS.master).getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        if args.train:
            metrics = train(spark, args.output)
            print(metrics)
        else:
            # Quick sanity: load and show it exists
            PipelineModel.load(args.output)
            print(f"model OK at {args.output}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
