"""Shared configuration for the payment streaming pipeline.

Every setting can be overridden with an environment variable, which keeps
secrets and infra details out of the code. Defaults target the local
docker-compose setup.
"""

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass
class Settings:
    # Kafka
    bootstrap_servers: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP", "localhost:9092"))
    payments_topic: str = field(default_factory=lambda: _env("PAYMENTS_TOPIC", "payments"))
    consumer_group: str = field(default_factory=lambda: _env("KAFKA_GROUP", "payments-streaming"))

    # Streaming windows
    window_duration: str = field(default_factory=lambda: _env("WINDOW_DURATION", "1 minute"))
    watermark_delay: str = field(default_factory=lambda: _env("WATERMARK_DELAY", "2 minutes"))

    # Sinks
    output_dir: str = field(default_factory=lambda: _env("OUTPUT_DIR", "data/output/scored_payments"))
    agg_output_dir: str = field(default_factory=lambda: _env("AGG_OUTPUT_DIR", "data/output/payment_aggregates"))
    checkpoint_dir: str = field(default_factory=lambda: _env("CHECKPOINT_DIR", "data/checkpoints"))

    # Fraud model
    model_dir: str = field(default_factory=lambda: _env("FRAUD_MODEL_DIR", "models/fraud_model"))
    fraud_threshold: float = float(_env("FRAUD_THRESHOLD", "0.8"))

    # Simulator defaults
    sim_rate_per_sec: float = float(_env("SIM_RATE", "20"))
    sim_fraud_ratio: float = float(_env("SIM_FRAUD_RATIO", "0.02"))

    # Spark
    app_name: str = field(default_factory=lambda: _env("SPARK_APP", "payment-streaming"))
    master: str = field(default_factory=lambda: _env("SPARK_MASTER", "local[*]"))


SETTINGS = Settings()


def summary() -> str:
    s = SETTINGS
    return (
        f"topic={s.payments_topic} bootstrap={s.bootstrap_servers} "
        f"window={s.window_duration} watermark={s.watermark_delay}"
    )
