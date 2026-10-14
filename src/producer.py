"""Payment event simulator -> Kafka.

Generates synthetic payment events (transaction_id, user_id, amount, merchant,
merchant category, location, timestamp) and publishes them as JSON to the
`payments` topic. A small fraction of events are injected with fraud-like
patterns (odd hours, high amounts, risky merchants) so the downstream model
has something realistic to catch.
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.config import SETTINGS

MERCHANTS = [
    ("grocery", 0.05), ("fuel", 0.08), ("restaurant", 0.12),
    ("electronics", 0.45), ("travel", 0.38), ("gaming", 0.55),
    ("crypto", 0.85), ("luxury", 0.70),
]

COUNTRIES = ["US", "US", "US", "GB", "DE", "FR", "AE", "NG", "BR", "IN"]


def make_event(fraud_ratio: float) -> dict:
    """Build one payment event; occasionally with fraud-like traits."""
    is_fraud_like = random.random() < fraud_ratio
    merchant, risk = random.choice(MERCHANTS)
    if is_fraud_like:
        # Fraud-like: pick a risky merchant and an unusual amount
        merchant, risk = random.choice([m for m in MERCHANTS if m[1] > 0.4])
        amount = round(random.uniform(800, 9500), 2)
        hour = random.choice([1, 2, 3, 4])
    else:
        amount = round(random.lognormvariate(3.2, 1.1), 2)
        hour = random.randint(6, 23)

    return {
        "transaction_id": str(uuid.uuid4()),
        "user_id": f"user_{random.randint(1, 5000):05d}",
        "amount": amount,
        "currency": "USD",
        "merchant": merchant,
        "merchant_risk": risk,
        "country": random.choice(COUNTRIES),
        "hour": hour,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "label": 1 if is_fraud_like else 0,  # only meaningful for training
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulate payment events into Kafka")
    parser.add_argument("--rate", type=float, default=SETTINGS.sim_rate_per_sec,
                        help="events per second")
    parser.add_argument("--duration", type=float, default=0,
                        help="seconds to run (0 = forever)")
    parser.add_argument("--topic", default=SETTINGS.payments_topic)
    parser.add_argument("--bootstrap", default=SETTINGS.bootstrap_servers)
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )
    print(f"Producing to {args.topic} @ {args.bootstrap} ({args.rate}/s)")
    interval = 1.0 / max(args.rate, 0.1)
    start = time.time()
    sent = 0
    try:
        while True:
            event = make_event(SETTINGS.sim_fraud_ratio)
            producer.send(args.topic, key=event["user_id"].encode(), value=event)
            sent += 1
            if sent % 100 == 0:
                print(f"sent {sent} events")
            if args.duration and time.time() - start > args.duration:
                break
            time.sleep(interval)
    except KeyboardInterrupt:
        print(f"\nstopped after {sent} events")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
