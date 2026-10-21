"""Produce synthetic telecom network events to Kafka."""
from __future__ import annotations

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

REGIONS = ["north", "south", "east", "west"]
METRICS = {
    "rssi": (-125.0, -45.0),
    "latency_ms": (4.0, 450.0),
    "throughput_mbps": (5.0, 950.0),
}


def build_event(index: int, duplicate_of: str | None = None) -> dict:
    metric = random.choice(list(METRICS))
    low, high = METRICS[metric]
    # Deliberately repeat some IDs and vary event-time order to exercise the pipeline.
    event_id = duplicate_of or str(uuid.uuid4())
    event_time = datetime.now(timezone.utc) - timedelta(seconds=random.randint(0, 25))
    return {
        "event_id": event_id,
        "event_time": event_time.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "region": random.choice(REGIONS),
        "node_id": f"node-{random.randint(1, 24):03d}",
        "metric": metric,
        "value": round(random.uniform(low, high), 2),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="kafka:9092")
    parser.add_argument("--topic", default="network-events")
    parser.add_argument("--count", type=int, default=300)
    parser.add_argument("--interval", type=float, default=0.5, help="Seconds between events")
    args = parser.parse_args()

    producer = None
    for attempt in range(30):
        try:
            producer = KafkaProducer(
                bootstrap_servers=args.bootstrap_servers,
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                retries=5,
            )
            break
        except NoBrokersAvailable:
            if attempt == 29:
                raise
            time.sleep(2)
    assert producer is not None

    prior_ids: list[str] = []
    for index in range(args.count):
        repeated = prior_ids[-1] if index > 0 and index % 25 == 0 else None
        event = build_event(index, duplicate_of=repeated)
        producer.send(args.topic, key=event["event_id"].encode(), value=event)
        if repeated is None:
            prior_ids.append(event["event_id"])
        if (index + 1) % 50 == 0:
            producer.flush()
            print(f"Sent {index + 1}/{args.count} events")
        time.sleep(args.interval)
    producer.flush()
    producer.close()
    print(f"Finished sending {args.count} synthetic events to {args.topic}")


if __name__ == "__main__":
    main()
