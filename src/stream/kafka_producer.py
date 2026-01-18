"""
Kafka producer for e-commerce orders.
Sends events to Kafka topic instead of writing files.
"""

import time
import random
import json
import yaml
import pandas as pd
from pathlib import Path
from kafka import KafkaProducer
from kafka.errors import KafkaError

PROJECT_ROOT = Path(__file__).parent.parent.parent


def load_config():
    """Load configuration from YAML file."""
    config_path = PROJECT_ROOT / "config" / "config.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def create_producer(bootstrap_servers):
    """
    Create Kafka producer.

    Args:
        bootstrap_servers: Kafka broker address (e.g., 'localhost:9092')

    Returns:
        KafkaProducer instance
    """
    producer = KafkaProducer(
        bootstrap_servers=[bootstrap_servers],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
        max_in_flight_requests_per_connection=1,
    )

    print(f"✓ Connected to Kafka broker: {bootstrap_servers}")
    return producer


def send_to_kafka(
    source_file, topic_name, bootstrap_servers, batch_size, sleep_min, sleep_max
):
    """
    Read source file and send events to Kafka topic.

    Args:
        source_file: Path to source CSV file
        topic_name: Kafka topic name
        bootstrap_servers: Kafka broker address
        batch_size: Number of rows per micro-batch
        sleep_min: Minimum sleep between batches (seconds)
        sleep_max: Maximum sleep between batches (seconds)
    """

    producer = create_producer(bootstrap_servers)

    df = pd.read_csv(source_file)
    total_rows = len(df)

    print(f"\n{'=' * 70}")
    print(f"KAFKA PRODUCER STARTED")
    print(f"{'=' * 70}")
    print(f"Total rows: {total_rows}")
    print(f"Batch size: {batch_size}")
    print(f"Sleep interval: {sleep_min} - {sleep_max} seconds")
    print(f"Topic: {topic_name}")
    print(f"{'=' * 70}\n")

    batch_num = 1
    total_sent = 0
    total_failed = 0

    try:
        for start_idx in range(0, total_rows, batch_size):
            end_idx = min(start_idx + batch_size, total_rows)
            batch_df = df.iloc[start_idx:end_idx]

            batch_sent = 0
            batch_failed = 0

            for _, row in batch_df.iterrows():
                event = row.to_dict()

                if pd.notna(event.get("order_timestamp")):
                    event["order_timestamp"] = str(event["order_timestamp"])

                try:
                    future = producer.send(topic_name, value=event)

                    record_metadata = future.get(timeout=10)

                    batch_sent += 1
                    total_sent += 1

                except KafkaError as e:
                    print(f"  ✗ Failed to send event: {e}")
                    batch_failed += 1
                    total_failed += 1

            producer.flush()

            print(
                f"[Batch {batch_num:04d}] Sent: {batch_sent} | Failed: {batch_failed}"
            )

            batch_num += 1

            if end_idx < total_rows:
                sleep_time = random.uniform(sleep_min, sleep_max)
                print(f"  Sleeping for {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)

        print(f"\n{'=' * 70}")
        print(f"✓ KAFKA PRODUCER COMPLETED")
        print(f"{'=' * 70}")
        print(f"Total batches: {batch_num - 1}")
        print(f"Total sent: {total_sent}")
        print(f"Total failed: {total_failed}")
        print(f"{'=' * 70}")

    except KeyboardInterrupt:
        print("\n\n  Received interrupt signal. Stopping producer...")

    finally:
        producer.close()
        print("✓ Producer closed")


def main():
    """Main function for Kafka producer."""

    config = load_config()
    kafka_config = config.get("kafka", {})
    event_config = config.get("event_generator", {})

    source_path = PROJECT_ROOT / event_config["source_file"]

    if not source_path.exists():
        print(f"ERROR: Source file not found: {source_path}")
        print("Please generate data first")
        return

    send_to_kafka(
        source_file=str(source_path),
        topic_name=kafka_config["topic"],
        bootstrap_servers=kafka_config["bootstrap_servers"],
        batch_size=event_config["batch_size"],
        sleep_min=event_config["sleep_min"],
        sleep_max=event_config["sleep_max"],
    )


if __name__ == "__main__":
    main()
