"""
Event generator for streaming simulation.
Splits source CSV into micro-batches and writes them as separate files.
"""

import argparse
import time
import random
import yaml
import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent


def load_config():
    """Load configuration from YAML file."""
    config_path = PROJECT_ROOT / "config" / "config.yaml"
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def split_into_batches(source_file, output_dir, batch_size, sleep_min, sleep_max):
    """
    Split source file into micro-batches and write to output directory.

    Args:
        source_file: Path to source CSV file
        output_dir: Directory to write batch files
        batch_size: Number of rows per batch
        sleep_min: Minimum sleep between batches (seconds)
        sleep_max: Maximum sleep between batches (seconds)
    """

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    for f in output_path.glob("events_batch_*.csv"):
        f.unlink()

    df = pd.read_csv(source_file)
    total_rows = len(df)

    print(f"Total rows: {total_rows}")
    print(f"Batch size: {batch_size}")
    print(f"Sleep interval: {sleep_min} - {sleep_max} seconds")
    print(f"Output directory: {output_path}")

    batch_num = 1
    total_written = 0

    for start_idx in range(0, total_rows, batch_size):
        end_idx = min(start_idx + batch_size, total_rows)
        batch_df = df.iloc[start_idx:end_idx]

        batch_filename = f"events_batch_{batch_num:04d}.csv"
        batch_path = output_path / batch_filename

        batch_df.to_csv(batch_path, index=False)

        rows_in_batch = len(batch_df)
        total_written += rows_in_batch

        print(
            f"[Batch {batch_num:04d}] Written {rows_in_batch} rows → {batch_filename}"
        )

        batch_num += 1

        if end_idx < total_rows:
            sleep_time = random.uniform(sleep_min, sleep_max)
            print(f"Sleeping for {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)

    print(f"✓ Generation complete!")
    print(f"  Total batches: {batch_num - 1}")
    print(f"  Total rows written: {total_written}")


def main():
    """Main function for event generator."""
    parser = argparse.ArgumentParser(
        description="Generate micro-batch CSV files for streaming simulation"
    )

    # Load config for defaults
    config = load_config()
    event_config = config.get("event_generator", {})

    parser.add_argument(
        "--source-file",
        type=str,
        default=event_config.get(
            "source_file", config["event_generator"]["source_file"]
        ),
        help="Path to source CSV file",
    )

    parser.add_argument(
        "--output-dir",
        type=str,
        default=event_config.get("output_dir", config["event_generator"]["output_dir"]),
        help="Output directory for batch files",
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=event_config.get("batch_size", 100),
        help="Number of rows per micro-batch",
    )

    parser.add_argument(
        "--sleep-min",
        type=float,
        default=event_config.get("sleep_min", 1.0),
        help="Minimum sleep between batches (seconds)",
    )

    parser.add_argument(
        "--sleep-max",
        type=float,
        default=event_config.get("sleep_max", 3.0),
        help="Maximum sleep between batches (seconds)",
    )

    args = parser.parse_args()

    source_path = PROJECT_ROOT / args.source_file
    output_path = PROJECT_ROOT / args.output_dir

    if not source_path.exists():
        print(f"ERROR: Source file not found: {source_path}")
        print("Please generate data first using: python src/common/data_generator.py")
        return

    split_into_batches(
        source_file=str(source_path),
        output_dir=str(output_path),
        batch_size=args.batch_size,
        sleep_min=args.sleep_min,
        sleep_max=args.sleep_max,
    )


if __name__ == "__main__":
    main()
