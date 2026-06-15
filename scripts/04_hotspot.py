# Find earthquake hotspots by dividing the map into a grid.
# Defaults to HDFS paths, with CLI overrides for local Spark verification.

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round, when


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_PATH = "/earthquake/input/earthquakes.csv"
DEFAULT_OUTPUT_BASE = "/earthquake/output/hotspots"


def is_uri(path_value: str) -> bool:
    return "://" in path_value


def normalize_spark_path(path_value: str) -> str:
    if is_uri(path_value) or path_value.startswith("/earthquake/"):
        return path_value

    path = Path(path_value).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return str(path.resolve())


def join_spark_path(base_path: str, *parts: str) -> str:
    return "/".join([base_path.rstrip("/"), *parts])


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Run Spark earthquake hotspot detection.")
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT_PATH,
        help="Input CSV path. Defaults to HDFS /earthquake/input/earthquakes.csv.",
    )
    parser.add_argument(
        "--output-base",
        default=DEFAULT_OUTPUT_BASE,
        help="Base output path. Defaults to HDFS /earthquake/output/hotspots.",
    )
    parser.add_argument(
        "--master",
        default=None,
        help="Optional Spark master, for example local[*] for local verification.",
    )
    args = parser.parse_args(argv)
    args.input = normalize_spark_path(args.input)
    args.output_base = normalize_spark_path(args.output_base)
    args.global_output_path = join_spark_path(args.output_base, "global")
    args.pakistan_output_path = join_spark_path(args.output_base, "pakistan")
    return args


def create_spark_session(master=None):
    builder = SparkSession.builder.appName("EarthquakeHotspotDetection")
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()


def add_risk_level(hotspots_df):
    # Classify risk based on earthquake frequency in each grid cell
    # CRITICAL (>50): Very high activity - dangerous zone
    # HIGH (>30): Significant activity - very prone to earthquakes
    # MEDIUM (>10): Moderate activity - could have earthquakes
    # LOW (<=10): Low activity - mostly safe
    return hotspots_df.withColumn(
        "risk_level",
        when(col("count") > 50, "CRITICAL")
        .when(col("count") > 30, "HIGH")
        .when(col("count") > 10, "MEDIUM")
        .otherwise("LOW"),
    )


def create_grid_and_count(df, region_filter=None):
    # Round coordinates to 1 decimal (roughly 10km precision)
    # This creates a grid where we can count earthquakes per cell
    grid_df = (
        df.withColumn("lat_grid", round(col("latitude"), 1))
        .withColumn("lon_grid", round(col("longitude"), 1))
    )
    
    if region_filter:
        grid_df = grid_df.filter(col("region") == region_filter)
    
    # Count how many earthquakes hit each grid cell
    hotspots = (
        grid_df.groupBy("lat_grid", "lon_grid")
        .agg(count("*").alias("count"))
        .orderBy(col("count").desc())
    )
    
    return add_risk_level(hotspots)


def main(argv=None) -> None:
    args = parse_args(argv)
    spark = create_spark_session(args.master)

    print("\n" + "=" * 50)
    print("  EARTHQUAKE HOTSPOT DETECTION")
    print("=" * 50 + "\n")

    # Read earthquake data
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(args.input)
    )

    # Find global hotspots
    print("Finding global hotspots (grid: 0.1° x 0.1°)...")
    global_hotspots = create_grid_and_count(df)
    
    print("\nTop 10 Global Hotspots:")
    global_hotspots.limit(10).show(truncate=False)

    # Find Pakistan hotspots
    print("\nFinding Pakistan hotspots...")
    pakistan_hotspots = create_grid_and_count(df, region_filter="Pakistan")
    
    print("\nTop 5 Pakistan Hotspots:")
    pakistan_hotspots.limit(5).show(truncate=False)

    # Save results
    print("\nSaving Spark results...")
    global_hotspots.write.mode("overwrite").option("header", True).csv(args.global_output_path)
    pakistan_hotspots.write.mode("overwrite").option("header", True).csv(args.pakistan_output_path)

    print(f"✓ Global hotspots  → {args.global_output_path}")
    print(f"✓ Pakistan hotspots → {args.pakistan_output_path}\n")

    spark.stop()


if __name__ == "__main__":
    main()
