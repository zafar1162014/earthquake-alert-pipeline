# Analyze earthquake data using Spark batch processing
# Defaults to HDFS paths, with CLI overrides for local Spark verification.

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, lit, max as spark_max, min as spark_min, when

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT_PATH = "/earthquake/input/earthquakes.csv"
DEFAULT_OUTPUT_BASE = "/earthquake/output/batch"


def is_uri(path_value: str) -> bool:
    return "://" in path_value


def normalize_spark_path(path_value: str) -> str:
    if is_uri(path_value) or path_value.startswith("/earthquake/"):
        return path_value

    path = Path(path_value).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return str(path.resolve())


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Run Spark batch analysis for earthquake data.")
    parser.add_argument(
        "--input",
        default=DEFAULT_INPUT_PATH,
        help="Input CSV path. Defaults to HDFS /earthquake/input/earthquakes.csv.",
    )
    parser.add_argument(
        "--output-base",
        default=DEFAULT_OUTPUT_BASE,
        help="Base output path. Defaults to HDFS /earthquake/output/batch.",
    )
    parser.add_argument(
        "--master",
        default=None,
        help="Optional Spark master, for example local[*] for local verification.",
    )
    args = parser.parse_args(argv)
    args.input = normalize_spark_path(args.input)
    args.output_base = normalize_spark_path(args.output_base)
    return args


def create_spark_session(master=None):
    builder = SparkSession.builder.appName("EarthquakeBatchAnalysis")
    if master:
        builder = builder.master(master)
    return builder.getOrCreate()


def analyze_by_magnitude(df):
    # Categorize earthquakes: Minor (<2), Light (2-4), Moderate (4-6), Strong (6-8), Major (>8)
    # This helps us understand the distribution of earthquake strengths
    df_with_ranges = df.withColumn(
        "mag_range",
        when(col("mag") < 2, lit("Minor"))
        .when((col("mag") >= 2) & (col("mag") < 4), lit("Light"))
        .when((col("mag") >= 4) & (col("mag") < 6), lit("Moderate"))
        .when((col("mag") >= 6) & (col("mag") <= 8), lit("Strong"))
        .otherwise(lit("Major")),
    )
    
    mag_ranges = (
        df_with_ranges.groupBy("mag_range")
        .agg(count("*").alias("count"))
        .orderBy(col("count").desc())
    )
    
    print("Magnitude Distribution:")
    mag_ranges.show(truncate=False)
    return mag_ranges


def analyze_by_region(df):
    # See which regions have the most earthquake activity
    # Helps identify which areas are most seismically active
    region_counts = (
        df.groupBy("region")
        .agg(count("*").alias("count"))
        .orderBy(col("count").desc())
    )
    
    print("\nEarthquakes by Region:")
    region_counts.show(truncate=False)
    return region_counts


def analyze_top_places(df):
    # Find the 10 places that have had the most earthquakes
    # Useful for identifying major earthquake hotspots
    top_places = (
        df.groupBy("place")
        .agg(count("*").alias("count"))
        .orderBy(col("count").desc())
        .limit(10)
    )
    
    print("\nTop 10 Most Affected Places:")
    top_places.show(truncate=False)
    return top_places


def analyze_pakistan(df):
    # Calculate stats specifically for Pakistan: count, average/min/max magnitude
    # Helps us understand seismic risk in the Pakistan region
    pakistan_df = df.filter(col("region") == "Pakistan")
    
    pakistan_stats = pakistan_df.agg(
        count("*").alias("total"),
        avg("mag").alias("avg_mag"),
        spark_max("mag").alias("max_mag"),
        spark_min("mag").alias("min_mag"),
    )
    
    print("\nPakistan Earthquake Statistics:")
    pakistan_stats.show(truncate=False)
    return pakistan_stats


def main(argv=None) -> None:
    args = parse_args(argv)
    spark = create_spark_session(args.master)

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(args.input)
    )

    print("\n" + "=" * 50)
    print("  EARTHQUAKE BATCH ANALYSIS")
    print("=" * 50)

    total_records = df.count()
    print(f"\nTotal earthquake records: {total_records}\n")

    # Run all analyses
    mag_ranges = analyze_by_magnitude(df)
    region_counts = analyze_by_region(df)
    top_places = analyze_top_places(df)
    pakistan_stats = analyze_pakistan(df)

    # Save results to HDFS
    print("\nSaving Spark results...")
    mag_ranges.write.mode("overwrite").option("header", True).csv(f"{args.output_base}/mag_ranges")
    region_counts.write.mode("overwrite").option("header", True).csv(f"{args.output_base}/region_counts")
    top_places.write.mode("overwrite").option("header", True).csv(f"{args.output_base}/top_places")
    pakistan_stats.write.mode("overwrite").option("header", True).csv(f"{args.output_base}/pakistan_stats")

    print(f"✓ Results saved to: {args.output_base}/\n")

    spark.stop()


if __name__ == "__main__":
    main()
