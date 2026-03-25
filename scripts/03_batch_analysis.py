# Analyze earthquake data using Spark batch processing
# Reads from HDFS and outputs results by magnitude, region, location, and Pakistan stats

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, lit, max as spark_max, min as spark_min, when

INPUT_PATH = "/earthquake/input/earthquakes.csv"
OUTPUT_BASE = "/earthquake/output/batch"


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


def main() -> None:
    spark = SparkSession.builder.appName("EarthquakeBatchAnalysis").getOrCreate()

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(INPUT_PATH)
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
    print("\nSaving results to HDFS...")
    mag_ranges.write.mode("overwrite").option("header", True).csv(f"{OUTPUT_BASE}/mag_ranges")
    region_counts.write.mode("overwrite").option("header", True).csv(f"{OUTPUT_BASE}/region_counts")
    top_places.write.mode("overwrite").option("header", True).csv(f"{OUTPUT_BASE}/top_places")
    pakistan_stats.write.mode("overwrite").option("header", True).csv(f"{OUTPUT_BASE}/pakistan_stats")

    print(f"✓ Results saved to: {OUTPUT_BASE}/\n")

    spark.stop()


if __name__ == "__main__":
    main()
