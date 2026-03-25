from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, lit, max as spark_max, min as spark_min, when


INPUT_PATH = "/earthquake/input/earthquakes.csv"
OUTPUT_BASE = "/earthquake/output/batch"


def main() -> None:
    spark = (
        SparkSession.builder.appName("EarthquakeBatchAnalysis")
        .getOrCreate()
    )

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(INPUT_PATH)
    )

    print("============================================")
    print(" Earthquake Batch Analysis")
    print("============================================")

    print("\nAnalysis 1: Total Earthquakes")
    total_earthquakes = df.count()
    print(f"Total records: {total_earthquakes}")

    print("\nAnalysis 2: By Magnitude Range")
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
    mag_ranges.show(truncate=False)

    print("\nAnalysis 3: By Region")
    region_counts = (
        df.groupBy("region")
        .agg(count("*").alias("count"))
        .orderBy(col("count").desc())
    )
    region_counts.show(truncate=False)

    print("\nAnalysis 4: Top 10 Most Affected Places")
    top_places = (
        df.groupBy("place")
        .agg(count("*").alias("count"))
        .orderBy(col("count").desc())
        .limit(10)
    )
    top_places.show(truncate=False)

    print("\nAnalysis 5: Pakistan Statistics")
    pakistan_df = df.filter(col("region") == "Pakistan")

    pakistan_stats = pakistan_df.agg(
        count("*").alias("total"),
        avg("mag").alias("avg_mag"),
        spark_max("mag").alias("max_mag"),
        spark_min("mag").alias("min_mag"),
    )
    pakistan_stats.show(truncate=False)

    mag_ranges.write.mode("overwrite").option("header", True).csv(f"{OUTPUT_BASE}/mag_ranges")
    region_counts.write.mode("overwrite").option("header", True).csv(f"{OUTPUT_BASE}/region_counts")
    top_places.write.mode("overwrite").option("header", True).csv(f"{OUTPUT_BASE}/top_places")
    pakistan_stats.write.mode("overwrite").option("header", True).csv(f"{OUTPUT_BASE}/pakistan_stats")

    print(f"\nResults saved to HDFS: {OUTPUT_BASE}/")

    spark.stop()


if __name__ == "__main__":
    main()
