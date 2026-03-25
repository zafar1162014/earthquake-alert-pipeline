from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round, when


INPUT_PATH = "/earthquake/input/earthquakes.csv"
GLOBAL_OUTPUT_PATH = "/earthquake/output/hotspots/global"
PAKISTAN_OUTPUT_PATH = "/earthquake/output/hotspots/pakistan"


def add_risk_level(dataframe):
    return dataframe.withColumn(
        "risk_level",
        when(col("count") > 50, "CRITICAL")
        .when(col("count") > 30, "HIGH")
        .when(col("count") > 10, "MEDIUM")
        .otherwise("LOW"),
    )


def main() -> None:
    spark = SparkSession.builder.appName("EarthquakeHotspotDetection").getOrCreate()

    print("============================================")
    print(" Earthquake Hotspot Detection")
    print("============================================")

    print("\nStep 1: Creating geographic grid cells (lat_grid, lon_grid)...")
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(INPUT_PATH)
    )

    grid_df = (
        df.withColumn("lat_grid", round(col("latitude"), 1))
        .withColumn("lon_grid", round(col("longitude"), 1))
    )

    print("\nStep 2: Counting earthquakes by grid cell globally...")
    global_hotspots = (
        grid_df.groupBy("lat_grid", "lon_grid")
        .agg(count("*").alias("count"))
        .orderBy(col("count").desc())
    )

    global_hotspots_with_risk = add_risk_level(global_hotspots)

    print("Top 10 Global Hotspots:")
    global_hotspots_with_risk.limit(10).show(truncate=False)

    print("\nStep 3: Counting earthquakes by grid cell for Pakistan region...")
    pakistan_hotspots = (
        grid_df.filter(col("region") == "Pakistan")
        .groupBy("lat_grid", "lon_grid")
        .agg(count("*").alias("count"))
        .orderBy(col("count").desc())
    )

    pakistan_hotspots_with_risk = add_risk_level(pakistan_hotspots)

    print("Top 5 Pakistan Hotspots:")
    pakistan_hotspots_with_risk.limit(5).show(truncate=False)

    print("\nStep 4: Applying hotspot risk levels (CRITICAL/HIGH/MEDIUM/LOW)...")
    print("Risk levels added to hotspot tables.")

    print("\nStep 5: Saving hotspot results to HDFS as CSV...")
    global_hotspots_with_risk.write.mode("overwrite").option("header", True).csv(GLOBAL_OUTPUT_PATH)
    pakistan_hotspots_with_risk.write.mode("overwrite").option("header", True).csv(PAKISTAN_OUTPUT_PATH)

    print("\nResults saved to HDFS:")
    print(f"  Global   -> {GLOBAL_OUTPUT_PATH}")
    print(f"  Pakistan -> {PAKISTAN_OUTPUT_PATH}")
    print("\nSuccess: Hotspot detection completed.")

    spark.stop()


if __name__ == "__main__":
    main()
