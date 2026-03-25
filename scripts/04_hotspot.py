# Find earthquake hotspots by dividing the map into a grid
# Earthquakes are grouped by location, and high-activity areas are marked as CRITICAL/HIGH risk

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, round, when


INPUT_PATH = "/earthquake/input/earthquakes.csv"
GLOBAL_OUTPUT_PATH = "/earthquake/output/hotspots/global"
PAKISTAN_OUTPUT_PATH = "/earthquake/output/hotspots/pakistan"


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


def main() -> None:
    spark = SparkSession.builder.appName("EarthquakeHotspotDetection").getOrCreate()

    print("\n" + "=" * 50)
    print("  EARTHQUAKE HOTSPOT DETECTION")
    print("=" * 50 + "\n")

    # Read earthquake data
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(INPUT_PATH)
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
    print("\nSaving results to HDFS...")
    global_hotspots.write.mode("overwrite").option("header", True).csv(GLOBAL_OUTPUT_PATH)
    pakistan_hotspots.write.mode("overwrite").option("header", True).csv(PAKISTAN_OUTPUT_PATH)

    print(f"✓ Global hotspots  → {GLOBAL_OUTPUT_PATH}")
    print(f"✓ Pakistan hotspots → {PAKISTAN_OUTPUT_PATH}\n")

    spark.stop()


if __name__ == "__main__":
    main()
