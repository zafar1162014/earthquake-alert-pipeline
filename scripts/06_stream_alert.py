# Real-time earthquake alert system using Spark Streaming
# Consumes live earthquake data from socket, classifies by severity, and outputs alerts
# Critical events (mag > 6) are saved to Parquet for permanent storage

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, trim, when


SOCKET_HOST = "localhost"
SOCKET_PORT = 9999
ALERT_OUTPUT_PATH = "/earthquake/output/streaming/alerts"
CHECKPOINT_PATH = "/earthquake/output/streaming/checkpoint"


def parse_stream_data(raw_stream):
    # Parse comma-separated data from socket into structured columns
    parts = split(col("value"), ",")
    
    return (
        raw_stream
        .withColumn("time", trim(parts.getItem(0)))
        .withColumn("latitude", trim(parts.getItem(1)).cast("double"))
        .withColumn("longitude", trim(parts.getItem(2)).cast("double"))
        .withColumn("depth", trim(parts.getItem(3)).cast("double"))
        .withColumn("mag", trim(parts.getItem(4)).cast("double"))
        .withColumn("place", trim(parts.getItem(5)))
        .withColumn("type", trim(parts.getItem(6)))
        .withColumn("region", trim(parts.getItem(7)))
        .drop("value")
        .filter(col("time").isNotNull() & col("mag").isNotNull())
    )


def classify_alert_level(stream_df):
    # Classify earthquakes by magnitude into alert levels
    # CRITICAL (mag > 6.0): Extremely dangerous - major damage possible
    # HIGH (mag > 5.0): Dangerous - significant damage likely
    # MEDIUM (mag > 4.0): Moderate - buildings may be affected
    # LOW (mag <= 4.0): Minor - usually not dangerous
    return (
        stream_df
        .withColumn(
            "alert_level",
            when(col("mag") > 6.0, "CRITICAL")
            .when(col("mag") > 5.0, "HIGH")
            .when(col("mag") > 4.0, "MEDIUM")
            .otherwise("LOW"),
        )
        .withColumn(
            "alert_level",
            when(col("alert_level") == "CRITICAL", "CRITICAL !!").otherwise(col("alert_level")),
        )
        .withColumn("is_pakistan", col("region") == "Pakistan")
    )


def main() -> None:
    spark = SparkSession.builder.appName("EarthquakeStreamingAlert").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("\n" + "=" * 50)
    print("  REAL-TIME EARTHQUAKE ALERT SYSTEM")
    print("=" * 50)
    print("\nListening for earthquake data on socket...\n")

    # Connect to data stream
    raw_stream = (
        spark.readStream
        .format("socket")
        .option("host", SOCKET_HOST)
        .option("port", SOCKET_PORT)
        .load()
    )

    # Parse and classify alerts
    parsed_stream = parse_stream_data(raw_stream)
    alerts = classify_alert_level(parsed_stream)

    # Show HIGH and CRITICAL alerts in console (for monitoring)
    high_critical = alerts.filter(
        (col("alert_level") == "HIGH") | (col("alert_level") == "CRITICAL !!")
    ).select("time", "place", "mag", "depth", "alert_level", "is_pakistan")

    console_query = (
        high_critical.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .start()
    )

    # Save CRITICAL alerts to Parquet (for permanent record)
    critical_alerts = alerts.filter(col("alert_level") == "CRITICAL !!").select(
        "time", "latitude", "longitude", "depth", "mag", "place", "type", "region", "alert_level", "is_pakistan"
    )

    critical_query = (
        critical_alerts.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", ALERT_OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .start()
    )

    # Keep streaming until interrupted
    spark.streams.awaitAnyTermination()

    console_query.stop()
    critical_query.stop()


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
