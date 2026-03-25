from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, trim, when


SOCKET_HOST = "localhost"
SOCKET_PORT = 9999
ALERT_OUTPUT_PATH = "/earthquake/output/streaming/alerts"
CHECKPOINT_PATH = "/earthquake/output/streaming/checkpoint"


def main() -> None:
    spark = SparkSession.builder.appName("EarthquakeStreamingAlert").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("Earthquake Alert System Started. Awaiting live data.")

    raw_stream = (
        spark.readStream
        .format("socket")
        .option("host", SOCKET_HOST)
        .option("port", SOCKET_PORT)
        .load()
    )

    parts = split(col("value"), ",")

    parsed_stream = (
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

    alerts = (
        parsed_stream
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

    high_critical_alerts = alerts.filter(
        (col("alert_level") == "HIGH") | (col("alert_level") == "CRITICAL !!")
    ).select("time", "place", "mag", "depth", "alert_level", "is_pakistan")

    console_query = (
        high_critical_alerts.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .start()
    )

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

    spark.streams.awaitAnyTermination()

    console_query.stop()
    critical_query.stop()


if __name__ == "__main__":
    main()
