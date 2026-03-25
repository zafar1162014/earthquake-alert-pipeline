import os
import time
from pathlib import Path

import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, lit, when


APP_NAME = "EarthquakeAmdahlBenchmark"
PARALLEL_FRACTION = 0.85


def theoretical_speedup(cores: int, parallel_fraction: float = PARALLEL_FRACTION) -> float:
    return 1.0 / ((parallel_fraction / cores) + (1.0 - parallel_fraction))


def run_spark_job(core_count: int) -> float:
    project_root = Path(__file__).resolve().parents[1]
    input_path = str(project_root / "data" / "earthquakes.csv")
    tmp_dir = project_root / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder.master(f"local[{core_count}]")
        .appName(APP_NAME)
        .config("spark.local.dir", str(tmp_dir))
        .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={tmp_dir}")
        .config("spark.executor.extraJavaOptions", f"-Djava.io.tmpdir={tmp_dir}")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    start_time = time.time()
    try:
        dataframe = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(input_path)
        )

        dataframe.count()

        dataframe_with_ranges = dataframe.withColumn(
            "mag_range",
            when(col("mag") < 2, lit("Minor"))
            .when((col("mag") >= 2) & (col("mag") < 4), lit("Light"))
            .when((col("mag") >= 4) & (col("mag") < 6), lit("Moderate"))
            .when((col("mag") >= 6) & (col("mag") <= 8), lit("Strong"))
            .otherwise(lit("Major")),
        )

        (
            dataframe_with_ranges.groupBy("mag_range")
            .agg(count("*").alias("count"))
            .orderBy(col("count").desc())
            .collect()
        )

        (
            dataframe.groupBy("region")
            .agg(count("*").alias("count"))
            .orderBy(col("count").desc())
            .collect()
        )

        (
            dataframe.groupBy("place")
            .agg(count("*").alias("count"))
            .orderBy(col("count").desc())
            .limit(10)
            .collect()
        )

        (
            dataframe.filter(col("region") == "Pakistan")
            .agg(avg("mag").alias("avg_mag"))
            .collect()
        )

        elapsed_seconds = time.time() - start_time
        return elapsed_seconds
    finally:
        spark.stop()


def print_summary(cores_list: list[int], times: dict[int, float], actual_speedups: dict[int, float]) -> None:
    print("\n============================================")
    print(" Results Summary")
    print("============================================\n")
    print("+-------+-----------+-----------------+--------------------+")
    print("| Cores | Time (s)  | Actual Speedup  | Theoretical Speedup|")
    print("+-------+-----------+-----------------+--------------------+")

    for cores in cores_list:
        time_taken = times[cores]
        actual = actual_speedups[cores]
        theoretical = theoretical_speedup(cores)
        print(f"|{cores:^7}|{time_taken:^11.2f}|{actual:^17.2f}|{theoretical:^20.2f}|")

    print("+-------+-----------+-----------------+--------------------+")


def build_chart(output_file: Path, times: dict[int, float], actual_speedups: dict[int, float]) -> None:
    measured_cores = [1, 2, 4]
    measured_values = [actual_speedups[c] for c in measured_cores]

    full_cores = list(range(1, 33))
    theoretical_values = [theoretical_speedup(core) for core in full_cores]

    figure, axes = plt.subplots(1, 2, figsize=(14, 5))

    bars = axes[0].bar([str(c) for c in measured_cores], measured_values, color=["#4C78A8", "#F58518", "#54A24B"])
    axes[0].set_title("Actual Speedup (Measured)")
    axes[0].set_xlabel("Number of Cores")
    axes[0].set_ylabel("Speedup")
    axes[0].grid(axis="y", linestyle="--", alpha=0.5)

    for bar, value in zip(bars, measured_values):
        axes[0].text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.03,
            f"{value:.2f}",
            ha="center",
            va="bottom",
        )

    axes[1].plot(full_cores, theoretical_values, label="Theoretical Amdahl Curve (P=0.85)", linewidth=2)
    axes[1].plot(measured_cores, measured_values, marker="o", linewidth=2, label="Actual Measured Speedup")
    axes[1].set_title("Amdahl's Law: Theoretical vs Actual")
    axes[1].set_xlabel("Number of Cores")
    axes[1].set_ylabel("Speedup")
    axes[1].grid(True, linestyle="--", alpha=0.5)
    axes[1].legend()

    figure.tight_layout()
    figure.savefig(output_file, dpi=200)
    try:
        plt.show(block=False)
        plt.pause(2)
    except Exception:
        pass
    finally:
        plt.close(figure)


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    tmp_dir = project_root / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    os.environ["TMPDIR"] = str(tmp_dir)

    print("============================================")
    print(" Amdahl's Law Benchmark")
    print("============================================\n")

    cores_to_test = [1, 2, 4]
    execution_times: dict[int, float] = {}

    for core_count in cores_to_test:
        elapsed = run_spark_job(core_count)
        execution_times[core_count] = elapsed
        print(f"Running with {core_count} core{'s' if core_count > 1 else ''}...   Time: {elapsed:.2f} seconds")

    baseline = execution_times[1]
    actual_speedups = {
        1: 1.0,
        2: baseline / execution_times[2],
        4: baseline / execution_times[4],
    }

    output_dir = project_root / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    chart_path = output_dir / "speedup_chart.png"

    build_chart(chart_path, execution_times, actual_speedups)
    print_summary(cores_to_test, execution_times, actual_speedups)

    print(f"\nChart saved to: {chart_path}")


if __name__ == "__main__":
    main()
