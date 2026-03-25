import os
import time
from pathlib import Path

import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, lit, when


APP_NAME = "EarthquakeAmdahlBenchmark"
# P = fraction of work that can be parallelized (85% in this case)
# The remaining 15% is sequential and can't be sped up no matter how many cores
PARALLEL_FRACTION = 0.85


def theoretical_speedup(cores: int, parallel_fraction: float = PARALLEL_FRACTION) -> float:
    # Amdahl's Law formula: S(N) = 1 / ((P/N) + (1-P))
    # N = number of cores
    # P = parallel fraction (how much work can run in parallel)
    # Result shows the maximum possible speedup with N cores
    parallel_part = parallel_fraction / cores
    sequential_part = 1.0 - parallel_fraction
    return 1.0 / (parallel_part + sequential_part)


def run_spark_job(core_count: int) -> float:
    # Run the analysis job with a specific number of cores and return execution time
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
        df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

        # Run a series of analysis queries to stress-test parallelization
        df.count()

        # Analysis 1: Magnitude distribution
        df_with_ranges = df.withColumn(
            "mag_range",
            when(col("mag") < 2, lit("Minor"))
            .when((col("mag") >= 2) & (col("mag") < 4), lit("Light"))
            .when((col("mag") >= 4) & (col("mag") < 6), lit("Moderate"))
            .when((col("mag") >= 6) & (col("mag") <= 8), lit("Strong"))
            .otherwise(lit("Major")),
        )
        df_with_ranges.groupBy("mag_range").agg(count("*").alias("count")).orderBy(col("count").desc()).collect()

        # Analysis 2: Regional distribution
        df.groupBy("region").agg(count("*").alias("count")).orderBy(col("count").desc()).collect()

        # Analysis 3: Top 10 places
        df.groupBy("place").agg(count("*").alias("count")).orderBy(col("count").desc()).limit(10).collect()

        # Analysis 4: Pakistan stats
        df.filter(col("region") == "Pakistan").agg(avg("mag").alias("avg_mag")).collect()

        elapsed = time.time() - start_time
        return elapsed
    finally:
        spark.stop()


def print_results_table(cores_list: list[int], times: dict[int, float], speedups: dict[int, float]) -> None:
    # Print a nice table showing the benchmark results
    print("\n" + "=" * 70)
    print("  RESULTS SUMMARY")
    print("=" * 70 + "\n")
    
    header = f"{'Cores':<8} {'Time (s)':<12} {'Actual Speedup':<18} {'Theoretical':<12}"
    print(header)
    print("-" * 70)
    
    for cores in cores_list:
        time_val = times[cores]
        actual = speedups[cores]
        theoretical = theoretical_speedup(cores)
        print(f"{cores:<8} {time_val:<12.2f} {actual:<18.2f} {theoretical:<12.2f}")
    
    print("-" * 70 + "\n")


def save_chart(output_file: Path, times: dict[int, float], speedups: dict[int, float]) -> None:
    # Create a professional chart comparing actual vs theoretical speedup
    measured_cores = [1, 2, 4]
    measured_speedups = [speedups[c] for c in measured_cores]

    # Generate theoretical curve for up to 32 cores
    all_cores = list(range(1, 33))
    theoretical_speedups = [theoretical_speedup(c) for c in all_cores]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    # Left plot: Bar chart of actual measurements
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c"]
    bars = ax1.bar([str(c) for c in measured_cores], measured_speedups, color=colors, alpha=0.8, edgecolor="black")
    ax1.set_title("Actual Speedup (Measured)", fontsize=12, fontweight="bold")
    ax1.set_xlabel("Number of Cores", fontsize=11)
    ax1.set_ylabel("Speedup Factor", fontsize=11)
    ax1.grid(axis="y", linestyle="--", alpha=0.4)
    
    # Add value labels on bars
    for bar, value in zip(bars, measured_speedups):
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width() / 2, height + 0.05, f"{value:.2f}", 
                ha="center", va="bottom", fontsize=10)

    # Right plot: Line graph comparing actual vs theoretical
    ax2.plot(all_cores, theoretical_speedups, "b-", linewidth=2.5, label="Theoretical (P=0.85)", alpha=0.8)
    ax2.plot(measured_cores, measured_speedups, "ro-", linewidth=2, markersize=7, label="Actual Measured", alpha=0.8)
    ax2.set_title("Amdahl's Law: Theory vs Reality", fontsize=12, fontweight="bold")
    ax2.set_xlabel("Number of Cores", fontsize=11)
    ax2.set_ylabel("Speedup Factor", fontsize=11)
    ax2.grid(True, linestyle="--", alpha=0.4)
    ax2.legend(loc="lower right", fontsize=10)
    ax2.set_xlim(0, 33)

    fig.tight_layout()
    fig.savefig(output_file, dpi=200, bbox_inches="tight")
    print(f"✓ Chart saved to: {output_file}\n")
    
    try:
        plt.close(fig)
    except Exception:
        pass


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    tmp_dir = project_root / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    os.environ["TMPDIR"] = str(tmp_dir)

    print("\n" + "=" * 70)
    print("  AMDAHL'S LAW BENCHMARK - EARTHQUAKE ANALYSIS")
    print("=" * 70 + "\n")

    cores_to_test = [1, 2, 4]
    execution_times = {}

    # Run the benchmark with different core counts
    for num_cores in cores_to_test:
        print(f"Running with {num_cores} core(s)...", end=" ", flush=True)
        elapsed = run_spark_job(num_cores)
        execution_times[num_cores] = elapsed
        print(f"✓ {elapsed:.2f}s")

    # Calculate speedup ratios (compared to single-core baseline)
    baseline_time = execution_times[1]
    actual_speedups = {
        1: 1.0,
        2: baseline_time / execution_times[2],
        4: baseline_time / execution_times[4],
    }

    # Save chart and print results
    output_dir = project_root / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    chart_path = output_dir / "speedup_chart.png"

    save_chart(chart_path, execution_times, actual_speedups)
    print_results_table(cores_to_test, execution_times, actual_speedups)
    
    # Interpretation
    print("Interpretation:")
    print(f"  • 2 cores achieved {actual_speedups[2]:.2f}x speedup (theory: {theoretical_speedup(2):.2f}x)")
    print(f"  • 4 cores achieved {actual_speedups[4]:.2f}x speedup (theory: {theoretical_speedup(4):.2f}x)")
    print(f"  • Gap between actual and theory shows overhead from parallelization\n")


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
