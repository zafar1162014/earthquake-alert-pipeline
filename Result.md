# EarthquakeWatch Result Guide

## 1) Project Overview

EarthquakeWatch is a full data pipeline and dashboard project for earthquake monitoring.
It combines:

- Data collection from the USGS API
- Storage in local CSV and Hadoop HDFS
- Batch analytics with Apache Spark
- Hotspot detection with Spark
- Real-time streaming alerts with Spark Structured Streaming
- A Flask dashboard for visualization and API access
- A benchmark script (Amdahl's Law) to explain and measure parallel speedup

This file is a detailed explanation for presentation, verification, and learning.
It does not replace `README.md`.

## 2) Purpose Of The Application

The project helps answer practical questions such as:

- How many earthquakes happened recently?
- Which regions are most active?
- Which map cells are hotspots?
- Which events should raise HIGH or CRITICAL alerts?
- How much speedup do we get from parallel execution?

It is designed for learning distributed data processing and for demonstrating an end-to-end mini big-data system.

## 3) Full Architecture Flow

```text
USGS API
  -> scripts/01_download_data.py
  -> data/earthquakes.csv (local)
  -> scripts/02_upload_hdfs.sh
  -> /earthquake/input/earthquakes.csv (HDFS)
  -> Spark Batch (03_batch_analysis.py)
       -> /earthquake/output/batch/*
  -> Spark Hotspot (04_hotspot.py)
       -> /earthquake/output/hotspots/*
  -> Stream Feed (05_stream_feed.py, localhost:9999)
       -> Stream Consumer (06_stream_alert.py)
       -> /earthquake/output/streaming/alerts (Parquet)

Flask Dashboard (app.py)
  -> reads data/earthquakes.csv
  -> serves templates/index.html
  -> serves JSON APIs and speedup chart
```

### Architecture Notes

- HDFS is the default distributed storage target for Spark pipeline scripts.
- Local mode is available for development by passing CLI overrides such as `--master 'local[*]'` and local input/output paths.
- The dashboard reads local files for fast UI access.

## 4) How Hadoop Works In This Project

Hadoop (HDFS) is used as distributed storage for pipeline outputs.

### HDFS default paths used by this project

- Input: `/earthquake/input/earthquakes.csv`
- Batch output base: `/earthquake/output/batch`
- Hotspot output base: `/earthquake/output/hotspots`
- Streaming output: `/earthquake/output/streaming/alerts`
- Streaming checkpoint: `/earthquake/output/streaming/checkpoint`

### Verify Hadoop and HDFS

```bash
java -version
command -v hdfs
hdfs getconf -confKey fs.defaultFS
jps
hdfs dfs -ls /
```

Expected key process in `jps`: `NameNode` (and usually `DataNode`).

### Upload local CSV to HDFS

```bash
bash scripts/02_upload_hdfs.sh
```

Manual verification:

```bash
hdfs dfs -ls /earthquake/input
hdfs dfs -du -h /earthquake/input
```

## 5) How Spark Works In This Project

Spark is used in three ways:

- Batch analytics (`03_batch_analysis.py`)
- Hotspot grid analysis (`04_hotspot.py`)
- Structured Streaming alerts (`06_stream_alert.py`)

### 5.1 Batch analytics (03_batch_analysis.py)

This script reads earthquake CSV data and computes:

- Magnitude range distribution
- Region counts
- Top places by earthquake count
- Pakistan-specific statistics (total, average, min, max magnitude)

Default behavior:

- Reads from HDFS input path
- Writes to HDFS batch output path

Run with defaults (HDFS):

```bash
.venv/bin/python scripts/03_batch_analysis.py
```

Verify HDFS batch outputs:

```bash
hdfs dfs -ls /earthquake/output/batch
hdfs dfs -ls /earthquake/output/batch/mag_ranges
hdfs dfs -ls /earthquake/output/batch/region_counts
hdfs dfs -ls /earthquake/output/batch/top_places
hdfs dfs -ls /earthquake/output/batch/pakistan_stats
```

### 5.2 Local Spark mode (no HDFS)

Use this when Hadoop is not running or for quick development checks.

```bash
.venv/bin/python scripts/03_batch_analysis.py \
  --master 'local[*]' \
  --input data/earthquakes.csv \
  --output-base output/batch
```

Check local output folders:

```bash
ls output/batch
```

### 5.3 Hotspot analysis (04_hotspot.py)

This script rounds latitude/longitude into grid cells (0.1 degree precision), counts events per cell, and adds a risk level.

Run with defaults (HDFS):

```bash
.venv/bin/python scripts/04_hotspot.py
```

Verify HDFS hotspot outputs:

```bash
hdfs dfs -ls /earthquake/output/hotspots
hdfs dfs -ls /earthquake/output/hotspots/global
hdfs dfs -ls /earthquake/output/hotspots/pakistan
```

Run hotspot in local Spark mode:

```bash
.venv/bin/python scripts/04_hotspot.py \
  --master 'local[*]' \
  --input data/earthquakes.csv \
  --output-base output/hotspots
```

### 5.4 Streaming alerts (05_stream_feed.py + 06_stream_alert.py)

Streaming uses two processes:

1. `05_stream_feed.py` opens a TCP server on `localhost:9999` and sends CSV rows line-by-line.
2. `06_stream_alert.py` uses Spark Structured Streaming to consume socket data, classify alerts, print HIGH/CRITICAL alerts to console, and write CRITICAL alerts to Parquet.

Run order (important):

Terminal 1:

```bash
.venv/bin/python scripts/05_stream_feed.py
```

Terminal 2:

```bash
.venv/bin/python scripts/06_stream_alert.py
```

Verify streaming output path (HDFS):

```bash
hdfs dfs -ls /earthquake/output/streaming
hdfs dfs -ls /earthquake/output/streaming/alerts
```

### 5.5 Amdahl benchmark (07_amdahl.py)

This script runs Spark jobs with `local[1]`, `local[2]`, and `local[4]` and compares:

- Actual measured speedup
- Theoretical speedup from Amdahl's Law

It saves:

- `output/speedup_chart.png`

Run:

```bash
mkdir -p tmp
export TMPDIR="$PWD/tmp"
.venv/bin/python scripts/07_amdahl.py
```

Verify chart exists:

```bash
ls -lh output/speedup_chart.png
```

## 6) Parallel Execution Explained (Beginner Friendly)

### `local[*]`, `local[1]`, `local[2]`, `local[4]`

- `local[*]`: Spark uses all available CPU cores on your machine.
- `local[1]`: Spark uses 1 core (baseline, mostly sequential).
- `local[2]`: Spark uses 2 cores.
- `local[4]`: Spark uses 4 cores.

More cores usually reduce runtime, but speedup is not perfect.

### Why speedup is limited

Amdahl's Law says total speedup is limited by the sequential part of the job.
If a fraction of the work cannot be parallelized, that part becomes the bottleneck.

Formula used by the project:

$S(N) = \frac{1}{(P/N) + (1-P)}$

- $N$: number of cores
- $P$: parallel fraction
- $S(N)$: theoretical maximum speedup

### Spark partitions, transformations, and actions

- Data is split into partitions.
- Transformations (for example `withColumn`, `groupBy`) build a lazy execution plan.
- Actions (for example `count`, `collect`, `show`, writes) trigger execution.
- More partitions and more cores can improve throughput, with overhead trade-offs.

### HDFS role in parallelism

HDFS stores data in distributed blocks, which allows parallel reads/writes across workers in real clusters.
In this project, HDFS paths model distributed workflow even when running on local infrastructure.

## 7) File-By-File Explanation

### Core application

- `app.py`
  - Flask app and API backend.
  - Loads local CSV (`data/earthquakes.csv`), computes summary/recent/hotspot JSON responses.
  - Serves `templates/index.html`.
  - Serves benchmark image at `/api/speedup`.
  - Provides `POST /api/refresh-run` to trigger local refresh scripts.
  - Runs on `localhost:5001`.

- `templates/index.html`
  - Dashboard frontend page.
  - Uses Bootstrap, Leaflet, Chart.js, and Three.js for UI components (map/charts/visual effects).
  - Fetches Flask APIs to render live summary and visual content.

### Pipeline scripts

- `scripts/01_download_data.py`
  - Downloads earthquake CSV data from USGS.
  - Combines global and Pakistan-focused data.
  - Adds/assigns `region` values.
  - Writes `data/earthquakes.csv`.

- `scripts/02_upload_hdfs.sh`
  - Shell script to validate Hadoop/HDFS availability.
  - Checks NameNode and HDFS root access.
  - Creates `/earthquake/input` and `/earthquake/output` directories.
  - Uploads local CSV to HDFS input path.

- `scripts/03_batch_analysis.py`
  - Spark batch analytics script.
  - Default input/output paths target HDFS.
  - Supports local Spark mode with CLI overrides.

- `scripts/04_hotspot.py`
  - Spark hotspot detection script.
  - Bins coordinates into grids and classifies risk level.
  - Default output is HDFS; local mode supported via CLI overrides.

- `scripts/05_stream_feed.py`
  - TCP socket producer.
  - Streams local CSV rows to `localhost:9999` with delay.

- `scripts/06_stream_alert.py`
  - Spark Structured Streaming consumer.
  - Parses socket rows, classifies alert levels.
  - Prints HIGH/CRITICAL alerts to console.
  - Writes CRITICAL alerts to Parquet in HDFS path.

- `scripts/07_amdahl.py`
  - Runs the same analysis workload with multiple local core counts.
  - Calculates theoretical + measured speedup.
  - Saves `output/speedup_chart.png`.

### Tests and configs

- `tests/test_pipeline_config.py`
  - Verifies argument defaults and path normalization for batch/hotspot scripts.
  - Confirms HDFS defaults and local override behavior.

- `tests/test_pipeline_helpers.py`
  - Verifies stream formatting helper behavior.
  - Verifies Amdahl formula output.
  - Verifies scripts include one `if __name__ == "__main__"` guard.

- `requirements.txt`
  - Main app dependencies (Flask, pandas, requests).

- `requirements-pipeline.txt`
  - Pipeline extras (PySpark, matplotlib) and base requirements include.

- `vercel.json`
  - Deploy route/build config for serving `app.py` on Vercel.

### Data and outputs

- `data/earthquakes.csv`
  - Local dataset generated by `01_download_data.py`.

- `output/speedup_chart.png`
  - Benchmark chart generated by `07_amdahl.py`.

- `output/batch/*`, `output/hotspots/*`
  - Local Spark output examples (when local mode is used).

- HDFS directories under `/earthquake/output/*`
  - Distributed output locations when defaults are used.

## 8) Step-By-Step Run Commands (Fresh Setup To Dashboard)

### 8.1 Create environment and install dependencies

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-pipeline.txt
```

### 8.2 Download data

```bash
.venv/bin/python scripts/01_download_data.py
```

### 8.3 Optional: upload to HDFS

```bash
bash scripts/02_upload_hdfs.sh
```

### 8.4 Run Spark analyses

HDFS default mode:

```bash
.venv/bin/python scripts/03_batch_analysis.py
.venv/bin/python scripts/04_hotspot.py
```

Local Spark mode:

```bash
.venv/bin/python scripts/03_batch_analysis.py \
  --master 'local[*]' --input data/earthquakes.csv --output-base output/batch

.venv/bin/python scripts/04_hotspot.py \
  --master 'local[*]' --input data/earthquakes.csv --output-base output/hotspots
```

### 8.5 Run streaming demo

Terminal 1:

```bash
.venv/bin/python scripts/05_stream_feed.py
```

Terminal 2:

```bash
.venv/bin/python scripts/06_stream_alert.py
```

### 8.6 Run benchmark chart

```bash
mkdir -p tmp
export TMPDIR="$PWD/tmp"
.venv/bin/python scripts/07_amdahl.py
```

### 8.7 Run dashboard

```bash
.venv/bin/python app.py
```

Open:

```text
http://localhost:5001
```

## 9) Exact Verification Commands

### Python syntax check

```bash
python3 -m py_compile app.py scripts/01_download_data.py scripts/03_batch_analysis.py scripts/04_hotspot.py scripts/05_stream_feed.py scripts/06_stream_alert.py scripts/07_amdahl.py
```

### Unit tests

```bash
python3 -m unittest discover -s tests -v
```

### Flask API smoke checks

```bash
python3 - <<'PY'
from app import app
client = app.test_client()
for path in ['/api/summary', '/api/recent', '/api/hotspots', '/api/speedup']:
    r = client.get(path)
    print(path, r.status_code, r.content_type)
PY
```

### Hadoop verification

```bash
jps
hdfs dfs -ls /
bash scripts/02_upload_hdfs.sh
hdfs dfs -ls /earthquake/input
```

### Spark local mode checks

```bash
.venv/bin/python scripts/03_batch_analysis.py --master 'local[*]' --input data/earthquakes.csv --output-base output/batch
.venv/bin/python scripts/04_hotspot.py --master 'local[*]' --input data/earthquakes.csv --output-base output/hotspots
ls output/batch
ls output/hotspots
```

### Spark HDFS output checks

```bash
hdfs dfs -ls /earthquake/output/batch
hdfs dfs -ls /earthquake/output/hotspots
```

### Streaming two-terminal check

```bash
# Terminal 1
.venv/bin/python scripts/05_stream_feed.py

# Terminal 2
.venv/bin/python scripts/06_stream_alert.py
```

### Amdahl chart check

```bash
.venv/bin/python scripts/07_amdahl.py
ls -lh output/speedup_chart.png
```

## 10) Troubleshooting

### 10.1 HDFS `Connection refused`

Symptoms:

- `hdfs dfs -ls /` fails with connection refused
- upload script reports HDFS not reachable

Fix:

```bash
start-dfs.sh
jps
hdfs dfs -ls /
```

Confirm NameNode is running and `fs.defaultFS` is correct:

```bash
hdfs getconf -confKey fs.defaultFS
```

### 10.2 Missing CSV (`data/earthquakes.csv`)

Symptoms:

- Flask APIs return CSV not found
- stream feed script reports missing file

Fix:

```bash
.venv/bin/python scripts/01_download_data.py
ls -lh data/earthquakes.csv
```

### 10.3 Spark/Py4J errors

Common causes:

- Java not installed or wrong version
- PySpark missing in environment
- wrong Python environment active

Checks:

```bash
java -version
.venv/bin/python -m pip show pyspark
```

If needed, reinstall pipeline dependencies:

```bash
pip install -r requirements-pipeline.txt
```

### 10.4 Missing speedup chart (`/api/speedup` returns 404)

Fix:

```bash
mkdir -p tmp
export TMPDIR="$PWD/tmp"
.venv/bin/python scripts/07_amdahl.py
ls -lh output/speedup_chart.png
```

### 10.5 Streaming socket order issue

Symptoms:

- stream consumer waits forever
- no records processed

Fix order:

1. Start `scripts/05_stream_feed.py` first (opens socket server).
2. Start `scripts/06_stream_alert.py` second (connects as client).

If port is busy, stop old process and retry.

### 10.6 Local Spark path mistakes

If local mode writes/reads wrong locations, pass explicit local paths:

```bash
--master 'local[*]' --input data/earthquakes.csv --output-base output/batch
```

The scripts normalize local relative paths to absolute filesystem paths internally.

## 11) Presentation Checklist

Use this quick checklist before demo:

- Data file exists: `data/earthquakes.csv`
- HDFS available (if using defaults)
- Batch and hotspot jobs completed
- Streaming demo runs with two terminals
- Speedup chart exists at `output/speedup_chart.png`
- Flask app starts on `http://localhost:5001`
- APIs return HTTP 200 for summary/recent/hotspots/speedup

## 12) Final Note

`Result.md` is a teaching and verification guide. It explains how all components work together and how to prove that each stage is working correctly in practice.
