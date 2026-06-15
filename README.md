# EarthquakeWatch

EarthquakeWatch is an earthquake monitoring pipeline and dashboard for global and Pakistan-focused seismic events. It combines a Flask web dashboard, USGS data collection, Hadoop HDFS storage, PySpark batch analytics, Spark streaming alerts, and an Amdahl's Law benchmark.

## How It Works

1. `scripts/01_download_data.py` downloads earthquake CSV data from the USGS API and writes `data/earthquakes.csv`.
2. `scripts/02_upload_hdfs.sh` verifies Hadoop HDFS and uploads the CSV to `/earthquake/input/earthquakes.csv`.
3. `scripts/03_batch_analysis.py` runs Spark batch analytics for magnitude ranges, regions, top places, and Pakistan statistics.
4. `scripts/04_hotspot.py` runs Spark hotspot detection by latitude/longitude grid.
5. `scripts/05_stream_feed.py` simulates a live socket feed from the CSV.
6. `scripts/06_stream_alert.py` consumes that socket feed with Spark Structured Streaming and writes critical alerts.
7. `scripts/07_amdahl.py` runs the Spark speedup benchmark and writes `output/speedup_chart.png`.
8. `app.py` serves the Flask dashboard and JSON APIs from the local CSV and generated output chart.

HDFS is the main distributed storage path. The Spark batch and hotspot scripts also support local file paths for development and verification when Hadoop services are not running.

## Project Structure

```text
app.py                         Flask dashboard and API routes
data/earthquakes.csv           Local earthquake dataset
output/speedup_chart.png       Generated Amdahl benchmark chart
scripts/01_download_data.py    Download USGS earthquake data
scripts/02_upload_hdfs.sh      Verify Hadoop and upload CSV to HDFS
scripts/03_batch_analysis.py   Spark batch analysis
scripts/04_hotspot.py          Spark hotspot analysis
scripts/05_stream_feed.py      Socket stream simulator
scripts/06_stream_alert.py     Spark streaming alert consumer
scripts/07_amdahl.py           Spark Amdahl benchmark
templates/index.html           Dashboard UI
tests/                         Unit tests for pipeline helpers/config
```

## Requirements

- Python 3.10+
- Java/JDK, required by Hadoop and Spark
- Hadoop, required for the HDFS workflow
- Apache Spark or PySpark, required for Spark scripts
- Python packages from `requirements.txt` and `requirements-pipeline.txt`

This repository already separates dashboard runtime dependencies from heavier local pipeline dependencies:

- `requirements.txt`: Flask dashboard runtime
- `requirements-pipeline.txt`: PySpark and matplotlib pipeline dependencies

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-pipeline.txt
```

Verify the Python packages:

```bash
.venv/bin/python -m pip freeze | grep -E '^(Flask|pandas|requests|pyspark|matplotlib)=='
```

## Run The Dashboard

Download or refresh the CSV:

```bash
.venv/bin/python scripts/01_download_data.py
```

Start Flask:

```bash
.venv/bin/python app.py
```

Open:

```text
http://localhost:5001
```

Useful dashboard APIs:

```bash
curl http://localhost:5001/api/summary
curl http://localhost:5001/api/recent
curl http://localhost:5001/api/hotspots
curl http://localhost:5001/api/speedup --output speedup_chart.png
```

## Verify Hadoop HDFS

Check Java:

```bash
java -version
```

Check Hadoop commands and default filesystem:

```bash
command -v hdfs
hdfs getconf -confKey fs.defaultFS
```

Expected default filesystem for this project is usually:

```text
hdfs://localhost:9000
```

Start Hadoop DFS if it is not running:

```bash
start-dfs.sh
```

Verify Hadoop processes:

```bash
jps
```

Expected processes include:

```text
NameNode
DataNode
SecondaryNameNode
```

Verify HDFS is reachable:

```bash
hdfs dfs -ls /
```

If this fails with `Connection refused` to `localhost:9000`, Hadoop HDFS is installed but the NameNode is not running or is not listening on the configured port.

## Upload Data To HDFS

After `data/earthquakes.csv` exists and Hadoop is running:

```bash
bash scripts/02_upload_hdfs.sh
```

The script creates:

```text
/earthquake/input
/earthquake/output
```

Then it uploads:

```text
/earthquake/input/earthquakes.csv
```

Verify manually:

```bash
hdfs dfs -ls /earthquake/input
hdfs dfs -du -h /earthquake/input
```

## Run Spark With Hadoop HDFS

These commands use the default HDFS paths:

```bash
.venv/bin/python scripts/03_batch_analysis.py
.venv/bin/python scripts/04_hotspot.py
```

Expected HDFS outputs:

```text
/earthquake/output/batch/mag_ranges
/earthquake/output/batch/region_counts
/earthquake/output/batch/top_places
/earthquake/output/batch/pakistan_stats
/earthquake/output/hotspots/global
/earthquake/output/hotspots/pakistan
```

Verify outputs:

```bash
hdfs dfs -ls /earthquake/output
hdfs dfs -ls /earthquake/output/batch
hdfs dfs -ls /earthquake/output/hotspots
```

## Run Spark Locally Without HDFS

Use local mode when developing or when Hadoop is temporarily unavailable. This still uses Spark, but reads and writes local files instead of HDFS.

```bash
.venv/bin/python scripts/03_batch_analysis.py \
  --master 'local[*]' \
  --input data/earthquakes.csv \
  --output-base output/batch
```

```bash
.venv/bin/python scripts/04_hotspot.py \
  --master 'local[*]' \
  --input data/earthquakes.csv \
  --output-base output/hotspots
```

Local outputs:

```text
output/batch/
output/hotspots/
```

The scripts convert local relative paths to absolute filesystem paths internally so Spark can read local data even when Hadoop's `fs.defaultFS` points at HDFS.

## Run Streaming Alerts

Use two terminals.

Terminal 1, start the socket feed:

```bash
.venv/bin/python scripts/05_stream_feed.py
```

Terminal 2, start Spark streaming:

```bash
.venv/bin/python scripts/06_stream_alert.py
```

The streaming job reads from:

```text
localhost:9999
```

Critical alert output defaults to HDFS:

```text
/earthquake/output/streaming/alerts
/earthquake/output/streaming/checkpoint
```

Stop both commands with `Ctrl+C`.

## Run The Amdahl Benchmark

```bash
mkdir -p tmp
export TMPDIR="$PWD/tmp"
.venv/bin/python scripts/07_amdahl.py
```

Generated chart:

```text
output/speedup_chart.png
```

The dashboard serves this chart from:

```text
/api/speedup
```

## Build And Verification Checklist

Run syntax checks:

```bash
.venv/bin/python -m py_compile app.py scripts/01_download_data.py scripts/03_batch_analysis.py scripts/04_hotspot.py scripts/05_stream_feed.py scripts/06_stream_alert.py scripts/07_amdahl.py
```

Run unit tests:

```bash
.venv/bin/python -m unittest discover -s tests -v
```

Run Flask API smoke checks:

```bash
.venv/bin/python - <<'PY'
from app import app
client = app.test_client()
for path in ["/api/summary", "/api/recent", "/api/hotspots", "/api/speedup"]:
    response = client.get(path)
    print(path, response.status_code, response.content_type)
PY
```

Run local Spark smoke checks:

```bash
.venv/bin/python scripts/03_batch_analysis.py --master 'local[*]' --input data/earthquakes.csv --output-base output/batch
.venv/bin/python scripts/04_hotspot.py --master 'local[*]' --input data/earthquakes.csv --output-base output/hotspots
```

Run Hadoop + Spark verification:

```bash
start-dfs.sh
jps
hdfs dfs -ls /
bash scripts/02_upload_hdfs.sh
.venv/bin/python scripts/03_batch_analysis.py
.venv/bin/python scripts/04_hotspot.py
hdfs dfs -ls /earthquake/output
```

## Deploy Dashboard To Vercel

The Flask dashboard can deploy with the included `vercel.json`.

```bash
git add .
git commit -m "Prepare Vercel deployment"
git push origin main
vercel --prod
```

Notes:

- Vercel runs the dashboard only.
- Hadoop and Spark scripts are local/distributed pipeline commands, not Vercel serverless tasks.
- `/api/refresh-run` returns HTTP 501 on Vercel because serverless instances should not run local refresh scripts.
- Ensure `data/earthquakes.csv` exists before deployment.

Live App:

```text
https://earthquakewatch.vercel.app
```

## Troubleshooting

### HDFS connection refused

Symptom:

```text
Call From ... to localhost:9000 failed on connection exception: java.net.ConnectException: Connection refused
```

Fix:

```bash
start-dfs.sh
jps
hdfs dfs -ls /
```

If `NameNode` is missing from `jps`, Hadoop DFS is not running.

### `jps` command not found

Install a JDK and make sure Java tools are in `PATH`.

```bash
java -version
command -v jps
```

### Spark local Py4J socket error in restricted environments

PySpark opens a local Java gateway socket. If a sandbox blocks localhost sockets, Spark can fail with:

```text
Py4JNetworkException: Failed to bind to /127.0.0.1
```

Run Spark commands in a normal terminal or an environment that allows local socket binding.

### Missing CSV

Create the dataset:

```bash
.venv/bin/python scripts/01_download_data.py
```

Then verify:

```bash
ls -lh data/earthquakes.csv
```

### Missing speedup chart

Generate it:

```bash
.venv/bin/python scripts/07_amdahl.py
```

Then verify:

```bash
ls -lh output/speedup_chart.png
```

### Matplotlib cache warnings

If matplotlib cannot write to home-directory caches, use the project temp directory:

```bash
mkdir -p tmp/matplotlib
export MPLCONFIGDIR="$PWD/tmp/matplotlib"
```

## Screenshots

Screenshots are available in `docs/screenshots`.

- `docs/screenshots/dashboard-01-overview-v2.png`
- `docs/screenshots/dashboard-02-filters-map.png`
- `docs/screenshots/dashboard-03-charts-table.png`
- `docs/screenshots/dashboard-04-alerts-table.png`
- `docs/screenshots/dashboard-05-benchmark.png`
- `docs/screenshots/speedup_chart.png`

## License

This repository is intended for educational and demonstration use.
