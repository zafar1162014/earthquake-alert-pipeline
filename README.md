# 🌍 EarthquakeWatch: Real-Time Disaster Alert Pipeline

> A comprehensive earthquake monitoring and alert system leveraging **Hadoop** and **Apache Spark** for big data processing, with a focus on **Pakistan and global seismic activity**.

---

## ✨ Overview

EarthquakeWatch is a full-stack data engineering project that demonstrates:

- **Data Ingestion**: Fetch real-time earthquake data from USGS feeds
- **Distributed Processing**: Hadoop-based storage and Spark-powered analytics
- **Stream Processing**: Real-time alert classification using Spark Structured Streaming
- **Hotspot Analysis**: Identify high-risk earthquake zones globally and regionally
- **Performance Benchmarking**: Validate multi-core speedup against Amdahl's Law
- **Interactive Dashboard**: Beautiful, real-time web UI for monitoring and visualization

Perfect for learning **big data pipelines**, **distributed computing**, and **real-world data engineering practices**.

---

## 🚀 Quick Start

### Prerequisites

- **Python 3.8+** (tested on 3.12)
- **Apache Spark** (optional for streaming/batch tasks)
- **Hadoop** (optional for HDFS operations)
- **Git**

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/zafar1162014/earthquake-alert-pipeline.git
   cd earthquake-alert-pipeline
   ```

2. **Create a Python virtual environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

---

## 📋 Project Structure

| Component | Purpose |
|-----------|---------|
| **01_download_data.py** | Fetch earthquake data from USGS (global + Pakistan) and prepare CSV |
| **02_upload_hdfs.sh** | Upload processed CSV to Hadoop HDFS |
| **03_batch_analysis.py** | Run Spark batch jobs for analytics (magnitude, regions, hotspots) |
| **04_hotspot.py** | Detect high-risk earthquake zones with severity classification |
| **05_stream_feed.py** | Simulate live earthquake data feed via socket |
| **06_stream_alert.py** | Spark Structured Streaming for real-time alert classification |
| **07_amdahl.py** | Multi-core performance benchmark with speedup visualization |
| **app.py** | Flask backend API server for dashboard |
| **templates/index.html** | Interactive web dashboard with maps, charts, and alerts |

---

## 🛠️ How to Run

### Option A: Dashboard Only (Recommended for First-Time Users) ⭐

If you just want to see the beautiful dashboard and explore the data:

```bash
# 1. Download earthquake data (required once)
python scripts/01_download_data.py

# 2. Start the Flask dashboard
python app.py
```

Then open **http://localhost:5001** in your browser. You'll see:
- 📍 Interactive live map with earthquake markers
- 📊 Real-time charts and statistics
- 🔔 Alert severity breakdown
- 🎯 Regional and magnitude filtering
- ⚡ "Run & Update" button to refresh data

---

### Option B: Full Pipeline (With Spark & Hadoop)

To run all 7 tasks end-to-end:

```bash
# Task 1: Download and prepare data
python scripts/01_download_data.py
# Output: data/earthquakes.csv

# Task 2: Upload to HDFS (optional, requires Hadoop)
bash scripts/02_upload_hdfs.sh

# Task 3: Batch analysis
spark-submit scripts/03_batch_analysis.py

# Task 4: Hotspot detection
spark-submit scripts/04_hotspot.py

# Task 5 & 6: Streaming pipeline (needs 2 terminals)
# Terminal 1:
python scripts/05_stream_feed.py

# Terminal 2:
spark-submit scripts/06_stream_alert.py

# Task 7: Benchmark and performance testing
python scripts/07_amdahl.py
```

---

## 📊 Dashboard Features

### Live Map
Interactive Leaflet map showing earthquake epicenters with:
- Color-coded severity (critical 🔴, high 🟠, medium 🟡, low 🟢)
- Click for detailed earthquake information
- Zoom and pan controls

### Real-Time Alerts Table
- Searchable table of recent earthquakes
- Sortable columns (time, magnitude, depth, region)
- Alert severity badges
- Pakistan-specific highlighting

### Analytics Charts
- **Magnitude Distribution**: Histogram of earthquake magnitudes
- **Alert Level Breakdown**: Pie chart of severity levels
- **Earthquake Frequency**: Time-series of events (last 30 days)

### Filters & Controls
- **Data View**: All data, recent 100, Pakistan only
- **Region Select**: Filter by country/region
- **Time Range**: Last 24 hours, 7 days, 30 days, or all time
- **Alert Level**: Critical, High, Medium, Low, or mixed
- **Minimum Magnitude**: Slider to adjust detection threshold

### Performance Benchmark
- Amdahl's Law validation chart
- Compares actual Spark speedup vs. theoretical prediction
- Shows multi-core efficiency

---

## 📁 Data & Outputs

| File | Description |
|------|-------------|
| `data/earthquakes.csv` | Processed earthquake records (USGS source) |
| `output/speedup_chart.png` | Performance benchmark visualization |
| `output/*.parquet` | Structured Streaming results (if running Task 6) |
| HDFS `/earthquake/output/` | Distributed analytics results (if using Hadoop) |

---

## 🔧 Configuration

Edit these files to customize behavior:

- **app.py**: Flask routes, port settings, API endpoints
- **templates/index.html**: Dashboard UI, map styles, chart options
- **scripts/**: Each script has configurable parameters (magnitude thresholds, regions, etc.)

### API Endpoints

The Flask backend provides these endpoints:

```
GET  /                      → Dashboard homepage
GET  /api/summary           → Quick statistics
GET  /api/earthquakes       → All earthquake data
GET  /api/pakistan          → Pakistan-only earthquakes
GET  /api/recent            → Last 100 events
GET  /api/hotspots          → Hotspot analysis
GET  /api/speedup           → Benchmark chart image
POST /api/refresh-run       → Run scripts and refresh data
```

---

## 📈 Key Technologies

- **Apache Spark**: Distributed data processing (batch + streaming)
- **Hadoop HDFS**: Distributed file storage
- **Flask**: Lightweight Python web framework
- **Pandas**: Data manipulation and analysis
- **Leaflet**: Interactive map library
- **Chart.js**: Data visualization
- **Bootstrap 5**: Responsive UI framework
- **USGS API**: Real-time earthquake data source

---

## 🤝 Contributing

We'd love your contributions! Here's how:

1. **Fork** the repository
2. **Create a feature branch**: `git checkout -b feature/awesome-feature`
3. **Commit changes**: `git commit -m "Add awesome feature"`
4. **Push to branch**: `git push origin feature/awesome-feature`
5. **Open a Pull Request**

### Improvement Ideas
- Add email/SMS notifications for critical events
- Implement machine learning for magnitude prediction
- Add historical trend analysis and forecasting
- Support for multiple countries/regions expansion
- Mobile app version (React Native/Flutter)
- Database integration for persistent storage
- Kubernetes deployment templates

---

## 📝 License

This project is open source and available under the **MIT License**.

---

## 🙋 Support & Questions

- **Issues?** Check the [GitHub Issues](https://github.com/zafar1162014/earthquake-alert-pipeline/issues) page
- **Documentation**: Review individual script comments for detailed info
- **Spark/Hadoop**: Ensure they're installed for running the full pipeline
- **Port conflict**: Dashboard runs on `http://localhost:5001` (macOS issue with port 5000)
- **Data source**: Uses live USGS earthquake feed API

---

## 🎓 Learning Resources

This project demonstrates real-world concepts from parallel and distributed computing:

- **Amdahl's Law**: Understanding parallelization limits
- **Hadoop MapReduce**: Distributed batch processing fundamentals
- **Apache Spark**: In-memory distributed computing paradigms
- **Stream Processing**: Real-time data pipeline architectures
- **Big Data Stack**: Designing scalable, production-ready systems

Perfect for:
- 📚 **Educational purposes** (PDC/Big Data coursework)
- 💼 **Portfolio projects** (impress interviewers)
- 🔍 **Exploring distributed systems** (hands-on learning)
- 🏗️ **Building data pipelines** (production patterns)

---

## 📸 Screenshots

The dashboard includes:
- Beautiful light-themed UI with interactive maps
- Real-time earthquake markers with color-coded severity
- Rich analytics with multiple chart types
- Responsive design for desktop and tablet
- Fast search and filtering capabilities

---

## 🚀 Getting Help

If you're new to this project:
1. Start with **Option A** (dashboard only)
2. Explore the live data and visualizations
3. Read through the [USGS API docs](https://earthquake.usgs.gov/earthquakes/feed/)
4. Check script comments for implementation details
5. Gradually work through Option B if interested in Spark/Hadoop

Happy monitoring! 🌍🔔
