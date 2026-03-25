# EarthquakeWatch

A comprehensive earthquake monitoring and alert system leveraging **Hadoop** and **Apache Spark** for big data processing, with a focus on **Pakistan and global seismic activity**.

EarthquakeWatch is a full-stack data engineering project that demonstrates real-time data ingestion from USGS feeds, distributed processing with Hadoop and Spark, stream processing for alerts, hotspot analysis, and performance benchmarking. The project includes an interactive web dashboard with 3D globe visualization, live maps, real-time charts, and alert management for monitoring global and regional earthquake activity.

---

## Project Structure

```
earthquake_alert_pipeline/
├── app.py                      # Flask backend API server
├── requirements.txt            # Python dependencies
├── README.md                   # Documentation
├── .gitignore                  # Git ignore rules
├── data/
│   └── earthquakes.csv         # Earthquake dataset
├── output/                     # Results and charts
├── scripts/
│   ├── 01_download_data.py     # Fetch USGS earthquake data
│   ├── 02_upload_hdfs.sh       # Upload to Hadoop HDFS
│   ├── 03_batch_analysis.py    # Spark batch analytics
│   ├── 04_hotspot.py           # Hotspot detection
│   ├── 05_stream_feed.py       # Socket-based data simulation
│   ├── 06_stream_alert.py      # Spark Streaming alerts
│   └── 07_amdahl.py            # Performance benchmark
└── templates/
    └── index.html              # Interactive web dashboard
```

---

## Technologies Used

| Technology | Purpose |
|-----------|---------|
| **Python 3.8+** | Core programming language |
| **Flask** | Web framework for API backend |
| **Pandas** | Data manipulation and analysis |
| **PySpark** | Distributed batch and stream processing |
| **Hadoop HDFS** | Distributed file storage |
| **Requests** | HTTP client for USGS API |
| **Matplotlib** | Chart and visualization generation |
| **Three.js** | 3D globe visualization (frontend) |
| **Leaflet.js** | Interactive mapping (frontend) |
| **Chart.js** | Data charts (frontend) |
| **Bootstrap 5** | UI framework (frontend) |

---

## How To Run

### Prerequisites
- Python 3.8 or higher
- pip package manager
- Virtual environment (recommended)

### Step 1: Clone and Setup
```bash
git clone https://github.com/zafar1162014/earthquake-alert-pipeline.git
cd earthquake-alert-pipeline
```

### Step 2: Create Virtual Environment
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

### Step 3: Install Dependencies
```bash
pip install -r requirements.txt
```

### Step 4: Download Earthquake Data
```bash
python scripts/01_download_data.py
```

### Step 5: Start Flask Dashboard
```bash
python app.py
```

### Step 6: Open in Browser
Navigate to **http://localhost:5000** to view the interactive dashboard.

---

## API Endpoints

| Method | Route | Description |
|--------|-------|-------------|
| **GET** | `/` | Serve index.html dashboard |
| **GET** | `/api/summary` | Returns total, Pakistan, critical, high counts and magnitude stats |
| **GET** | `/api/earthquakes` | Returns all earthquake rows with alert level classification |
| **GET** | `/api/hotspots` | Returns top 20 hotspot grid cells by earthquake count |
| **GET** | `/api/pakistan` | Returns Pakistan-only earthquake rows sorted by time |
| **GET** | `/api/recent` | Returns last 100 earthquake rows in descending time order |
| **GET** | `/api/speedup` | Returns Amdahl's Law speedup chart image (PNG) |

---

## GitHub

Repository: https://github.com/zafar1162014/earthquake-alert-pipeline
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

| File                       | Description                                      |
| -------------------------- | ------------------------------------------------ |
| `data/earthquakes.csv`     | Processed earthquake records (USGS source)       |
| `output/speedup_chart.png` | Performance benchmark visualization              |
| `output/*.parquet`         | Structured Streaming results (if running Task 6) |
| HDFS `/earthquake/output/` | Distributed analytics results (if using Hadoop)  |

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
