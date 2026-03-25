from __future__ import annotations

from pathlib import Path

import pandas as pd
from flask import Flask, jsonify, render_template, send_file

# =========================
# Configuration
# =========================
BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / "data" / "earthquakes.csv"
OUTPUT_DIR = BASE_DIR / "output"
SPEEDUP_CHART_PATH = OUTPUT_DIR / "speedup_chart.png"

app = Flask(__name__)


def load_data() -> pd.DataFrame:
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV file not found: {CSV_PATH}")

    dataframe = pd.read_csv(CSV_PATH)

    required_columns = ["time", "latitude", "longitude", "depth", "mag", "place", "type", "region"]
    for column in required_columns:
        if column not in dataframe.columns:
            dataframe[column] = None

    dataframe["mag"] = pd.to_numeric(dataframe["mag"], errors="coerce")
    dataframe["latitude"] = pd.to_numeric(dataframe["latitude"], errors="coerce")
    dataframe["longitude"] = pd.to_numeric(dataframe["longitude"], errors="coerce")
    dataframe["depth"] = pd.to_numeric(dataframe["depth"], errors="coerce")
    dataframe["time"] = pd.to_datetime(dataframe["time"], errors="coerce", utc=True)

    return dataframe


def classify_alert(magnitude: float) -> str:
    if pd.isna(magnitude):
        return "LOW"
    if magnitude > 6.0:
        return "CRITICAL"
    if magnitude > 5.0:
        return "HIGH"
    if magnitude > 4.0:
        return "MEDIUM"
    return "LOW"


def magnitude_ranges(dataframe: pd.DataFrame) -> pd.Series:
    categories = pd.cut(
        dataframe["mag"],
        bins=[-float("inf"), 2, 4, 6, 8, float("inf")],
        labels=["Minor", "Light", "Moderate", "Strong", "Major"],
        right=False,
    )
    counts = categories.value_counts().reindex(["Minor", "Light", "Moderate", "Strong", "Major"], fill_value=0)
    return counts


def hotspot_table(dataframe: pd.DataFrame, top_n: int) -> list[dict]:
    hotspots = (
        dataframe.dropna(subset=["latitude", "longitude"])
        .assign(
            lat_grid=lambda d: d["latitude"].round(1),
            lon_grid=lambda d: d["longitude"].round(1),
        )
        .groupby(["lat_grid", "lon_grid"], as_index=False)
        .size()
        .rename(columns={"size": "count"})
        .sort_values("count", ascending=False)
        .head(top_n)
    )

    result = []
    for _, row in hotspots.iterrows():
        result.append(
            {
                "latitude": float(row["lat_grid"]),
                "longitude": float(row["lon_grid"]),
                "count": int(row["count"]),
            }
        )
    return result


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/summary")
def api_summary():
    try:
        dataframe = load_data()
        total_eq = int(len(dataframe))
        pakistan_eq = int((dataframe["region"] == "Pakistan").sum())
        critical_count = int((dataframe["mag"] > 6.0).sum())
        high_count = int(((dataframe["mag"] > 5.0) & (dataframe["mag"] <= 6.0)).sum())

        return jsonify(
            {
                "total_earthquakes": total_eq,
                "total_pakistan_earthquakes": pakistan_eq,
                "total_critical_alerts": critical_count,
                "total_high_alerts": high_count,
            }
        )
    except Exception as error:  # noqa: BLE001
        return jsonify({"error": str(error)}), 500


@app.route("/api/magnitude-chart")
def api_magnitude_chart():
    try:
        dataframe = load_data()
        counts = magnitude_ranges(dataframe)

        return jsonify(
            {
                "labels": ["Minor", "Light", "Moderate", "Strong", "Major"],
                "counts": [int(counts[label]) for label in ["Minor", "Light", "Moderate", "Strong", "Major"]],
            }
        )
    except Exception as error:  # noqa: BLE001
        return jsonify({"error": str(error)}), 500


@app.route("/api/pakistan-hotspots")
def api_pakistan_hotspots():
    try:
        dataframe = load_data()
        pakistan_df = dataframe[dataframe["region"] == "Pakistan"]
        return jsonify({"hotspots": hotspot_table(pakistan_df, top_n=10)})
    except Exception as error:  # noqa: BLE001
        return jsonify({"error": str(error)}), 500


@app.route("/api/global-hotspots")
def api_global_hotspots():
    try:
        dataframe = load_data()
        global_df = dataframe[dataframe["region"] != "Pakistan"]
        return jsonify({"hotspots": hotspot_table(global_df, top_n=10)})
    except Exception as error:  # noqa: BLE001
        return jsonify({"error": str(error)}), 500


@app.route("/api/recent-alerts")
def api_recent_alerts():
    try:
        dataframe = load_data()
        alerts = dataframe[dataframe["mag"] > 4.0].copy()
        alerts["alert_level"] = alerts["mag"].apply(classify_alert)
        alerts = alerts.sort_values("time", ascending=False).head(50)

        response = []
        for _, row in alerts.iterrows():
            response.append(
                {
                    "time": row["time"].isoformat() if pd.notna(row["time"]) else None,
                    "place": row["place"],
                    "mag": float(row["mag"]) if pd.notna(row["mag"]) else None,
                    "depth": float(row["depth"]) if pd.notna(row["depth"]) else None,
                    "region": row["region"],
                    "alert_level": row["alert_level"],
                }
            )

        return jsonify({"alerts": response})
    except Exception as error:  # noqa: BLE001
        return jsonify({"error": str(error)}), 500


@app.route("/api/speedup-chart")
def api_speedup_chart():
    if not SPEEDUP_CHART_PATH.exists():
        return jsonify({"error": f"Speedup chart not found: {SPEEDUP_CHART_PATH}"}), 404
    return send_file(SPEEDUP_CHART_PATH, mimetype="image/png")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
