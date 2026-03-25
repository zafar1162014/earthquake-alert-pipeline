from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pandas as pd
from flask import Flask, jsonify, render_template, request, send_file

# =============================
# EarthquakeWatch Configuration
# =============================
BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / "data" / "earthquakes.csv"
OUTPUT_DIR = BASE_DIR / "output"
SPEEDUP_CHART_PATH = OUTPUT_DIR / "speedup_chart.png"
APP_NAME = "EarthquakeWatch"
SCRIPTS_DIR = BASE_DIR / "scripts"

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


def serialize_earthquakes(dataframe: pd.DataFrame) -> list[dict]:
    response: list[dict] = []
    for _, row in dataframe.iterrows():
        response.append(
            {
                "time": row["time"].isoformat() if pd.notna(row["time"]) else None,
                "latitude": float(row["latitude"]) if pd.notna(row["latitude"]) else None,
                "longitude": float(row["longitude"]) if pd.notna(row["longitude"]) else None,
                "depth": float(row["depth"]) if pd.notna(row["depth"]) else None,
                "mag": float(row["mag"]) if pd.notna(row["mag"]) else None,
                "place": row["place"] if pd.notna(row["place"]) else None,
                "region": row["region"] if pd.notna(row["region"]) else None,
                "alert_level": classify_alert(row["mag"]),
            }
        )
    return response


def run_python_script(script_name: str, timeout_seconds: int = 300) -> dict:
    script_path = SCRIPTS_DIR / script_name
    if not script_path.exists():
        return {
            "script": script_name,
            "success": False,
            "return_code": -1,
            "output": f"Script not found: {script_path}",
        }

    try:
        completed = subprocess.run(
            [sys.executable, str(script_path)],
            cwd=str(BASE_DIR),
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
        output = (completed.stdout or "") + ("\n" + completed.stderr if completed.stderr else "")
        return {
            "script": script_name,
            "success": completed.returncode == 0,
            "return_code": completed.returncode,
            "output": output.strip(),
        }
    except subprocess.TimeoutExpired:
        return {
            "script": script_name,
            "success": False,
            "return_code": -2,
            "output": f"Timed out after {timeout_seconds}s",
        }
    except Exception as error:  # noqa: BLE001
        return {
            "script": script_name,
            "success": False,
            "return_code": -3,
            "output": str(error),
        }


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
        avg_mag = float(dataframe["mag"].mean()) if not dataframe["mag"].dropna().empty else 0.0
        max_mag = float(dataframe["mag"].max()) if not dataframe["mag"].dropna().empty else 0.0

        return jsonify(
            {
                "app_name": APP_NAME,
                "total_earthquakes": total_eq,
                "pakistan_count": pakistan_eq,
                "critical_count": critical_count,
                "high_count": high_count,
                "average_magnitude": round(avg_mag, 2),
                "max_magnitude": round(max_mag, 2),
            }
        )
    except Exception as error:  # noqa: BLE001
        return jsonify({"error": str(error)}), 500


@app.route("/api/earthquakes")
def api_earthquakes():
    try:
        dataframe = load_data()
        return jsonify({"earthquakes": serialize_earthquakes(dataframe)})
    except Exception as error:  # noqa: BLE001
        return jsonify({"error": str(error)}), 500


@app.route("/api/hotspots")
def api_hotspots():
    try:
        dataframe = load_data()
        return jsonify({"hotspots": hotspot_table(dataframe, top_n=20)})
    except Exception as error:  # noqa: BLE001
        return jsonify({"error": str(error)}), 500


@app.route("/api/pakistan")
def api_pakistan():
    try:
        dataframe = load_data()
        pakistan_df = dataframe[dataframe["region"] == "Pakistan"].copy()
        pakistan_df = pakistan_df.sort_values("time", ascending=False)
        return jsonify({"earthquakes": serialize_earthquakes(pakistan_df)})
    except Exception as error:  # noqa: BLE001
        return jsonify({"error": str(error)}), 500


@app.route("/api/recent")
def api_recent():
    try:
        dataframe = load_data()
        recent_df = dataframe.sort_values("time", ascending=False).head(100)
        return jsonify({"earthquakes": serialize_earthquakes(recent_df)})
    except Exception as error:  # noqa: BLE001
        return jsonify({"error": str(error)}), 500


@app.route("/api/speedup")
def api_speedup():
    if not SPEEDUP_CHART_PATH.exists():
        return jsonify({"error": f"Speedup chart not found: {SPEEDUP_CHART_PATH}"}), 404
    return send_file(SPEEDUP_CHART_PATH, mimetype="image/png")


@app.route("/api/refresh-run", methods=["POST"])
def api_refresh_run():
    try:
        payload = request.get_json(silent=True) or {}
        include_benchmark = bool(payload.get("includeBenchmark", False))

        scripts_to_run = ["01_download_data.py"]
        if include_benchmark:
            scripts_to_run.append("07_amdahl.py")

        results = [run_python_script(script_name) for script_name in scripts_to_run]
        all_success = all(result["success"] for result in results)

        return jsonify(
            {
                "success": all_success,
                "message": "Refresh run completed" if all_success else "Refresh run finished with errors",
                "results": results,
            }
        ), (200 if all_success else 500)
    except Exception as error:  # noqa: BLE001
        return jsonify({"success": False, "error": str(error)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
