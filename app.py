from pathlib import Path
import os
import subprocess
import sys

import pandas as pd
from flask import Flask, jsonify, render_template, request, send_file

# Flask app configuration
BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / "data" / "earthquakes.csv"
OUTPUT_DIR = BASE_DIR / "output"
SPEEDUP_CHART_PATH = OUTPUT_DIR / "speedup_chart.png"
SCRIPTS_DIR = BASE_DIR / "scripts"
APP_NAME = "EarthquakeWatch"

app = Flask(__name__)


def load_earthquakes():
    # Load CSV file with all required columns
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"CSV not found: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH)

    # Ensure all expected columns exist
    required_cols = ["time", "latitude", "longitude", "depth", "mag", "place", "type", "region"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    # Convert to numeric types and parse dates
    df["mag"] = pd.to_numeric(df["mag"], errors="coerce")
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["depth"] = pd.to_numeric(df["depth"], errors="coerce")
    df["time"] = pd.to_datetime(df["time"], errors="coerce", utc=True)

    return df


def get_alert_level(magnitude):
    # Classify earthquakes by magnitude threshold
    if pd.isna(magnitude):
        return "LOW"
    if magnitude > 6.0:
        return "CRITICAL"
    if magnitude > 5.0:
        return "HIGH"
    if magnitude > 4.0:
        return "MEDIUM"
    return "LOW"


def serialize_earthquake_row(row):
    # Convert a DataFrame row to JSON format with alert level
    return {
        "time": row["time"].isoformat() if pd.notna(row["time"]) else None,
        "latitude": float(row["latitude"]) if pd.notna(row["latitude"]) else None,
        "longitude": float(row["longitude"]) if pd.notna(row["longitude"]) else None,
        "depth": float(row["depth"]) if pd.notna(row["depth"]) else None,
        "mag": float(row["mag"]) if pd.notna(row["mag"]) else None,
        "place": str(row["place"]) if pd.notna(row["place"]) else None,
        "region": str(row["region"]) if pd.notna(row["region"]) else None,
        "alert_level": get_alert_level(row["mag"]),
    }


def run_script(script_name):
    # Run a local script and return basic execution details
    script_path = SCRIPTS_DIR / script_name
    if not script_path.exists():
        return {
            "script": script_name,
            "success": False,
            "return_code": -1,
            "output": f"Script not found: {script_path}",
        }

    completed = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(BASE_DIR),
        capture_output=True,
        text=True,
        check=False,
    )

    log_output = (completed.stdout or "") + ("\n" + completed.stderr if completed.stderr else "")
    return {
        "script": script_name,
        "success": completed.returncode == 0,
        "return_code": completed.returncode,
        "output": log_output.strip(),
    }


@app.route("/")
def serve_dashboard():
    # Serve the main dashboard HTML page
    return render_template("index.html")


@app.route("/api/summary")
def api_summary():
    # Return summary statistics: total count, Pakistan count, critical/high alerts
    try:
        df = load_earthquakes()
        return jsonify({
            "app_name": APP_NAME,
            "total_earthquakes": int(len(df)),
            "pakistan_count": int((df["region"] == "Pakistan").sum()),
            "critical_count": int((df["mag"] > 6.0).sum()),
            "high_count": int(((df["mag"] > 5.0) & (df["mag"] <= 6.0)).sum()),
            "average_magnitude": round(float(df["mag"].mean()) if not df["mag"].dropna().empty else 0.0, 2),
            "max_magnitude": round(float(df["mag"].max()) if not df["mag"].dropna().empty else 0.0, 2),
        })
    except FileNotFoundError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"Error loading summary: {str(e)}"}), 500


@app.route("/api/earthquakes")
def api_earthquakes():
    # Return all earthquakes with alert level classifications
    try:
        df = load_earthquakes()
        earthquakes = [serialize_earthquake_row(row) for _, row in df.iterrows()]
        return jsonify({"earthquakes": earthquakes})
    except FileNotFoundError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"Error loading earthquakes: {str(e)}"}), 500


@app.route("/api/hotspots")
def api_hotspots():
    # Return top 20 hotspot grid cells (latitude/longitude bins)
    try:
        df = load_earthquakes()
        
        # Group by rounded coordinates to find hotspots
        hotspots_df = (
            df.dropna(subset=["latitude", "longitude"])
            .assign(lat_grid=lambda d: d["latitude"].round(1), lon_grid=lambda d: d["longitude"].round(1))
            .groupby(["lat_grid", "lon_grid"], as_index=False)
            .size()
            .rename(columns={"size": "count"})
            .sort_values("count", ascending=False)
            .head(20)
        )
        
        hotspots = [
            {"latitude": float(row["lat_grid"]), "longitude": float(row["lon_grid"]), "count": int(row["count"])}
            for _, row in hotspots_df.iterrows()
        ]
        return jsonify({"hotspots": hotspots})
    except FileNotFoundError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"Error loading hotspots: {str(e)}"}), 500


@app.route("/api/pakistan")
def api_pakistan():
    # Return earthquakes from Pakistan region only, sorted by time descending
    try:
        df = load_earthquakes()
        pakistan_df = df[df["region"] == "Pakistan"].copy()
        pakistan_df = pakistan_df.sort_values("time", ascending=False)
        earthquakes = [serialize_earthquake_row(row) for _, row in pakistan_df.iterrows()]
        return jsonify({"earthquakes": earthquakes})
    except FileNotFoundError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"Error loading Pakistan data: {str(e)}"}), 500


@app.route("/api/recent")
def api_recent():
    # Return the 100 most recent earthquake records
    try:
        df = load_earthquakes()
        recent_df = df.sort_values("time", ascending=False).head(100)
        earthquakes = [serialize_earthquake_row(row) for _, row in recent_df.iterrows()]
        return jsonify({"earthquakes": earthquakes})
    except FileNotFoundError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"Error loading recent data: {str(e)}"}), 500


@app.route("/api/speedup")
def api_speedup():
    # Return the Amdahl's Law speedup benchmark chart image
    if not SPEEDUP_CHART_PATH.exists():
        return jsonify({"error": f"Chart not found: {SPEEDUP_CHART_PATH}"}), 404
    return send_file(SPEEDUP_CHART_PATH, mimetype="image/png")


@app.route("/api/refresh-run", methods=["POST"])
def api_refresh_run():
    # Run refresh scripts and return execution status
    try:
        # Serverless instances should not run local refresh scripts.
        if os.environ.get("VERCEL"):
            return jsonify({
                "success": False,
                "error": "Refresh is not available on Vercel. Run scripts locally instead.",
            }), 501

        payload = request.get_json(silent=True) or {}
        include_benchmark = bool(payload.get("includeBenchmark", False))

        scripts_to_run = ["01_download_data.py"]
        if include_benchmark:
            scripts_to_run.append("07_amdahl.py")

        results = [run_script(script_name) for script_name in scripts_to_run]
        all_success = all(item["success"] for item in results)

        return jsonify(
            {
                "success": all_success,
                "message": "Refresh completed" if all_success else "Refresh could not complete. Check script logs locally.",
                "results": results,
            }
        ), (200 if all_success else 500)
    except Exception as e:
        return jsonify({"success": False, "error": f"Error running refresh: {str(e)}"}), 500


if __name__ == "__main__":
    print(f"\n{'='*60}")
    print(f"  Starting {APP_NAME}")
    print(f"{'='*60}")
    print(f"\nServer running at: http://localhost:5001")
    print(f"Dashboard: http://localhost:5001/\n")
    app.run(host="0.0.0.0", port=5001, debug=True)
