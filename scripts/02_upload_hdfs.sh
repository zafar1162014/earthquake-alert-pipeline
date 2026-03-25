#!/usr/bin/env bash

set -euo pipefail

LOCAL_FILE="data/earthquakes.csv"
HDFS_INPUT_DIR="/earthquake/input"
HDFS_OUTPUT_DIR="/earthquake/output"
HDFS_FILE_PATH="${HDFS_INPUT_DIR}/earthquakes.csv"


echo "Step 1: Checking Hadoop services..."
if command -v jps >/dev/null 2>&1; then
  if jps | grep -q "NameNode"; then
    echo "Hadoop is running (NameNode detected)"
  else
    echo "Hadoop does not appear to be running (NameNode not detected)."
    echo "Please start Hadoop first (e.g., start-dfs.sh and start-yarn.sh), then rerun this script."
    exit 1
  fi
else
  echo "Error: 'jps' command not found. Make sure Java/JDK is installed and JAVA_HOME is configured."
  exit 1
fi

echo
echo "Step 2: Creating HDFS directories..."
hdfs dfs -mkdir -p "${HDFS_INPUT_DIR}"
echo "Created: ${HDFS_INPUT_DIR}"
hdfs dfs -mkdir -p "${HDFS_OUTPUT_DIR}"
echo "Created: ${HDFS_OUTPUT_DIR}"

echo
echo "Step 3: Checking local CSV file..."
if [[ ! -f "${LOCAL_FILE}" ]]; then
  echo "Error: Local file not found: ${LOCAL_FILE}"
  exit 1
fi
echo "Found: ${LOCAL_FILE}"

echo
echo "Step 4: Uploading to HDFS..."
hdfs dfs -put -f "${LOCAL_FILE}" "${HDFS_FILE_PATH}"
echo "Upload complete."

echo
echo "Step 5: Verifying upload..."
hdfs dfs -ls "${HDFS_INPUT_DIR}"
echo "File size details:"
hdfs dfs -du -h "${HDFS_INPUT_DIR}"

echo
echo "Step 6: Success!"
echo "File available at: ${HDFS_FILE_PATH}"
