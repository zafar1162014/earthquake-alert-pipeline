#!/usr/bin/env bash

set -euo pipefail

LOCAL_FILE="data/earthquakes.csv"
HDFS_INPUT_DIR="/earthquake/input"
HDFS_OUTPUT_DIR="/earthquake/output"
HDFS_FILE_PATH="${HDFS_INPUT_DIR}/earthquakes.csv"


echo "Checking if Hadoop is running..."
if command -v jps >/dev/null 2>&1; then
  if jps | grep -q "NameNode"; then
    echo "✓ Hadoop is running"
  else
    echo "✗ Hadoop NameNode not found. Start it with: start-dfs.sh"
    exit 1
  fi
else
  echo "✗ 'jps' command not found. Make sure Java/JDK is installed."
  exit 1
fi

# Create HDFS directories
echo ""
echo "Creating HDFS directories..."
hdfs dfs -mkdir -p "${HDFS_INPUT_DIR}"
hdfs dfs -mkdir -p "${HDFS_OUTPUT_DIR}"
echo "✓ Directories created"

# Check if local CSV exists
echo ""
echo "Checking for local CSV file..."
if [[ ! -f "${LOCAL_FILE}" ]]; then
  echo "✗ File not found: ${LOCAL_FILE}"
  exit 1
fi
echo "✓ Found: ${LOCAL_FILE}"

# Upload to HDFS
echo ""
echo "Uploading to HDFS..."
hdfs dfs -put -f "${LOCAL_FILE}" "${HDFS_FILE_PATH}"
echo "✓ Upload complete"

# Verify the file was uploaded
echo ""
echo "Verifying upload..."
hdfs dfs -ls "${HDFS_INPUT_DIR}"
echo ""
hdfs dfs -du -h "${HDFS_INPUT_DIR}"
echo ""
echo "✓ All done!"
