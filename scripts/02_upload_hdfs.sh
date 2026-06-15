#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LOCAL_FILE="${PROJECT_ROOT}/data/earthquakes.csv"
HDFS_INPUT_DIR="/earthquake/input"
HDFS_OUTPUT_DIR="/earthquake/output"
HDFS_FILE_PATH="${HDFS_INPUT_DIR}/earthquakes.csv"

require_command() {
  local command_name="$1"
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "✗ '${command_name}' command not found."
    return 1
  fi
}

print_hadoop_start_help() {
  cat <<'EOF'

Hadoop HDFS is not reachable.

Start Hadoop DFS, then rerun this script:

  start-dfs.sh
  jps
  hdfs dfs -ls /

Expected jps processes include NameNode and DataNode.
EOF
}

echo "Checking if Hadoop is running..."
require_command jps || {
  echo "Make sure Java/JDK is installed and available in PATH."
  exit 1
}
require_command hdfs || {
  echo "Install Hadoop and make sure hdfs is available in PATH."
  exit 1
}

DEFAULT_FS="$(hdfs getconf -confKey fs.defaultFS 2>/dev/null || true)"
echo "Hadoop fs.defaultFS: ${DEFAULT_FS:-unknown}"

if jps | grep -q "NameNode"; then
  echo "✓ NameNode process found"
else
  echo "✗ Hadoop NameNode process not found."
  print_hadoop_start_help
  exit 1
fi

if hdfs dfs -ls / >/dev/null 2>&1; then
  echo "✓ HDFS root is reachable"
else
  echo "✗ HDFS root is not reachable."
  print_hadoop_start_help
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
    echo "Create it with: python3 scripts/01_download_data.py"
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
