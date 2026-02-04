#!/bin/bash

# Configuration
REPO="octocat/Hello-World" # Change this to your desired repo
export BUCKET_NAME="gh-pipeline-hivel"
export AWS_ACCESS_KEY_ID="AKIASBHQGJ77T5MTYUMY"
export AWS_SECRET_ACCESS_KEY="hd7Qi9Szslwg7L9jssJ6fWDHWTIu/dKT6ptwZt9Y"
export AWS_DEFAULT_REGION="us-east-1" # Defaulting to us-east-1, common default.

# Optional: Set dates
# SINCE="2023-01-01"
# UNTIL="2023-01-31"

echo "========================================"
echo "Starting Pipeline for $REPO"
echo "========================================"

# 1. Ingest
echo "[1/3] Running Ingest (GitHub -> S3)..."
# Add --since/--until if set
if [ -n "$SINCE" ]; then
    ./venv/bin/python3 main.py ingest --repo "$REPO" --since "$SINCE" --until "$UNTIL"
else
    ./venv/bin/python3 main.py ingest --repo "$REPO"
fi

if [ $? -ne 0 ]; then
    echo "Ingest failed!"
    exit 1
fi

# 2. Worker (Queue -> S3 + SQLite Issues)
echo "----------------------------------------"
echo "[2/2] Running Worker (Queue -> S3 + Issues)... from Queue"
./venv/bin/python3 main.py worker
if [ $? -ne 0 ]; then
    echo "Worker failed!"
    exit 1
fi

echo "========================================"
echo "Pipeline Completed Successfully!"
echo "========================================"
echo "Verification:"
echo "Run: sqlite3 pipeline.db 'SELECT id, number, title FROM issues LIMIT 5;'"
