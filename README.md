# GitHub -> Queue -> S3 Data Pipeline

A robust, 3-stage data pipeline to fetch GitHub data, buffer it in a queue, and archive/transform it into a structured database.

![Pipeline Architecture]
## Architecture

This pipeline follows a **Queue-First** architecture for robustness:

1.  **Ingest Stage** (`GitHub -> Queue`):
    *   Fetches Issues and PRs from GitHub API.
    *   Writes raw JSON directly to a local SQLite Queue (`queue_items` table).
    *   **Goal**: Fast, low-latency ingestion.

2.  **Worker Stage** (`Queue -> S3 + Issues DB`):
    *   Reads `pending` items from the Queue.
    *   **Archival**: Uploads the raw payload to S3 (Data Lake) partitioned by date.
    *   **Transformation**: Extracts key fields (ID, title, lead time, etc.) and upserts them into the SQLite `issues` table.

## Prerequisites

-   **Python 3.8+**
-   **AWS Credentials** (for S3 actions)
-   **GitHub Token** (optional, avoids rate limits)

## Setup

1.  **Environment Variables**:
    ```bash
    export GITHUB_TOKEN="your_pat"
    export AWS_ACCESS_KEY_ID="your_access_key"
    export AWS_SECRET_ACCESS_KEY="your_secret_key"
    export BUCKET_NAME="your-s3-bucket"
    ```

2.  **Dependencies**:
    We use a virtual environment:
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install requests boto3
    ```

## Usage

### Automated Run
Run the full end-to-end pipeline:
```bash
./run_pipeline.sh
```

### Manual Commands

**1. Ingest Data**
```bash
# Fetch data from a repo (defaults to last 7 days)
python main.py ingest --repo octocat/Hello-World
```

**2. Process Data**
```bash
# Process queue -> S3 & DB
python main.py worker
```

**3. Verify**
Check the database:
```bash
sqlite3 pipeline.db "SELECT id, title, state FROM issues LIMIT 5;"
```

## File Structure
-   `main.py`: CLI Entry point.
-   `fetcher.py`: GitHub API interaction layer.
-   `queue_manager.py`: Handles SQLite Queue operations.
-   `worker.py`: Orchestrates S3 upload and DB insertion.
-   `s3_writer.py`: S3 upload logic.
-   `db.py`: Database schema initialization.
