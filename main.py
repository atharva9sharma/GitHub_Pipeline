import argparse
import sys
import os
from fetcher import DataFetcher
from s3_writer import S3Writer
from queue_manager import QueueManager
from worker import Worker

BUCKET_NAME = os.getenv("BUCKET_NAME", "my-data-lake-bucket")

import datetime

def ingest(args):
    print(f"Starting ingest for {args.repo}...")
    
    # helper to format date to ISO 8601 for GitHub
    def to_iso(date_str):
        if not date_str: return None
        # If length is 10 (YYYY-MM-DD), append time
        if len(date_str) == 10:
            return f"{date_str}T00:00:00Z"
        return date_str

    since = args.since
    until = args.until
    
    # Default to last 7 days if since is not provided
    if not since:
        since_dt = datetime.datetime.now() - datetime.timedelta(days=7)
        since = since_dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        print(f"No --since provided. Defaulting to last 7 days: {since}")
    else:
        since = to_iso(since)
        
    until = to_iso(until)
    
    # 1. Fetch Data
    fetcher = DataFetcher()
    
    print(f"Fetching issues since {since}...")
    issues = fetcher.fetch_items(args.repo, "issues", since=since, until=until)
    print(f"Fetched {len(issues)} issues.")
    
    # 2. Enqueue directly
    qm = QueueManager()
    
    count = 0
    for item in issues:
        if qm.enqueue_item(item):
            count += 1
            
    print(f"Ingest complete. Enqueued {count} new items (skipped duplicates).")

def enqueue(args):
    print("Enqueue command is deprecated in Queue-First architecture. Ingest now enqueues directly.")


def worker(args):
    print("Starting worker...")
    w = Worker()
    w.process_pending_items()


def main():
    parser = argparse.ArgumentParser(description="GitHub -> S3 -> SQLite Pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Ingest Command
    parser_ingest = subparsers.add_parser("ingest", help="Pull from GitHub to S3")
    parser_ingest.add_argument("--repo", required=True, help="GitHub repo (owner/name)")
    parser_ingest.add_argument("--since", help="Start date (YYYY-MM-DD)")
    parser_ingest.add_argument("--until", help="End date (YYYY-MM-DD)")
    parser_ingest.set_defaults(func=ingest)

    # Enqueue Command
    parser_enqueue = subparsers.add_parser("enqueue", help="S3 -> SQLite Queue")
    parser_enqueue.set_defaults(func=enqueue)

    # Worker Command
    parser_worker = subparsers.add_parser("worker", help="Process Queue -> Issues Table")
    parser_worker.set_defaults(func=worker)

    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()