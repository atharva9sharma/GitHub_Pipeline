import sqlite3
import json
import os
from datetime import datetime
import db
from s3_writer import S3Writer

BUCKET_NAME = os.getenv("BUCKET_NAME", "my-data-lake-bucket")

class Worker:
    def __init__(self):
        self.conn = db.get_db_connection()
        self.s3_writer = S3Writer(bucket_name=BUCKET_NAME)

    def process_pending_items(self):
        c = self.conn.cursor()
        
        # Fetch pending items
        rows = c.execute("SELECT id, payload, created_at FROM queue_items WHERE status = 'pending'").fetchall()
        
        print(f"Found {len(rows)} pending items.")
        
        for row in rows:
            queue_id = row['id']
            try:
                payload = json.loads(row['payload'])
                
                # Step 1: Archival (S3)
                self.s3_writer.upload(payload, "github_issues", row['created_at'])
                
                # Step 2: Transform & Load (SQLite Issues)
                issue_record = self._transform(payload)
                
                # Upsert into issues
                c.execute('''
                    INSERT INTO issues (
                        id, number, title, state, user_login, 
                        is_pull_request, created_at, closed_at, 
                        lead_time_hours, raw_payload
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        title=excluded.title,
                        state=excluded.state,
                        closed_at=excluded.closed_at,
                        lead_time_hours=excluded.lead_time_hours
                ''', issue_record)
                
                # Mark queue item as processed
                c.execute("UPDATE queue_items SET status = 'processed' WHERE id = ?", (queue_id,))
                
            except Exception as e:
                print(f"Error processing queue item {queue_id}: {e}")
                c.execute("UPDATE queue_items SET status = 'failed' WHERE id = ?", (queue_id,))
        
        self.conn.commit()
        print("Worker run complete.")

    def _transform(self, data):
        gh_id = data.get('id')
        number = data.get('number')
        title = data.get('title')
        state = data.get('state')
        user = data.get('user', {})
        user_login = user.get('login') if user else None
        
        is_pr = 1 if 'pull_request' in data else 0
        
        created_at = data.get('created_at')
        closed_at = data.get('closed_at')
        
        lead_time = None
        if closed_at and created_at:
            try:
                fmt = "%Y-%m-%dT%H:%M:%SZ"
                dt_created = datetime.strptime(created_at, fmt)
                dt_closed = datetime.strptime(closed_at, fmt)
                
                diff = dt_closed - dt_created
                lead_time = diff.total_seconds() / 3600.0
            except ValueError:
                pass
        
        return (
            gh_id, number, title, state, user_login, 
            is_pr, created_at, closed_at, lead_time, 
            json.dumps(data)
        )
