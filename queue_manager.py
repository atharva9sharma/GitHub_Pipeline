import boto3
import json
import sqlite3
import db

class QueueManager:
    def __init__(self, bucket_name=None, prefix=""):
        self.bucket = bucket_name # Optional now for ingest
        self.prefix = prefix
        self.s3 = boto3.client('s3')
        # Ensure DB is init
        db.init_db()

    def enqueue_item(self, data):
        """Directly enqueue a single item."""
        conn = db.get_db_connection()
        c = conn.cursor()
        
        item_id = str(data.get('id'))
        content = json.dumps(data)
        created_at = data.get('created_at')
        
        try:
            c.execute(
                "INSERT OR IGNORE INTO queue_items (external_id, item_type, payload, status, created_at) VALUES (?, ?, ?, ?, ?)",
                (item_id, "github_issue", content, "pending", created_at)
            )
            inserted = c.rowcount > 0
            conn.commit()
            return inserted
        except Exception as e:
            print(f"Failed to enqueue item {item_id}: {e}")
            return False
        finally:
            conn.close()

    def enqueue_from_s3(self):
        """
        Scans S3 for files and inserts them into queue_items if they are new.
        (For simplicity, we might just list all and check if they exist, or rely on distinctness)
        """
        print(f"Scanning S3 bucket {self.bucket}...")
        
        paginator = self.s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucket, Prefix=f"{self.prefix}/raw/")
        
        conn = db.get_db_connection()
        c = conn.cursor()
        
        count = 0
        for page in pages:
            if 'Contents' not in page:
                continue
                
            for obj in page['Contents']:
                key = obj['Key']
                # Skip directories?
                if key.endswith('/'): continue
                
                # Fetch object content
                # Optimization: In a real system, we'd track processed files to avoid re-reading.
                # For this exercise, we will just read and check existence or insert blindly?
                # The requirements say "re-runnable / idempotent".
                # Let's read the content.
                
                try:
                    resp = self.s3.get_object(Bucket=self.bucket, Key=key)
                    content = resp['Body'].read().decode('utf-8')
                    data = json.loads(content)
                    
                    # We use the GitHub ID as a way to check idempotency in queue?
                    # Or do we just insert everything and handle idempotency in the worker?
                    # "For each issue/PR record, insert a row into a queue table" with status 'pending'
                    # To allow re-running 'enqueue' without duplicates in queue, we should check if this specific item 
                    # (maybe by ID/Type combined?) is already in queue?
                    
                    item_id = str(data.get('id'))
                    
                    # Idempotency check:
                    # SQLite will reject INSERT if external_id exists due to UNIQUE constraint.
                    # We can use INSERT OR IGNORE.
                    
                    c.execute(
                        "INSERT OR IGNORE INTO queue_items (external_id, item_type, payload, status, created_at) VALUES (?, ?, ?, ?, ?)",
                        (item_id, "github_issue", content, "pending", data.get('created_at'))
                    )
                    
                    # Check if row was inserted (rowcount is not reliable with OR IGNORE in all sqlite versions, but usually 1 on insert, 0 on ignore)
                    if c.rowcount > 0:
                        count += 1
                    
                except Exception as e:
                    print(f"Failed to process {key}: {e}")
        
        conn.commit()
        conn.close()
        print(f"Enqueued {count} new items.")

    def _is_in_queue(self, cursor, item_id):
        # Stupid simple check: select all pending/processed items and parse? No, too slow.
        # I'll rely on a basic unique check. 
        # Actually, for this exercise, let's assume if it's in the queue, we skip.
        # Since I didn't add external_id, I have to rely on parsing payload or just trust.
        # Let's just insert for now? 
        # Requirement: "re-runnable".
        # I'll modify db.py in next step to add 'external_id' to make this efficient.
        return False
