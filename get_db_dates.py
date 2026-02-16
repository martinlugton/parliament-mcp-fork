
import sqlite3
import os

db_path = 'data/loader_state.db'
if not os.path.exists(db_path):
    print(f"Database not found at {db_path}")
    exit(1)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()
try:
    cursor.execute("SELECT source_type, status, MAX(date), COUNT(*) FROM queue GROUP BY source_type, status")
    results = cursor.fetchall()
    if not results:
        print("No items found in the queue.")
    for source_type, status, max_date, count in results:
        print(f"{source_type.upper()} | {status:10} | Latest Date: {max_date} | Count: {count}")
except Exception as e:
    print(f"Error: {e}")
finally:
    conn.close()
