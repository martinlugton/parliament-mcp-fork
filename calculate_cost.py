
import csv
import os
from datetime import datetime

csv_path = 'data/token_usage.csv'
total_cost = 0.0
today_cost = 0.0
today_str = datetime.now().strftime('%Y-%m-%d')

if os.path.exists(csv_path):
    with open(csv_path, 'r') as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
            for row in reader:
                if row and len(row) >= 5:
                    try:
                        cost = float(row[-1])
                        total_cost += cost
                        if row[0].startswith(today_str):
                            today_cost += cost
                    except ValueError:
                        continue
        except StopIteration:
            pass

print(f"Total Cumulative Cost: ${total_cost:.4f}")
print(f"Cost for Today ({today_str}): ${today_cost:.4f}")
