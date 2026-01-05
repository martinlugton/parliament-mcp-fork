import subprocess
import sys
import time
import argparse
from datetime import date, datetime, timedelta

def load_data_in_batches(start_date, end_date, load_hansard=True, load_pqs=True):
    current_start = start_date
    chunk_size = 7  # 1 week

    while current_start <= end_date:
        current_end = current_start + timedelta(days=chunk_size - 1)
        if current_end > end_date:
            current_end = end_date

        start_str = current_start.strftime("%Y-%m-%d")
        end_str = current_end.strftime("%Y-%m-%d")

        print(f"Processing batch: {start_str} to {end_str}")
        sys.stdout.flush()

        commands = []
        if load_hansard:
            commands.append([
                "uv", "run", "parliament-mcp", "--log-level", "WARNING",
                "load-data", "hansard", "--from-date", start_str, "--to-date", end_str
            ])
        
        if load_pqs:
            commands.append([
                "uv", "run", "parliament-mcp", "--log-level", "WARNING",
                "load-data", "parliamentary-questions", "--from-date", start_str, "--to-date", end_str
            ])

        for cmd in commands:
            cmd_str = " ".join(cmd)
            print(f"Running: {cmd_str}")
            sys.stdout.flush()
            retries = 3
            for attempt in range(retries):
                try:
                    subprocess.run(cmd, check=True)
                    break 
                except subprocess.CalledProcessError as e:
                    print(f"Error running batch {start_str} to {end_str} (Attempt {attempt+1}/{retries}): {e}")
                    if attempt < retries - 1:
                        print("Retrying in 10 seconds...")
                        time.sleep(10)
                    else:
                        print("Max retries reached. Moving to next command/batch.")
            
            time.sleep(2)

        current_start = current_end + timedelta(days=1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Batch load parliament data (internal).')
    parser.add_argument('--start-date', type=str, help='Start date in YYYY-MM-DD format', required=True)
    parser.add_argument('--end-date', type=str, help='End date in YYYY-MM-DD format (default: today)', required=False)
    parser.add_argument('--type', choices=['all', 'hansard', 'pqs'], default='all', help='Type of data to load')
    
    args = parser.parse_args()

    try:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    except ValueError:
        print("Invalid start date format. Please use YYYY-MM-DD.")
        sys.exit(1)

    if args.end_date:
        try:
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
        except ValueError:
            print("Invalid end date format. Please use YYYY-MM-DD.")
            sys.exit(1)
    else:
        end_date = date.today()
    
    load_hansard = args.type in ['all', 'hansard']
    load_pqs = args.type in ['all', 'pqs']

    load_data_in_batches(start_date, end_date, load_hansard, load_pqs)
