import subprocess
import json
import sys
from datetime import datetime, timedelta

def run_command(cmd):
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        return None
    return result.stdout

def main():
    print("Checking current data status...")
    output = run_command(["docker", "compose", "exec", "mcp-server", "uv", "run", "python", "check_progress.py", "--json"])
    
    if not output:
        print("Could not get current status. Make sure the containers are running.")
        sys.exit(1)

    # Filter out warnings if present
    json_str = [line for line in output.splitlines() if line.strip().startswith("{")][0]
    status = json.loads(json_str)
    
    today = datetime.now().date()
    
    for dtype in ["hansard", "pqs"]:
        latest_str = status.get(dtype, {}).get("latest")
        if not latest_str:
            print(f"No existing data found for {dtype}. Starting from 2024-07-04.")
            start_date = "2024-07-04"
        else:
            latest_dt = datetime.strptime(latest_str, "%Y-%m-%d").date()
            if latest_dt >= today:
                print(f"{dtype} is already up to date ({latest_str}).")
                continue
            
            # Start from the day after the latest entry
            start_date = (latest_dt + timedelta(days=1)).strftime("%Y-%m-%d")
            
        print(f"Syncing {dtype} from {start_date} to {today}...")
        subprocess.run(["python", "batch_load_data.py", "--start-date", start_date, "--type", dtype])

    print("\nSync complete.")

if __name__ == "__main__":
    main()
