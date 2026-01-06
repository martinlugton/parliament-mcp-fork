import subprocess
import sys
import time
import argparse
from datetime import date, datetime, timedelta

def run_command_with_check(cmd):
    """
    Runs a command, streams its output, and checks for errors in both return code and log content.
    Returns True if successful, False if failed.
    """
    cmd_str = " ".join(cmd)
    print(f"Running: {cmd_str}")
    sys.stdout.flush()

    # Start the process, capturing stdout and stderr
    # We merge stderr into stdout to simplify scanning
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True,
        shell=False
    )

    error_detected = False
    output_lines = []

    # Stream output to console and capture it
    while True:
        line = process.stdout.readline()
        if not line and process.poll() is not None:
            break
        if line:
            print(line, end='')
            output_lines.append(line)
            # Check for known error signatures in the log stream
            if "ERROR" in line or "RateLimitError" in line or "Failed to process" in line:
                error_detected = True

    return_code = process.poll()

    if return_code != 0:
        print(f"\nCommand failed with return code {return_code}")
        return False
    
    if error_detected:
        print(f"\nCommand finished with success code but errors were detected in the logs.")
        return False

    return True

def load_data_in_batches(start_date, end_date, load_hansard=True, load_pqs=True):
    current_start = start_date
    chunk_size = 7  # 1 week
    
    failed_batches = []

    while current_start <= end_date:
        current_end = current_start + timedelta(days=chunk_size - 1)
        if current_end > end_date:
            current_end = end_date

        start_str = current_start.strftime("%Y-%m-%d")
        end_str = current_end.strftime("%Y-%m-%d")

        print(f"\nProcessing batch: {start_str} to {end_str}")
        sys.stdout.flush()

        commands = []
        if load_hansard:
            commands.append(({"type": "hansard", "start": start_str, "end": end_str}, [
                "docker", "compose", "exec", "mcp-server", "uv", "run", "parliament-mcp", 
                "load-data", "hansard", "--from-date", start_str, "--to-date", end_str
            ]))
        
        if load_pqs:
            commands.append(({"type": "pqs", "start": start_str, "end": end_str}, [
                "docker", "compose", "exec", "mcp-server", "uv", "run", "parliament-mcp", 
                "load-data", "parliamentary-questions", "--from-date", start_str, "--to-date", end_str
            ]))

        for batch_info, cmd in commands:
            retries = 3
            success = False
            for attempt in range(retries):
                try:
                    if run_command_with_check(cmd):
                        success = True
                        break
                    else:
                        raise Exception("Command failed or errors detected in logs")
                except Exception as e:
                    print(f"Error running batch {start_str} to {end_str} (Attempt {attempt+1}/{retries}): {e}")
                    if attempt < retries - 1:
                        sleep_time = 10 * (attempt + 1)
                        print(f"Retrying in {sleep_time} seconds...")
                        time.sleep(sleep_time)
            
            if not success:
                print(f"Max retries reached for batch {start_str} to {end_str}. Adding to failed batches list.")
                failed_batches.append((batch_info, cmd))
            
            # small delay to let Qdrant catch up between commands
            time.sleep(2)

        # Move to next batch
        current_start = current_end + timedelta(days=1)

    return failed_batches

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Batch load parliament data.')
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

    print(f"Starting batched load from {start_date} to {end_date} (Hansard: {load_hansard}, PQs: {load_pqs})")
    failed = load_data_in_batches(start_date, end_date, load_hansard, load_pqs)

    if failed:
        print("\n" + "="*50)
        print(f"completed with {len(failed)} failed batches.")
        print("Attempting to resolve failed batches (Recovery Mode)...")
        print("="*50 + "\n")
        
        still_failed = []
        for batch_info, cmd in failed:
            print(f"Retrying failed batch: {batch_info['type']} ({batch_info['start']} to {batch_info['end']})")
            
            # Retry with more aggressive backoff
            retries = 3
            success = False
            for attempt in range(retries):
                try:
                    if run_command_with_check(cmd):
                        success = True
                        break
                    else:
                        raise Exception("Command failed or errors detected in logs")
                except Exception as e:
                    print(f"Error checking batch (Attempt {attempt+1}/{retries}): {e}")
                    if attempt < retries - 1:
                        sleep_time = 30 * (attempt + 1) # Longer backoff for recovery
                        print(f"Retrying in {sleep_time} seconds...")
                        time.sleep(sleep_time)
            
            if not success:
                print(f"Failed to recover batch: {batch_info}")
                still_failed.append(batch_info)
        
        if still_failed:
            print("\n" + "!"*50)
            print(f"FINAL REPORT: {len(still_failed)} batches could not be recovered.")
            for info in still_failed:
                print(f" - {info}")
            print("!"*50)
            sys.exit(1)
        else:
            print("\nAll failed batches successfully recovered!")
            sys.exit(0)
    else:
        print("\nAll batches processed successfully.")
        sys.exit(0)
