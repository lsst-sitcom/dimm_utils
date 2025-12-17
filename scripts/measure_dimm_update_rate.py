#!/usr/bin/env python3
import argparse
import subprocess
import time
from datetime import datetime

def monitor_file_events(filepath: str):
    """Monitor all inotify events on a file with timing information."""
    
    proc = subprocess.Popen(
        ['inotifywait', '-m', 
         '-e', 'modify',
         '-e', 'close_write', 
         '-e', 'move_self',
         '-e', 'attrib',
         filepath],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True
    )

    prev_time = None
    count = 0

    print(f"Monitoring all events on: {filepath}")
    print("Press Ctrl+C to stop\n")

    try:
        for line in proc.stdout:
            current_time = time.time()
            timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]  # milliseconds
            
            # Parse event type from inotifywait output
            parts = line.strip().split()
            event_type = parts[1] if len(parts) > 1 else "UNKNOWN"
            
            if prev_time:
                interval = (current_time - prev_time) * 1000  # ms
                hz = 1000 / interval
                print(f"{timestamp}  {event_type:15s}  Interval: {interval:6.2f} ms  Rate: {hz:6.1f} Hz")
            else:
                print(f"{timestamp}  {event_type:15s}  (first event)")
            
            prev_time = current_time
            count += 1
                
    except KeyboardInterrupt:
        print(f"\n\nStopped. Total events: {count}")
    finally:
        proc.terminate()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Monitor file system events with timing information"
    )
    parser.add_argument(
        "file",
        help="Path to the file to monitor"
    )
    
    args = parser.parse_args()
    monitor_file_events(args.file)

