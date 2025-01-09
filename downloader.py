#!/usr/bin/env python3

import os
import requests
import argparse
from datetime import datetime, timedelta
import concurrent.futures

def download_one_hour(instrument, year, month, day, hour, output_folder):
    """
    Download a single hour's .bi5 file from Dukascopy, only overwriting
    if the file on the server is a different size than the local file.
    """
    url = (
        f"https://datafeed.dukascopy.com/datafeed/{instrument}/"
        f"{year}/{month:02d}/{day:02d}/{hour:02d}h_ticks.bi5"
    )
    date_str = f"{year:04d}{month:02d}{day:02d}"
    filename = f"{instrument}_{date_str}_{hour:02d}h_ticks.bi5"
    
    # Folder: output_folder/instrument/
    folder = os.path.join(output_folder, instrument)
    os.makedirs(folder, exist_ok=True)
    filepath = os.path.join(folder, filename)

    # If the file exists locally, check size via HEAD request
    if os.path.exists(filepath):
        local_size = os.path.getsize(filepath)
        # If the local file is > 0 bytes, compare with server content-length
        if local_size > 0:
            try:
                head_resp = requests.head(url, timeout=10, allow_redirects=True)
                if head_resp.status_code == 200:
                    content_length = head_resp.headers.get("Content-Length")
                    if content_length is not None:
                        server_size = int(content_length)
                        # If sizes match, skip downloading
                        if server_size == local_size:
                            return f"Skipped (same size): {filepath}"
                # If HEAD fails or no content-length, proceed with GET
            except Exception as e:
                # HEAD request failed, proceed with GET
                pass

    # Either file doesn't exist, sizes differ, or HEAD didn't help -> do GET
    try:
        get_resp = requests.get(url, timeout=10)
        if get_resp.status_code == 200 and get_resp.content:
            new_data = get_resp.content
            new_size = len(new_data)
            
            # If the file already exists, compare actual sizes
            if os.path.exists(filepath):
                local_size = os.path.getsize(filepath)
                if new_size == local_size:
                    return f"Skipped (same size after GET): {filepath}"
            
            # Overwrite or create new file
            with open(filepath, "wb") as f:
                f.write(new_data)
            return f"Downloaded: {filepath}"
        else:
            return f"No data for {url} (status {get_resp.status_code})"
    except Exception as e:
        return f"Error downloading {url}: {e}"

def download_dukascopy_ticks(instrument, start_date, end_date, output_folder, threads):
    """
    Download .bi5 files in parallel, only overwriting local files if
    the server file size differs.
    """
    tasks = []
    current_date = start_date
    while current_date <= end_date:
        for hour in range(24):
            tasks.append((
                instrument,
                current_date.year,
                current_date.month,
                current_date.day,
                hour,
                output_folder
            ))
        current_date += timedelta(days=1)

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        future_to_task = {executor.submit(download_one_hour, *t): t for t in tasks}
        for future in concurrent.futures.as_completed(future_to_task):
            print(future.result())

def main():
    parser = argparse.ArgumentParser(
        description="Download Dukascopy tick data in .bi5 format with parallel threads, "
                    "only overwriting local files if the file size differs."
    )
    parser.add_argument(
        "--instrument",
        type=str,
        required=True,
        help="Instrument symbol (e.g. EURUSD, GBPUSD, USDJPY)."
    )
    parser.add_argument(
        "--start-date",
        type=str,
        required=True,
        help="Start date in YYYY-MM-DD format."
    )
    parser.add_argument(
        "--end-date",
        type=str,
        required=True,
        help="End date in YYYY-MM-DD format."
    )
    parser.add_argument(
        "--output-folder",
        type=str,
        required=True,
        help="Directory to store the downloaded .bi5 files."
    )
    parser.add_argument(
        "--threads",
        type=int,
        required=True,
        help="Number of parallel download threads."
    )

    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()

    download_dukascopy_ticks(
        instrument=args.instrument,
        start_date=start_date,
        end_date=end_date,
        output_folder=args.output_folder,
        threads=args.threads
    )

if __name__ == "__main__":
    main()
