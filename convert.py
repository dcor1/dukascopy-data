#!/usr/bin/env python3

import argparse
import csv
import lzma
import os
import struct
from datetime import datetime, timedelta

def bi5_to_csv(bi5_path, csv_path, date_str, hour):
    """
    Convert a Dukascopy .bi5 (LZMA-compressed) file to CSV.
    
    Assumes each tick record is 20 bytes, in the format (big-endian):
      [0..4)   int   ms_since_hour
      [4..8)   int   ask_price_raw
      [8..12)  int   bid_price_raw
      [12..16) int   ask_volume
      [16..20) int   bid_volume
    
    Prices are often scaled by 10^5. Timestamps are date + hour + ms offset.
    """
    
    # Parse date from string
    # Example date_str = "2024-11-03"
    base_date = datetime.strptime(date_str, "%Y-%m-%d")
    base_datetime = base_date.replace(hour=hour, minute=0, second=0, microsecond=0)
    
    # Read and decompress the .bi5 file
    with open(bi5_path, 'rb') as f_in:
        compressed_data = f_in.read()
    try:
        decompressed_data = lzma.decompress(compressed_data)
    except lzma.LZMAError as e:
        print(f"Error decompressing {bi5_path}: {e}")
        return

    # Prepare CSV output
    with open(csv_path, 'w', newline='') as f_out:
        writer = csv.writer(f_out)
        writer.writerow(["timestamp", "ask", "bid", "ask_volume", "bid_volume"])

        # Parse records in 20-byte chunks
        record_size = 20
        for i in range(0, len(decompressed_data), record_size):
            chunk = decompressed_data[i:i+record_size]
            if len(chunk) < record_size:
                # Ignore incomplete final chunks
                break
            
            # Unpack big-endian: 5 integers
            # Adjust format as needed if your files differ
            ms_since_hour, ask_raw, bid_raw, ask_vol, bid_vol = struct.unpack(">iiiii", chunk)
            
            # Build a full timestamp by adding ms_since_hour to base_datetime
            tick_time = base_datetime + timedelta(milliseconds=ms_since_hour)
            
            # Dukascopy often stores prices as integer = real_price * 10^5
            ask_price = ask_raw / 100000.0
            bid_price = bid_raw / 100000.0
            
            writer.writerow([
                tick_time.strftime("%Y-%m-%d %H:%M:%S.%f"),
                ask_price,
                bid_price,
                ask_vol,
                bid_vol
            ])

    print(f"Converted: {bi5_path} -> {csv_path}")

def main():
    parser = argparse.ArgumentParser(description="Convert Dukascopy .bi5 to CSV.")
    parser.add_argument("--bi5-file", required=True, help="Path to the .bi5 file.")
    parser.add_argument("--csv-file", required=True, help="Output CSV file.")
    parser.add_argument("--date", required=True, help="Date in YYYY-MM-DD format (e.g. 2024-11-03).")
    parser.add_argument("--hour", type=int, default=0, help="Hour of the day (0-23) for the .bi5 file.")
    
    args = parser.parse_args()

    # Ensure output directory exists
    os.makedirs(os.path.dirname(args.csv_file) or ".", exist_ok=True)

    bi5_to_csv(
        bi5_path=args.bi5_file,
        csv_path=args.csv_file,
        date_str=args.date,
        hour=args.hour
    )

if __name__ == "__main__":
    main()
