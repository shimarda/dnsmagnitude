import pandas as pd
import os
import argparse
import glob
import re

def count_dst_ip(year, month, day):
    csv_files = sorted(glob.glob("/mnt/qnap2/shimada/input/*.csv"))

    pattern = re.compile(rf"{year}-{month}-{day}-\d{{2}}\.csv")

    matched_files = [file for file in csv_files if pattern.match(os.path.basename(file))]

    if not matched_files:
        print("No csv file found")
        return
    
    data_frames = []
    for file in matched_files:
        df = pd.read_csv(file, dtype=str, low_memory=False, usecols=["ip.dst"])
        data_frames.append(df)

    full_data = pd.concat(data_frames)

    unique_dst_addr = full_data["ip.dst"].unique().tolist()
    print(len(unique_dst_addr))


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument('-y')
    parser.add_argument('-m')
    parser.add_argument('-d')

    args = parser.parse_args()

    year = args.y
    month = args.m
    day = args.d

    count_dst_ip(year, month, day)