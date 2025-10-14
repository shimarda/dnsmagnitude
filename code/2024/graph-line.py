import pandas as pd
import matplotlib.pyplot as plt
import glob
import os
import re
from datetime import datetime
from matplotlib.animation import FuncAnimation
from matplotlib.ticker import MultipleLocator
import argparse

def visualize_popularity_trend_per_domain_gif(year, month, day):
    # CSVファイル名の形式にマッチするファイルを全て読み込む
    csv_files = sorted(glob.glob("/home/shimada/analysis/output/dns_mag/*.csv"))

    pattern = re.compile(rf"({year})-({month})-({day})\.csv")
    filtered_files = [file for file in csv_files if pattern.match(os.path.basename(file))]
    
    if not filtered_files:
        print("No CSV files found. Please ensure there are CSV files in the correct format.")
        return
    
    # データフレームを全て読み込む
    data_frames = []
    for file in filtered_files:
        match = pattern.match(os.path.basename(file))
        if match:
            year, month, day = match.groups()
            try:
                date_time = datetime.strptime(f"{year}-{month}-{day}", "%Y-%m-%d")
                df = pd.read_csv(file, dtype=str, low_memory=False)
                df["date_time"] = date_time
                if "domain" in df.columns and "dnsmagnitude" in df.columns:
                    df = df.drop_duplicates(subset=["domain"])
                    data_frames.append(df)
                else:
                    print(f"Skipping file {file} due to missing required columns: 'domain' or 'dnsmagnitude'")
            except ValueError as e:
                print(f"Skipping file {file} due to date parsing error: {e}")
    
    if not data_frames:
        print("No valid data frames to concatenate. Please check the CSV files.")
        return
    
    # 全てのデータを結合する
    full_data = pd.concat(data_frames)

    # 全てのユニークなドメインと日付のリストを取得
    unique_domains = full_data["domain"].unique()
    unique_dates = full_data["date_time"].unique()
    unique_dates = pd.to_datetime(unique_dates)
    
    # データを整形して存在しない値を補完
    domain_list = []
    date_list = []
    magnitude_list = []
    
    for domain in unique_domains:
        for date in unique_dates:
            filtered = full_data[(full_data["domain"] == domain) & (full_data["date_time"] == date)]
            if not filtered.empty:
                domain_list.append(domain)
                date_list.append(date)
                magnitude_list.append(filtered["dnsmagnitude"].iloc[0])
            else:
                domain_list.append(domain)
                date_list.append(date)
                magnitude_list.append(0)
    
    full_data = pd.DataFrame({
        "domain": domain_list,
        "date_time": date_list,
        "dnsmagnitude": magnitude_list
    })

    # データ型を調整
    full_data["dnsmagnitude"] = pd.to_numeric(full_data["dnsmagnitude"], errors='coerce').fillna(0)

    # GIF用のアニメーション作成
    fig, ax = plt.subplots(figsize=(10, 6))
    
    def update(frame):
        ax.clear()
        domain = unique_domains[frame]
        domain_data = full_data[full_data["domain"] == domain].sort_values("date_time")
        
        ax.plot(domain_data["date_time"], domain_data["dnsmagnitude"], marker='o', label=f"{domain}")
        ax.set_xlabel("Date", fontsize=12)
        ax.set_ylabel("DNS Magnitude", fontsize=24)
        ax.set_title(f"{domain}", fontsize=26)
        ax.set_ylim(0, 10)
        ax.set_xticks(domain_data["date_time"])
        ax.set_xticklabels(domain_data["date_time"].dt.strftime("%Y-%m-%d"), rotation=45)
        ax.yaxis.set_major_locator(MultipleLocator(1))
        ax.grid()
        plt.tight_layout()
        plt.tick_params(labelsize=16)

        # 画像を保存
        output_image = f"/home/shimada/analysis/output/dns_mag/pic/{domain}.png"
        fig.savefig(output_image)

    # アニメーションを作成
    ani = FuncAnimation(fig, update, frames=len(unique_domains), repeat=True)
    # GIFとして保存
    ani.save(f"/home/shimada/analysis/output/dns_mag/{year}-{month}-{day}-line.gif", writer="pillow", fps=2)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('-y')
    parser.add_argument('-m')
    parser.add_argument('-d')

    args = parser.parse_args()

    year = args.y
    month = args.m
    day = args.d

    visualize_popularity_trend_per_domain_gif(year, month, day)