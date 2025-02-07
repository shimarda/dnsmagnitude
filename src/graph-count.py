import pandas as pd
import matplotlib.pyplot as plt
import glob
import os
import re
from datetime import datetime
from matplotlib.animation import FuncAnimation
from matplotlib.ticker import MultipleLocator
import argparse
import math

def round_up_nice(x):
    """
    数値 x を受け取り、x 以上の切りのいい数値に丸めて返す。
    数値の大きさに応じて丸める単位を変更する。
    例: 5764 → 6000, 150 → 200
    """
    if x < 10:
        base = 1
    elif x < 100:
        base = 10
    elif x < 1000:
        base = 100
    elif x < 10000:
        base = 1000
    elif x < 100000:
        base = 10000
    else:
        base = 100000
    return math.ceil(x / base) * base

def visualize_popularity_trend_per_domain_gif(year, month, day):
    # CSVファイル名の形式にマッチするファイルを全て読み込む
    # csv_files = sorted(glob.glob("/home/shimada/analysis/output/dns_mag/tuika/*.csv"))
    csv_files = sorted(glob.glob("/home/shimada/analysis/output/count/*.csv"))
    pattern = re.compile(rf"({year})-({month})-({day})\.csv")
    filtered_files = [file for file in csv_files if pattern.match(os.path.basename(file))]
    
    if not filtered_files:
        print("No CSV files found. Please ensure there are CSV files in the correct format.")
        return
    
    # 各CSVからデータフレームを読み込み
    data_frames = []
    for file in filtered_files:
        match = pattern.match(os.path.basename(file))
        if match:
            year_str, month_str, day_str = match.groups()
            try:
                date_time = datetime.strptime(f"{year_str}-{month_str}-{day_str}", "%Y-%m-%d")
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

    # ユニークなドメインと日付のリストを取得
    unique_domains = full_data["domain"].unique()
    unique_dates = pd.to_datetime(full_data["date_time"].unique())
    
    # ドメイン×日付の全組み合わせについて、データが存在しない場合は0を補完
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
                magnitude_list.append(0)  # データがない場合は0で補完
    
    full_data = pd.DataFrame({
        "domain": domain_list,
        "date_time": date_list,
        "dnsmagnitude": magnitude_list
    })

    # 数値型に変換（変換できない値は0に）
    full_data["dnsmagnitude"] = pd.to_numeric(full_data["dnsmagnitude"], errors='coerce').fillna(0)

    # GIF用のアニメーション作成
    fig, ax = plt.subplots(figsize=(10, 6))
    
    def update(frame):
        ax.clear()
        domain = unique_domains[frame]
        domain_data = full_data[full_data["domain"] == domain].sort_values("date_time")
        
        ax.plot(domain_data["date_time"], domain_data["dnsmagnitude"], marker='o', label=f"{domain}")
        ax.set_xlabel("Date", fontsize=12)
        ax.set_ylabel("Count", fontsize=12)
        ax.set_title(f"{domain}", fontsize=18)
        
        # 各ドメインの最大値に10%のマージンを加えた候補値を求め、切りのいい数値に丸める
        max_val = domain_data["dnsmagnitude"].max()
        if pd.isna(max_val) or max_val == 0:
            max_val = 1  # データがすべて0の場合は1を上限にする
        margin = max_val * 0.1
        candidate = max_val + margin
        nice_max = round_up_nice(candidate)
        ax.set_ylim(0, nice_max)
        plt.tick_params(labelsize=16)
        
        # X軸のラベル設定（全ての日付を表示）
        ax.set_xticks(domain_data["date_time"])
        ax.set_xticklabels(domain_data["date_time"].dt.strftime("%Y-%m-%d"), rotation=45)
        
        # Y軸の目盛りは、nice_max を 5 等分した値を目安として設定
        # tick_interval = nice_max / 5
        # ax.yaxis.set_major_locator(MultipleLocator(tick_interval))
        
        ax.grid()
        plt.tight_layout()

        # 各ドメインごとに画像を保存（任意）
        output_image = f"/home/shimada/analysis/output/pic/count/{domain}.png"
        fig.savefig(output_image)

    # アニメーション作成（各ドメインごとにフレームを更新）
    ani = FuncAnimation(fig, update, frames=len(unique_domains), repeat=True)
    # GIFとして保存
    gif_path = f"/home/shimada/analysis/output/dns_mag/{year}-{month}-{day}-line.gif"
    ani.save(gif_path, writer="pillow", fps=2)
    print(f"GIF saved to {gif_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-y', required=True, help="Year (YYYY)")
    parser.add_argument('-m', required=True, help="Month (MM)")
    parser.add_argument('-d', required=True, help="Day (DD)")
    args = parser.parse_args()

    visualize_popularity_trend_per_domain_gif(args.y, args.m, args.d)
