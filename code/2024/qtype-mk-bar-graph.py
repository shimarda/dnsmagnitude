import pandas as pd
import matplotlib.pyplot as plt
import glob
import os
from datetime import datetime
from matplotlib.animation import FuncAnimation

# CSVファイルの読み込みとGIFの作成

def visualize_popularity_trend():
    # CSVファイル名の形式にマッチするファイルを全て読み込む
    file_name = "auth"
    month = 11
    csv_files = sorted(glob.glob(f"/home/shimada/analysis/output/dns_mag/{file_name}-{month}*.csv"))
    
    if not csv_files:
        print("No CSV files found. Please ensure there are CSV files in the correct format.")
        return
    
    # データフレームを全て読み込む
    data_frames = []
    for file in csv_files:
        date = os.path.basename(file).replace(".csv", "").replace(f"{file_name}-", "")
        date = date.split("-")
        try:
            date_time = datetime.strptime(f"2023-{date[0]}-{date[1]}", "%Y-%m-%d")
            df = pd.read_csv(file, dtype=str, low_memory=False)
            df["date_time"] = date_time
            if "domain" in df.columns and "dnsmagnitude" in df.columns:
                data_frames.append(df)
            else:
                print(f"Skipping file {file} due to missing required columns: 'domain' or 'popularity'")
        except ValueError as e:
            print(f"Skipping file {file} due to date parsing error: {e}")
    
    if not data_frames:
        print("No valid data frames to concatenate. Please check the CSV files.")
        return
    
    # 全てのデータを結合する
    full_data = pd.concat(data_frames)

    # ドメインごとに色を固定するためにカラーマップを作成
    unique_domains = full_data["domain"].unique()
    color_map = {domain: plt.get_cmap('tab20')(i % 20) for i, domain in enumerate(unique_domains)}

    # 全てのドメインを含む人気度を棒グラフで描画し、日にちごとの変化をGIFとして保存
    fig, ax = plt.subplots(figsize=(80, 20))  # 幅を広くするためにfigsizeを変更
    # 最初のフレームのドメイン順序を保存
    initial_order = full_data[full_data["date_time"] == full_data["date_time"].unique()[0]]["domain"].tolist()

    def update(frame):
        ax.clear()
        current_time = full_data["date_time"].unique()[frame]
        current_data = full_data[full_data["date_time"] == current_time]
        #これで棒グラフの場所を1枚目の位置に固定
        current_data = current_data.set_index("domain").reindex(initial_order).reset_index()
        colors = [color_map[domain] for domain in current_data["domain"]]
        ax.bar(current_data["domain"], pd.to_numeric(current_data["dnsmagnitude"], errors='coerce').fillna(0), color=colors)

        ax.set_xlabel("Domain")
        ax.set_ylabel("DNS Magnitude", fontsize = 30)
        ax.set_title(f"DNS Magnitude on {current_time}", fontsize = 30)
        plt.xticks(rotation=45, ha='right')
        plt.ylim(0, 10)
        plt.tight_layout()

    ani = FuncAnimation(fig, update, frames=len(full_data["date_time"].unique()), repeat=True)
    ani.save(f"/home/shimada/analysis/output/dns_mag/{file_name}-{month}-bar.gif", writer="pillow", fps=1)

if __name__ == "__main__":
    visualize_popularity_trend()
