import re
import pandas as pd
import os
import glob
import csv
import statistics
import argparse
import io
import logging
import math
import numpy as np
from collections import defaultdict
from datetime import datetime, timedelta

# ===== 共通設定 =====
OUTPUT_BASE_DIR = "/home/shimada/code/refactored/output-2024"

def ensure_output_dir(subdir=""):
    """出力ディレクトリを確実に作成する"""
    if subdir:
        output_dir = os.path.join(OUTPUT_BASE_DIR, subdir)
    else:
        output_dir = OUTPUT_BASE_DIR
    os.makedirs(output_dir, exist_ok=True)
    return output_dir

def setup_logging(log_name="dns_analysis_2024"):
    """ログ設定を初期化"""
    log_dir = ensure_output_dir("logs")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(os.path.join(log_dir, f'{log_name}.log')),
            logging.StreamHandler()
        ]
    )

# ===== ファイル操作関連 =====
def file_lst(year, month, day, where=None):
    """パターンに一致するファイルリストを取得"""
    if where is None:
        # 2024年のcount.pyなど権威サーバーのみの場合
        path = "/mnt/qnap2/shimada/input/*.csv"
        print("権威")
    elif int(where) == 0:
        path = "/mnt/qnap2/shimada/input/*.csv"
        print("権威")
    else:
        path = "/mnt/qnap2/shimada/resolver/*.csv"
        print("リゾルバ")
    
    pat = re.compile(rf"{year}-{month}-{day}-\d{{2}}\.csv")
    files = sorted(glob.glob(path))
    filtered_files = [file for file in files if pat.match(os.path.basename(file))]
    return filtered_files

def file_time(file_lst):
    """ファイル名リストから時間のみを抽出"""
    time_list = [
        os.path.basename(f).replace('-', '').replace('.csv', '') for f in file_lst
    ]
    return time_list

def file_time_dict(file_lst):
    """ファイル名から年月日と時間を抽出して、キーを年月日、値を時間リストとする辞書を返す"""
    file_dict = {}
    for file in file_lst:
        basename = os.path.basename(file)
        parts = basename.split('-')
        if len(parts) >= 4:
            y = parts[0]
            m = parts[1]
            d = parts[2]
            # parts[3] は "HH.csv" となっているので拡張子を除去
            hour = parts[3].replace('.csv', '')
            date_key = f"{y}-{m}-{d}"  # 例: "2025-02-07"
            if date_key not in file_dict:
                file_dict[date_key] = []
            file_dict[date_key].append(hour)
    return file_dict

def find_output_files(year, month, day, where="", subdir="", pattern_prefix="count"):
    """出力ディレクトリからパターンに一致するファイルを検索"""
    dir_path = ensure_output_dir(subdir)
    if where:
        pattern = re.compile(fr"{pattern_prefix}-{where}-{year}-{month}-{day}\.csv")
    else:
        pattern = re.compile(fr"{year}-{month}-{day}\.csv")
    files = sorted(glob.glob(os.path.join(dir_path, "*.csv")))
    found_files = [file for file in files if pattern.search(os.path.basename(file))]
    return found_files

def find_output_files_by_pattern(where, year, month, day, subdir=""):
    """magnitude-average-variance.py用のファイル検索"""
    dir_path = ensure_output_dir(subdir)
    pattern = re.compile(fr"{where}-{year}-{month}-{day}\.csv")
    files = sorted(glob.glob(os.path.join(dir_path, "*.csv")))
    found_files = [file for file in files if pattern.search(os.path.basename(file))]
    return found_files

# ===== データ処理関連 =====
def extract_subdomain(qname):
    """DNSクエリ名からサブドメインを抽出"""
    suffix = '.tsukuba.ac.jp'
    if isinstance(qname, str):
        qname_lower = qname.lower()
        if qname_lower.count(suffix) != 1:
            return None
        if qname_lower.endswith(suffix):
            qname_no_suffix = qname_lower[:-len(suffix)]
            if qname_no_suffix.endswith('.'):
                qname_no_suffix = qname_no_suffix[:-1]
            if qname_no_suffix:
                subdom = qname_no_suffix.split('.')[-1]
                return subdom
    return None

def open_reader(file_name, where=None):
    """ファイル名からファイルを開く"""
    if where is None:
        # 2024年のcount.pyなど権威サーバーのみの場合
        file_path = f"/mnt/qnap2/shimada/input/{file_name}"
    elif int(where) == 0:
        file_path = f"/mnt/qnap2/shimada/input/{file_name}"
    else:
        file_path = f"/mnt/qnap2/shimada/resolver/{file_name}"
    df = pd.read_csv(file_path)
    return df

def open_reader_safe(file_name, where=None):
    """安全にファイルを開く関数"""
    try:
        df = open_reader(file_name, where)
        return df
    except Exception as e:
        print(f"ファイル {file_name} の読み込み中にエラーが発生しました: {str(e)}")
        return pd.DataFrame()

# ===== IP数カウント関連 =====
def count_dst_ip(year, month, day):
    """送信先IPアドレスの数をカウント"""
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
    return len(unique_dst_addr)

# ===== マグニチュード計算関連 =====
def calculate_magnitude_2024(domain_dict, A_tot):
    """2024年版のマグニチュード計算（送信先IP数ベース）"""
    magnitude_dict = {}
    for domain, src_addrs in domain_dict.items():
        src_addr_count = len(src_addrs)
        if src_addr_count > 0:
            magnitude_dict[domain] = src_addr_count
    
    # マグニチュードの降順でソート
    return dict(sorted(magnitude_dict.items(), key=lambda item: item[1], reverse=True))

def process_magnitude_hourly_data_2024(df, uni_src_set, domain_dict):
    """1時間分のマグニチュード計算用データを処理（2024年版）"""
    if df.empty:
        print(f"空のデータフレーム - スキップします")
        return

    # 'ip.dst' のユニークなアドレスをセットに追加
    uni_src_set.update(df['ip.dst'].unique())

    # 'dns.qry.name' からサブドメインを抽出し、新しい列 'subdomain' を作成
    df['subdomain'] = df['dns.qry.name'].apply(extract_subdomain)
    df_sub = df[df['subdomain'].notnull()]

    # サブドメインごとに、対応する送信先IPアドレスのセットを作成
    hour_domain_src_addr_dict = df_sub.groupby('subdomain')['ip.dst'].apply(set).to_dict()

    # 結果を更新
    for domain, src_addrs in hour_domain_src_addr_dict.items():
        if domain in domain_dict:
            domain_dict[domain].update(src_addrs)
        else:
            domain_dict[domain] = src_addrs

# ===== CSV出力関連 =====
def write_magnitude_csv_2024(mag_dict, date_key, subdir="count"):
    """2024年版のマグニチュード結果をCSVに書き込み"""
    output_dir = ensure_output_dir(subdir)
    csv_file_path = os.path.join(output_dir, f"{date_key}.csv")
    with open(csv_file_path, "w", newline='') as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow(['date', 'domain', 'dnsmagnitude'])
        for subdomain, magnitude in mag_dict.items():
            writer.writerow([date_key, subdomain, str(magnitude)])

def write_tshark_args_csv(mag_dict, day, year, month, where, subdir="dns_mag"):
    """tshark-args.py用のCSV出力"""
    output_dir = ensure_output_dir(subdir)
    csv_file_path = os.path.join(output_dir, f"{where}-{year}-{month}-{day}.csv")
    with open(csv_file_path, "w", newline='') as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow(['day', 'domain', 'dnsmagnitude'])
        for subdomain in mag_dict:
            writer.writerow([f"{day}", subdomain, str(mag_dict[subdomain])])

def write_average_distribution_csv_2024(data_dict, year, month, day, where, data_type, subdir="ave-distr"):
    """平均値または分散値をCSVに書き込み（2024年版）"""
    output_dir = ensure_output_dir(subdir)
    csv_file_path = os.path.join(output_dir, f"{data_type}-{where}-{year}-{month}-{day}.csv")
    with open(csv_file_path, "w", newline='') as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow(['domain', data_type])
        for domain, value in data_dict.items():
            writer.writerow([domain, value])

# ===== 統計分析関連 =====
def calculate_average_distribution_2024(file_list, all_domains):
    """ファイルリストから平均値と分散を計算（2024年版）"""
    total_dict = {dom: [] for dom in all_domains}

    # CSVファイルごとに値をリストに追加（存在しない場合は0を追加）
    for file in file_list:
        df = pd.read_csv(file)
        domain_to_value = dict(zip(df['domain'], df['dnsmagnitude']))

        for dom in all_domains:
            if dom in domain_to_value:
                total_dict[dom].append(float(domain_to_value[dom]))
            else:
                total_dict[dom].append(0.0)

    # 平均値を計算
    average_dict = {dom: statistics.mean(vals) for dom, vals in total_dict.items()}
    average_dict = dict(sorted(average_dict.items(), key=lambda item: item[1], reverse=True))

    # 分散を計算
    distribution_dict = {dom: statistics.pvariance(vals) for dom, vals in total_dict.items()}
    distribution_dict = dict(sorted(distribution_dict.items(), key=lambda item: item[1], reverse=True))

    return average_dict, distribution_dict

# ===== 度数分布表作成関連 =====
def make_frequency_table(file_path, threshold=5):
    """
    CSVファイルから average 列を読み込み、
    階級幅を1とした度数分布表を作成する。
    ビンは大きい方(右端)から順に表示し、
    もし最小値が0未満なら0に丸める（負の区間を防止）。
    また、平均が指定した閾値以上の行が全体の何パーセントかも計算する。
    """
    try:
        # CSV読み込み（average 列を float として読み込む）
        df = pd.read_csv(file_path, dtype={'average': float}, low_memory=False)
        
        # 追加: 平均が閾値以上の行の割合を計算
        total_rows = len(df)
        if total_rows > 0:
            count_ge_threshold = (df['average'] >= threshold).sum()
            percent_ge_threshold = count_ge_threshold / total_rows * 100
            print(f"平均が{threshold}以上のものは {percent_ge_threshold:.2f}% です。")
        else:
            print("CSVファイルにデータがありません。")
            return None, None
        
        # 最小値・最大値の取得
        min_val = df['average'].min()
        max_val = df['average'].max()

        # 最小値が0未満なら0に補正
        if min_val < 0:
            min_val = 0.0

        # 階級幅を1にするため、min_val を下方向に切り捨て、max_val を上方向に切り上げた端点からビンを作成
        min_edge = 0 if min_val < 0 else math.floor(min_val)
        max_edge = math.ceil(max_val)
        bin_edges = np.arange(min_edge, max_edge + 1, 1)  # 1刻みのビン端点
        print(f"bin_edges: {bin_edges}")

        # 度数分布表の作成
        df['bin'] = pd.cut(df['average'], bins=bin_edges, include_lowest=True)

        # 度数分布表の作成（昇順→降順に並べ替え）
        freq_table_asc = df['bin'].value_counts(sort=False)
        freq_table_desc = freq_table_asc.sort_index(ascending=False)
        freq_values_desc = freq_table_desc.values
        total_count = freq_values_desc.sum()

        return freq_table_desc, percent_ge_threshold
        
    except Exception as e:
        print(f"度数分布表作成中にエラーが発生しました: {str(e)}")
        return None, None

# ===== グラフ用データ処理関連 =====
def load_graph_data(year, month, day, data_dir="/home/shimada/analysis/output/dns_mag/"):
    """グラフ作成用のデータを読み込む"""
    try:
        csv_files = sorted(glob.glob(f"{data_dir}*.csv"))
        pattern = re.compile(rf"({year})-({month})-({day})\.csv")
        filtered_files = [file for file in csv_files if pattern.match(os.path.basename(file))]

        if not filtered_files:
            print("No CSV files found. Please ensure there are CSV files in the correct format.")
            return None
        
        # データフレームを全て読み込む
        data_frames = []
        for file in filtered_files:
            match = pattern.match(os.path.basename(file))
            if match:
                y, m, d = match.groups()
                try:
                    date_time = datetime.strptime(f"{y}-{m}-{d}", "%Y-%m-%d")
                    df = pd.read_csv(file, dtype=str, low_memory=False)
                    df["date_time"] = date_time
                    if "domain" in df.columns and "dnsmagnitude" in df.columns:
                        data_frames.append(df)
                    else:
                        print(f"Skipping file {file} due to missing required columns: 'domain' or 'dnsmagnitude'")
                except ValueError as e:
                    print(f"Skipping file {file} due to date parsing error: {e}")
        
        if not data_frames:
            print("No valid data frames to concatenate. Please check the CSV files.")
            return None
        
        # 全てのデータを結合する
        full_data = pd.concat(data_frames)
        return full_data
        
    except Exception as e:
        print(f"グラフデータ読み込み中にエラーが発生しました: {str(e)}")
        return None

def create_color_map(domains, colormap='tab20'):
    """ドメインごとに色を固定するためのカラーマップを作成"""
    import matplotlib.pyplot as plt
    color_map = {domain: plt.get_cmap(colormap)(i % 20) for i, domain in enumerate(domains)}
    return color_map

# ===== ユーティリティ関数 =====
def sort_lst(reader, lst):
    """一意にしてソートされたリストを作成"""
    import operator
    exist_set = set(lst)
    new_items = {row[0] for row in reader if row[0] not in exist_set}
    lst.extend(new_items)
    lst = sorted(lst, key=operator.itemgetter(0))
    return lst
