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
import ipaddress
from collections import defaultdict
from datetime import datetime, timedelta

# ===== 共通設定 =====
OUTPUT_BASE_DIR = "/home/shimada/output"

def ensure_output_dir(subdir=""):
    """出力ディレクトリを作成し、パスを返す"""
    if subdir:
        output_dir = os.path.join(OUTPUT_BASE_DIR, subdir)
    else:
        output_dir = OUTPUT_BASE_DIR
    
    os.makedirs(output_dir, exist_ok=True)
    return output_dir

# ===== ファイル操作関連 =====
def file_lst(input_directory):
    """指定されたディレクトリ内のCSVファイルリストを取得"""
    csv_files = glob.glob(os.path.join(input_directory, "*.csv"))
    return sorted(csv_files)

def safe_read_csv(file_path, encoding='utf-8'):
    """CSVファイルを安全に読み込み、エラー行をスキップ"""
    try:
        # pandasでCSVを読み込み、エラー行は自動的にスキップ
        df = pd.read_csv(file_path, encoding=encoding, on_bad_lines='skip')
        return df, []  # エラーがない場合は空のリストを返す
    except Exception as e:
        print(f"ファイル {file_path} の読み込み中にエラーが発生しました: {str(e)}")
        return pd.DataFrame(), []

def load_csv_with_error_handling(file_path):
    """エラーハンドリング付きでCSVファイルを読み込み"""
    try:
        df = pd.read_csv(file_path, dtype=str, low_memory=False)
        return df, []
    except Exception as e:
        print(f"ファイル読み込みエラー ({file_path}): {str(e)}")
        return pd.DataFrame(), []

# ===== データ処理関連 =====
def extract_subdomain(domain_name):
    """ドメイン名から最上位レベルのサブドメインを抽出
    
    例:
    aa.bb.cc.dd.tsukuba.ac.jp -> dd
    aa.bb.ss.dd.tsukuba.ac.jp -> dd
    www.tsukuba.ac.jp -> www
    """
    if pd.isna(domain_name) or domain_name == '':
        return None
    
    # .tsukuba.ac.jpで終わるかチェック
    if not str(domain_name).endswith('.tsukuba.ac.jp'):
        return None
    
    # .tsukuba.ac.jpを除去してサブドメイン部分を取得
    subdomain_part = str(domain_name).replace('.tsukuba.ac.jp', '')
    
    # 空文字列や無効な場合はNoneを返す
    if subdomain_part == '' or subdomain_part == '.':
        return None
    
    # ドットで分割して最後の部分（最上位レベルサブドメイン）を取得
    subdomain_parts = subdomain_part.split('.')
    if len(subdomain_parts) == 0:
        return None
    
    # 最後の部分が最上位レベルサブドメイン
    top_level_subdomain = subdomain_parts[-1]
    
    # 空文字列チェック
    if top_level_subdomain == '':
        return None
    
    return top_level_subdomain

def calculate_qtype_average_ratios(file_list, output_csv_path):
    """複数ファイルのqtype比率の平均を計算"""
    qtype_data = defaultdict(list)
    
    for file_path in file_list:
        df, _ = load_csv_with_error_handling(file_path)
        if df.empty:
            continue
        
        # qtypeカラムの存在確認
        if 'qtype' not in df.columns:
            print(f"警告: {file_path} にqtypeカラムが見つかりません")
            continue
        
        # 有効なqtypeのみを取得
        valid_qtypes = df['qtype'].dropna()
        if len(valid_qtypes) == 0:
            continue
        
        # qtype別の件数をカウント
        qtype_counts = valid_qtypes.value_counts()
        total_count = len(valid_qtypes)
        
        # 比率を計算
        for qtype, count in qtype_counts.items():
            ratio = count / total_count
            qtype_data[qtype].append(ratio)
    
    # 平均比率を計算
    average_ratios = {}
    for qtype, ratios in qtype_data.items():
        if ratios:
            average_ratios[qtype] = sum(ratios) / len(ratios)
    
    # 結果をCSVに保存
    output_dir = os.path.dirname(output_csv_path)
    os.makedirs(output_dir, exist_ok=True)
    
    with open(output_csv_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['qtype', 'average_ratio'])
        for qtype, ratio in sorted(average_ratios.items(), key=lambda x: x[1], reverse=True):
            writer.writerow([qtype, f"{ratio:.6f}"])
    
    print(f"qtype平均比率をCSVに保存しました: {output_csv_path}")
    return average_ratios

def qtype_ratio(df):
    """DataFrameからqtype比率を計算"""
    if df.empty or 'qtype' not in df.columns:
        return {}
    
    valid_qtypes = df['qtype'].dropna()
    if len(valid_qtypes) == 0:
        return {}
    
    qtype_counts = valid_qtypes.value_counts()
    total_count = len(valid_qtypes)
    
    return {qtype: count/total_count for qtype, count in qtype_counts.items()}

# ===== DNS Magnitude関連 =====
def calculate_dns_magnitude(df, date_str):
    """DNS Magnitudeを計算"""
    if df.empty:
        return {}
    
    # サブドメインを抽出
    df['subdomain'] = df['qname'].apply(extract_subdomain)
    
    # 有効なサブドメインのみをフィルタ
    valid_df = df[df['subdomain'].notna()].copy()
    
    if valid_df.empty:
        print(f"有効なサブドメインデータが見つかりませんでした: {date_str}")
        return {}
    
    # 全ユニークIPアドレス
    unique_ips = set(valid_df['ip'].unique())
    A_tot = len(unique_ips)
    
    if A_tot == 0:
        return {}
    
    # サブドメインごとのIPアドレス集計
    domain_ip_dict = valid_df.groupby('subdomain')['ip'].apply(set).to_dict()
    
    # マグニチュード計算
    magnitude_dict = {}
    for domain, ip_set in domain_ip_dict.items():
        ip_count = len(ip_set)
        if ip_count > 0:
            magnitude = 10 * math.log(ip_count) / math.log(A_tot)
            magnitude_dict[domain] = magnitude
    
    # 降順ソート
    return dict(sorted(magnitude_dict.items(), key=lambda item: item[1], reverse=True))

def write_magnitude_csv(magnitude_dict, date_str, output_csv_path):
    """マグニチュード結果をCSVに書き込み"""
    output_dir = os.path.dirname(output_csv_path)
    os.makedirs(output_dir, exist_ok=True)
    
    with open(output_csv_path, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['date', 'subdomain', 'magnitude'])
        
        for subdomain, magnitude in magnitude_dict.items():
            writer.writerow([date_str, subdomain, f"{magnitude:.6f}"])
    
    print(f"マグニチュード結果を保存しました: {output_csv_path}")

# ===== エラーハンドリング関連 =====
def write_error_log(total_error_lines, file_error_counts, error_log_file):
    """エラーログを出力"""
    log_dir = ensure_output_dir("logs")
    log_path = os.path.join(log_dir, error_log_file)
    
    with open(log_path, 'w', encoding='utf-8') as log_file:
        log_file.write("=== エラー行の詳細 ===\n")
        for file_name, line_num, line_content in total_error_lines:
            log_file.write(f"ファイル: {file_name}, 行番号: {line_num}\n")
            log_file.write(f"行内容: {line_content}\n")
            log_file.write("-" * 80 + "\n")
        
        log_file.write("\n=== ファイルごとのエラー行数 ===\n")
        for file_name, count in file_error_counts.items():
            log_file.write(f"{file_name}: {count}行\n")
        
        log_file.write(f"\n総エラー行数: {len(total_error_lines)}\n")
    
    print(f"詳細なエラーログは {log_path} に保存されました")

# ===== 学内・学外分類とクエリ・レスポンス分析関連 =====

def classify_ip_address(ip_str):
    """IPアドレスを学内・学外で分類"""
    try:
        ip = ipaddress.ip_address(ip_str)
        
        # 学内者用ネットワーク: 133.51.112.0/20
        internal_network = ipaddress.ip_network('133.51.112.0/20')
        # 学外者用ネットワーク: 133.51.192.0/21  
        external_network = ipaddress.ip_network('133.51.192.0/21')
        
        if ip in internal_network:
            return "internal"  # 学内者
        elif ip in external_network:
            return "external"  # 学外者
        else:
            return "other"     # その他
    except (ipaddress.AddressValueError, ValueError):
        return "invalid"

def load_query_response_csv(file_path):
    """クエリ・レスポンス用CSVファイルを安全に読み込み"""
    try:
        # 必要な列を指定して読み込み
        required_columns = [
            'frame.time', 'ip.src', 'ip.dst', 'dns.qry.name', 
            'dns.qry.type', 'dns.flags.response', 'vlan.id', 
            'dns.flags.rcode', 'dns.flags.authoritative'
        ]
        
        df = pd.read_csv(file_path, dtype=str, low_memory=False)
        
        # 必要な列が存在するかチェック
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"警告: 以下の列が見つかりません: {missing_columns}")
        
        return df
    except Exception as e:
        print(f"ファイル {file_path} の読み込み中にエラーが発生しました: {str(e)}")
        return pd.DataFrame()

def filter_query_response_data(df, analysis_type="query"):
    """クエリまたはレスポンスでデータをフィルタリング"""
    if df.empty:
        return df
    
    # dns.flags.responseカラムが存在するかチェック
    if 'dns.flags.response' not in df.columns:
        print("警告: dns.flags.response列が見つかりません")
        return df
    
    # NaN値を処理
    df_filtered = df.copy()
    df_filtered['dns.flags.response'] = df_filtered['dns.flags.response'].fillna('0')
    
    if analysis_type == "query":
        # クエリ（dns.flags.response = 0）
        result_df = df_filtered[df_filtered['dns.flags.response'] == '0'].copy()
        print(f"クエリデータ: {len(result_df)}件")
    elif analysis_type == "response":
        # レスポンス（dns.flags.response = 1）でrcode = 0のもの
        response_df = df_filtered[df_filtered['dns.flags.response'] == '1'].copy()
        
        # rcodeが0のもののみ（NOERROR）
        if 'dns.flags.rcode' in response_df.columns:
            response_df['dns.flags.rcode'] = response_df['dns.flags.rcode'].fillna('1')  # NaNは1（エラー）として扱う
            result_df = response_df[response_df['dns.flags.rcode'] == '0'].copy()
            print(f"レスポンスデータ（rcode=0）: {len(result_df)}件")
        else:
            print("警告: dns.flags.rcode列が見つかりません")
            result_df = response_df
            print(f"レスポンスデータ（rcode未確認）: {len(result_df)}件")
    else:
        print(f"不正な分析タイプ: {analysis_type}")
        return pd.DataFrame()
    
    return result_df

def classify_by_network_and_calculate_magnitude(df):
    """ネットワーク分類してマグニチュードを計算（応答パケット用）"""
    if df.empty:
        return {}
    
    # 応答パケットなので送信先IPアドレス（ip.dst）を分析対象とする
    target_ip_column = "ip.dst"
    
    if target_ip_column not in df.columns:
        print(f"警告: {target_ip_column}列が見つかりません")
        return {}
    
    # IPアドレス分類を追加
    df['network_type'] = df[target_ip_column].apply(classify_ip_address)
    
    # サブドメイン抽出
    df['subdomain'] = df['dns.qry.name'].apply(lambda x: extract_subdomain(x) if pd.notnull(x) else None)
    
    # 有効なサブドメインのみ
    df_valid = df[df['subdomain'].notnull()].copy()
    
    if df_valid.empty:
        print("有効なサブドメインデータが見つかりませんでした")
        return {}
    
    # ネットワークタイプ別の結果
    results = {}
    
    for network_type in ['internal', 'external', 'other']:
        network_df = df_valid[df_valid['network_type'] == network_type].copy()
        
        if network_df.empty:
            print(f"{network_type}ネットワークのデータが見つかりませんでした")
            results[network_type] = {}
            continue
        
        print(f"{network_type}ネットワーク: {len(network_df)}件")
        
        # 全ユニークIPアドレス
        unique_ips = set(network_df[target_ip_column].unique())
        A_tot = len(unique_ips)
        
        if A_tot == 0:
            results[network_type] = {}
            continue
        
        # サブドメインごとのIPアドレス集計
        domain_ip_dict = network_df.groupby('subdomain')[target_ip_column].apply(set).to_dict()
        
        # マグニチュード計算
        magnitude_dict = {}
        for domain, ip_set in domain_ip_dict.items():
            ip_count = len(ip_set)
            if ip_count > 0 and A_tot > 0:
                magnitude = 10 * math.log(ip_count) / math.log(A_tot)
                magnitude_dict[domain] = magnitude
        
        # 降順ソート
        results[network_type] = dict(sorted(magnitude_dict.items(), key=lambda item: item[1], reverse=True))
        
        print(f"{network_type}ネットワーク - 総IP数: {A_tot}, ドメイン数: {len(magnitude_dict)}")
    
    return results
    
def write_network_magnitude_csv(results, date_str):
    """ネットワーク別マグニチュード結果をCSVに書き込み"""
    import csv
    
    # 出力ディレクトリ作成
    output_dir = "output/network_analysis"
    os.makedirs(output_dir, exist_ok=True)
    
    for network_type, magnitude_dict in results.items():
        if not magnitude_dict:
            continue
        
        filename = f"magnitude-{network_type}-{date_str}.csv"
        output_path = os.path.join(output_dir, filename)
        
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['network_type', 'date', 'subdomain', 'magnitude'])
            
            for subdomain, magnitude in magnitude_dict.items():
                writer.writerow([network_type, date_str, subdomain, f"{magnitude:.6f}"])
        
        print(f"結果を保存: {output_path} ({len(magnitude_dict)}件)")


def process_network_analysis_files(year, month, day, analysis_type="query", 
                                 input_dir="/mnt/qnap2/shimada/resolver/"):
    """ネットワーク分析のメイン処理"""
    # パターンに一致するファイルを検索
    pattern = re.compile(rf"{year}-{month}-{day}-\d{{2}}\.csv")
    files = sorted(glob.glob(os.path.join(input_dir, "*.csv")))
    filtered_files = [file for file in files if pattern.match(os.path.basename(file))]
    
    if not filtered_files:
        print(f"対象ファイルが見つかりませんでした: {year}-{month}-{day}")
        return
    
    print(f"処理対象ファイル数: {len(filtered_files)}")
    
    # ファイルリストから日付を抽出し、ユニークな日付のリストを作成
    unique_dates = sorted(list(set([os.path.basename(f)[:10] for f in filtered_files])))
    
    for date_str in unique_dates:
        print(f"\n=== 処理中: {date_str} ({analysis_type}) ===")
        
        # この日付に該当するファイルのみを抽出
        daily_files = [f for f in filtered_files if os.path.basename(f).startswith(date_str)]
        
        # 1日分のデータを結合
        daily_dataframes = []
        for file_path in daily_files:
            print(f"読み込み中: {os.path.basename(file_path)}")
            df = load_query_response_csv(file_path)
            if not df.empty:
                daily_dataframes.append(df)
        
        if not daily_dataframes:
            print(f"有効なデータが見つかりませんでした: {date_str}")
            continue
        
        # データを結合
        combined_df = pd.concat(daily_dataframes, ignore_index=True)
        print(f"結合後データ数: {len(combined_df)}件")
        
        # クエリ/レスポンスでフィルタリング
        filtered_df = filter_query_response_data(combined_df, analysis_type)
        
        if filtered_df.empty:
            print(f"フィルタリング後のデータが見つかりませんでした: {date_str}")
            continue
        
        # ネットワーク分類とマグニチュード計算
        results = classify_by_network_and_calculate_magnitude(filtered_df, analysis_type)
        
        # 結果をCSVに出力
        write_network_magnitude_csv(results, date_str, analysis_type)

# ===== ネットワーク分析統計関連 =====

def load_magnitude_csv_files(output_dir, pattern):
    """マグニチュードCSVファイルを読み込み、統合"""
    import glob
    
    csv_files = sorted(glob.glob(os.path.join(output_dir, pattern)))
    if not csv_files:
        return pd.DataFrame()
    
    all_data = []
    for file_path in csv_files:
        try:
            df = pd.read_csv(file_path)
            all_data.append(df)
        except Exception as e:
            print(f"ファイル読み込みエラー ({file_path}): {str(e)}")
    
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    else:
        return pd.DataFrame()

def calculate_magnitude_statistics(df, network_type, analysis_type):
    """マグニチュード統計を計算"""
    if df.empty:
        return {}
    
    # 指定されたネットワークタイプと分析タイプでフィルタ
    filtered_df = df[
        (df['network_type'] == network_type) & 
        (df['analysis_type'] == analysis_type)
    ].copy()
    
    if filtered_df.empty:
        return {}
    
    # サブドメインごとの統計を計算
    stats_results = {}
    
    for subdomain in filtered_df['subdomain'].unique():
        subdomain_df = filtered_df[filtered_df['subdomain'] == subdomain]
        magnitudes = subdomain_df['magnitude'].astype(float)
        
        if len(magnitudes) == 0:
            continue
        
        # 統計計算
        stats = {
            'count': len(magnitudes),
            'mean': magnitudes.mean(),
            'std': magnitudes.std(),
            'var': magnitudes.var(),
            'min': magnitudes.min(),
            'max': magnitudes.max(),
            'median': magnitudes.median(),
            'q25': magnitudes.quantile(0.25),
            'q75': magnitudes.quantile(0.75)
        }
        
        # NaN値の処理
        for key, value in stats.items():
            if pd.isna(value):
                stats[key] = 0.0 if key != 'count' else 0
        
        stats_results[subdomain] = stats
    
    return stats_results

def write_magnitude_statistics_csv(stats_results, network_type, start_date, end_date, output_dir):
    """マグニチュード統計結果をCSVに書き込み"""
    import csv
    
    if not stats_results:
        print(f"統計データがありません: {network_type}")
        return
    
    filename = f"magnitude-stats-{network_type}-{start_date}-to-{end_date}.csv"
    output_path = os.path.join(output_dir, filename)
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'network_type', 'subdomain', 'count', 
            'mean', 'std', 'var', 'min', 'max', 'median', 'q25', 'q75',
            'start_date', 'end_date'
        ])
        
        for subdomain, stats in sorted(stats_results.items(), 
                                      key=lambda x: x[1]['mean'], reverse=True):
            writer.writerow([
                network_type, subdomain, stats['count'],
                f"{stats['mean']:.6f}", f"{stats['std']:.6f}", 
                f"{stats['var']:.6f}", f"{stats['min']:.6f}",
                f"{stats['max']:.6f}", f"{stats['median']:.6f}",
                f"{stats['q25']:.6f}", f"{stats['q75']:.6f}",
                start_date, end_date
            ])
    
    print(f"統計結果を保存: {output_path}")

def process_network_analysis_files(year, month, day, input_dir="/mnt/qnap2/shimada/resolver/"):
    """ネットワーク分析のメイン処理"""
    # パターンに一致するファイルを検索
    pattern = re.compile(rf"{year}-{month}-{day}-\d{{2}}\.csv")
    files = sorted(glob.glob(os.path.join(input_dir, "*.csv")))
    filtered_files = [file for file in files if pattern.match(os.path.basename(file))]
    
    if not filtered_files:
        print(f"対象ファイルが見つかりませんでした: {year}-{month}-{day}")
        return
    
    print(f"処理対象ファイル数: {len(filtered_files)}")
    
    # ファイルリストから日付を抽出し、ユニークな日付のリストを作成
    unique_dates = sorted(list(set([os.path.basename(f)[:10] for f in filtered_files])))
    
    for date_str in unique_dates:
        print(f"\n=== 処理中: {date_str} ===")
        
        # この日付に該当するファイルのみを抽出
        daily_files = [f for f in filtered_files if os.path.basename(f).startswith(date_str)]
        
        # 1日分のデータを結合
        daily_dataframes = []
        for file_path in daily_files:
            print(f"読み込み中: {os.path.basename(file_path)}")
            df = load_response_csv(file_path)
            if not df.empty:
                daily_dataframes.append(df)
        
        if not daily_dataframes:
            print(f"有効なデータが見つかりませんでした: {date_str}")
            continue
        
        # データを結合
        combined_df = pd.concat(daily_dataframes, ignore_index=True)
        print(f"結合後データ数: {len(combined_df)}件（すべて応答パケット）")
        
        if combined_df.empty:
            print(f"データが見つかりませんでした: {date_str}")
            continue
        
        # ネットワーク分類とマグニチュード計算
        results = classify_by_network_and_calculate_magnitude(combined_df)
        
        # 結果をCSVに出力
        write_network_magnitude_csv(results, date_str)

def load_response_csv(file_path):
    """応答パケット用CSVファイルを安全に読み込み"""
    try:
        # 必要な列を指定して読み込み
        required_columns = [
            'frame.time', 'ip.src', 'ip.dst', 'ipv6.dst', 'dns.qry.name', 'dns.qry.type'
        ]
        
        df = pd.read_csv(file_path, dtype=str, low_memory=False)
        
        # 必要な列が存在するかチェック
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"警告: 以下の列が見つかりません: {missing_columns}")
        
        return df
    except Exception as e:
        print(f"ファイル {file_path} の読み込み中にエラーが発生しました: {str(e)}")
        return pd.DataFrame()
