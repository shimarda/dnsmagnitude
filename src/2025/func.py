import re
import pandas as pd
import os
import glob
import csv
import statistics
import argparse
import io

def file_lst(year, month, day, where):
    if int(where) == 0:
        path = "/mnt/qnap2/shimada/input/*.csv"
        print("権威")
    else:
        path = "/mnt/qnap2/shimada/resolver/*.csv"
        print("リゾルバ")
    pat = re.compile(rf"{year}-{month}-{day}-\d{{2}}\.csv")
    files = sorted(glob.glob(path))
    filtered_files = [file for file in files if pat.match(os.path.basename(file))]
    return filtered_files

# ファイル名リストから時間のみを抽出
def file_time(file_lst):
    time_list = [
        os.path.basename(f).replace('-', '').replace('.csv', '') for f in file_lst
    ]
    return time_list

def count_query(df, domain_dict):
    df['subdom'] = df['dns.qry.name'].apply(extract_subdomain)
    df_sub = df[df['subdom'].notnull()]
    
    hourly_counts = df_sub['subdom'].value_counts().to_dict()
    for dom, c in hourly_counts.items():
        domain_dict[dom] = domain_dict.get(dom, 0) + c

def write_csv(dic, year, month, day, where):
    csv_file_path = f"/home/shimada/analysis/output-2025/count-{where}-{year}-{month}-{day}.csv"
    with open(csv_file_path, "w", newline='') as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow(['day', 'domain', 'count'])
        for subdom, c in dic:
            writer.writerow([f"{day}", subdom, c])

# 問題のある行を検出する関数
def detect_problematic_rows(file_path, column_index=5):
    """
    CSVファイルの特定の列で混合データ型を持つ行を検出します。
    
    Args:
        file_path: CSVファイルのパス
        column_index: 確認する列のインデックス（デフォルトは5）
    
    Returns:
        problematic_rows: 問題のある行の内容とその行番号のリスト
    """
    problematic_rows = []
    
    try:
        # まず列名を取得
        with open(file_path, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
            headers = first_line.split(',')
            if len(headers) <= column_index:
                print(f"警告: 列インデックス {column_index} は列数 {len(headers)} を超えています")
                return problematic_rows
            
            column_name = headers[column_index]
            print(f"確認対象の列名: {column_name}")
        
        # 行ごとに読み込んで型を確認
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)  # ヘッダー行をスキップ
            
            # 最初の有効な値の型を記録
            first_type = None
            
            for row_num, row in enumerate(reader, start=2):  # ヘッダーが1行目なので2から始める
                if len(row) <= column_index:
                    problematic_rows.append((row_num, "列数が足りません", row))
                    continue
                
                cell_value = row[column_index]
                
                # 空の値はスキップ
                if not cell_value:
                    continue
                
                # 最初の有効な値の型を記録
                if first_type is None:
                    try:
                        # 数値に変換できるか試みる
                        float(cell_value)
                        first_type = "numeric"
                    except ValueError:
                        first_type = "string"
                else:
                    # 以降の値が最初の型と一致するか確認
                    current_type = None
                    try:
                        float(cell_value)
                        current_type = "numeric"
                    except ValueError:
                        current_type = "string"
                    
                    if current_type != first_type:
                        problematic_rows.append((row_num, f"型の不一致: 期待={first_type}, 実際={current_type}, 値={cell_value}", row))
        
        return problematic_rows
    
    except Exception as e:
        print(f"ファイル分析中にエラーが発生しました: {str(e)}")
        return problematic_rows

# 安全にファイルを開く関数
def open_reader_safe(year, month, day, hour, where):
    file_name = f"{year}-{month}-{day}-{hour}.csv"
    if int(where) == 0:
        file_path = f"/mnt/qnap2/shimada/input/{file_name}"
    else:
        file_path = f"/mnt/qnap2/shimada/resolver/{file_name}"
    
    try:
        # まず問題のある行を検出
        print(f"ファイル {file_path} の問題のある行を検出しています...")
        problematic_rows = detect_problematic_rows(file_path)
        
        if problematic_rows:
            print(f"ファイル {file_path} で {len(problematic_rows)} 個の問題のある行を検出しました")
            for row_num, reason, row in problematic_rows[:10]:  # 最初の10行だけ表示
                print(f"  行 {row_num}: {reason}")
                print(f"  内容: {','.join(row)}")
            
            if len(problematic_rows) > 10:
                print(f"  ... さらに {len(problematic_rows) - 10} 行の問題があります")
            
            # 問題のある行をログファイルに出力
            log_dir = "/home/shimada/analysis/logs"
            os.makedirs(log_dir, exist_ok=True)
            log_file = os.path.join(log_dir, f"problematic_rows_{year}_{month}_{day}_{hour}.csv")
            
            with open(log_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['Row', 'Reason', 'Content'])
                for row_num, reason, row in problematic_rows:
                    writer.writerow([row_num, reason, ','.join(row)])
            
            print(f"問題のある行の詳細は {log_file} に保存されました")
        
        # 安全に読み込む
        df = pd.read_csv(file_path, dtype={5: str}, low_memory=False)  # 列5を文字列として読み込む
        return df
    except Exception as e:
        print(f"ファイル {file_path} の読み込み中にエラーが発生しました: {str(e)}")
        return pd.DataFrame()  # 空のデータフレームを返す

def extract_subdomain(qname):
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

def qtype_ratio(year_pattern, month_pattern, day_pattern, where):
    all_files = file_lst(year_pattern, month_pattern, day_pattern, where)
    if not all_files:
        print(f"Warning: No files found for the given patterns (Y:{year_pattern}, M:{month_pattern}, D:{day_pattern}). Skipping analysis.")
        return

    # ファイルリストから日付（YYYY-MM-DD）を抽出し、ユニークな日付のリストを作成
    unique_dates = sorted(list(set([os.path.basename(f)[:10] for f in all_files])))  # 'YYYY-MM-DD'形式

    for date_str in unique_dates:
        current_year = date_str[:4]
        current_month = date_str[5:7]
        current_day = date_str[8:10]

        daily_subdomain_qtype_counts = {}  # 各日ごとの集計辞書を初期化

        # この日付に該当するファイルのみを抽出
        daily_files = [f for f in all_files if os.path.basename(f).startswith(date_str)]
        daily_hours = sorted(list(set([os.path.basename(f)[11:13] for f in daily_files])))  # ユニークな時間リスト

        if not daily_hours:
            print(f"No hourly data found for {date_str}. Skipping this date.")
            continue

        for hour_str in daily_hours:
            print(f"処理中: {date_str} {hour_str}:00")
            df = open_reader_safe(current_year, current_month, current_day, hour_str, where)
            if df.empty:
                continue

            # サブドメイン抽出前に型確認
            print(f"dns.qry.name の型: {df['dns.qry.name'].dtype}")
            print(f"dns.qry.type の型: {df['dns.qry.type'].dtype}")
            
            # 非文字列データの確認
            non_str_qname = df[~df['dns.qry.name'].apply(lambda x: isinstance(x, str))]
            if not non_str_qname.empty:
                print(f"文字列でない dns.qry.name の値: {len(non_str_qname)} 件")
                print(non_str_qname['dns.qry.name'].head())
            
            # サブドメイン抽出（NaN や非文字列値を安全に処理）
            df['subdom'] = df['dns.qry.name'].apply(lambda x: extract_subdomain(x) if pd.notnull(x) else None)
            
            # dns.qry.type と subdom の両方が null でない行のみをフィルタリング
            df_sub_filtered = df[df['subdom'].notnull() & df['dns.qry.type'].notnull()].copy()
            
            if df_sub_filtered.empty:
                print(f"  有効なデータなし: {date_str} {hour_str}")
                continue
            
            # 文字列として正規化
            df_sub_filtered['subdom'] = df_sub_filtered['subdom'].astype(str).str.strip()
            df_sub_filtered['dns.qry.type'] = df_sub_filtered['dns.qry.type'].astype(str).str.strip()
            
            # サブドメインとqtypeでグループ化し、カウント
            subdomain_qtype_counts_hourly = df_sub_filtered.groupby(['subdom', 'dns.qry.type']).size().reset_index(name='count')
            
            # 時間ごとの集計結果を日ごとの集計辞書に累積加算
            for _, row in subdomain_qtype_counts_hourly.iterrows():
                subdom = row['subdom']
                qtype = row['dns.qry.type']
                count = row['count']
                
                if subdom not in daily_subdomain_qtype_counts:
                    daily_subdomain_qtype_counts[subdom] = {}
                daily_subdomain_qtype_counts[subdom][qtype] = daily_subdomain_qtype_counts[subdom].get(qtype, 0) + count
        
        # 日ごとの結果をCSVファイルに書き出し
        output_dir = "/home/shimada/analysis/output-2025/qtype/"
        os.makedirs(output_dir, exist_ok=True)
        output_csv_path = os.path.join(output_dir, f"qtype-{where}-{date_str}.csv")
        
        with open(output_csv_path, "w", newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['date', 'subdomain', 'qtype', 'count', 'ratio'])
            
            for subdom, qtype_counts in daily_subdomain_qtype_counts.items():
                total_queries_for_subdom = sum(qtype_counts.values())
                
                # qtypeを数値としてソート（文字列の場合もあるので文字列としてソート）
                sorted_qtypes = sorted(qtype_counts.keys())
                
                for qtype in sorted_qtypes:
                    count = qtype_counts[qtype]
                    ratio = count / total_queries_for_subdom if total_queries_for_subdom > 0 else 0
                    writer.writerow([date_str, subdom, qtype, count, f"{ratio:.4f}"])
        
        print(f"Qtype ratio analysis for {date_str} completed and saved to {output_csv_path}")

def qtype_ratio_total(year_pattern, month_pattern, day_pattern, where):
    """
    対象期間全体のトラフィックでqtypeの比率を計算する関数
    
    Args:
        year_pattern: 年のパターン（例: "2025"）
        month_pattern: 月のパターン（例: "01"）
        day_pattern: 日のパターン（例: "15"）
        where: 0=権威サーバー、1=リゾルバー
    """
    all_files = file_lst(year_pattern, month_pattern, day_pattern, where)
    if not all_files:
        print(f"Warning: No files found for the given patterns (Y:{year_pattern}, M:{month_pattern}, D:{day_pattern}). Skipping analysis.")
        return

    # 全期間の集計用辞書
    total_qtype_counts = {}
    
    # ファイルリストから日付（YYYY-MM-DD）を抽出し、ユニークな日付のリストを作成
    unique_dates = sorted(list(set([os.path.basename(f)[:10] for f in all_files])))
    
    print(f"Processing {len(unique_dates)} unique dates...")
    
    for date_str in unique_dates:
        current_year = date_str[:4]
        current_month = date_str[5:7]
        current_day = date_str[8:10]
        
        # この日付に該当するファイルのみを抽出
        daily_files = [f for f in all_files if os.path.basename(f).startswith(date_str)]
        daily_hours = sorted(list(set([os.path.basename(f)[11:13] for f in daily_files])))
        
        if not daily_hours:
            print(f"No hourly data found for {date_str}. Skipping this date.")
            continue
        
        for hour_str in daily_hours:
            print(f"処理中: {date_str} {hour_str}:00")
            df = open_reader_safe(current_year, current_month, current_day, hour_str, where)
            if df.empty:
                continue
            
            # サブドメイン抽出前に型確認
            print(f"dns.qry.name の型: {df['dns.qry.name'].dtype}")
            print(f"dns.qry.type の型: {df['dns.qry.type'].dtype}")
            
            # 非文字列データの確認
            non_str_qname = df[~df['dns.qry.name'].apply(lambda x: isinstance(x, str))]
            if not non_str_qname.empty:
                print(f"文字列でない dns.qry.name の値: {len(non_str_qname)} 件")
                print(non_str_qname['dns.qry.name'].head())
            
            # サブドメイン抽出（NaN や非文字列値を安全に処理）
            df['subdom'] = df['dns.qry.name'].apply(lambda x: extract_subdomain(x) if pd.notnull(x) else None)
            
            # dns.qry.type と subdom の両方が null でない行のみをフィルタリング
            df_sub_filtered = df[df['subdom'].notnull() & df['dns.qry.type'].notnull()].copy()
            
            if df_sub_filtered.empty:
                print(f"  有効なデータなし: {date_str} {hour_str}")
                continue
            
            # 文字列として正規化
            df_sub_filtered['dns.qry.type'] = df_sub_filtered['dns.qry.type'].astype(str).str.strip()
            
            # qtypeでグループ化し、カウント（サブドメイン関係なく全体で集計）
            qtype_counts_hourly = df_sub_filtered['dns.qry.type'].value_counts().to_dict()
            
            # 時間ごとの集計結果を全体の集計辞書に累積加算
            for qtype, count in qtype_counts_hourly.items():
                total_qtype_counts[qtype] = total_qtype_counts.get(qtype, 0) + count
    
    # 結果をCSVファイルに書き出し
    output_dir = "/home/shimada/analysis/output-2025/qtype/"
    os.makedirs(output_dir, exist_ok=True)
    
    # 期間の表現を作成
    period_str = f"{year_pattern}-{month_pattern}-{day_pattern}"
    output_csv_path = os.path.join(output_dir, f"qtype-total-{where}-{period_str}.csv")
    
    # 総クエリ数を計算
    total_queries = sum(total_qtype_counts.values())
    
    if total_queries == 0:
        print("No valid queries found in the specified period.")
        return
    
    with open(output_csv_path, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['period', 'qtype', 'count', 'ratio'])
        
        # qtypeを数値としてソート（文字列の場合もあるので文字列としてソート）
        sorted_qtypes = sorted(total_qtype_counts.keys())
        
        for qtype in sorted_qtypes:
            count = total_qtype_counts[qtype]
            ratio = count / total_queries
            writer.writerow([period_str, qtype, count, f"{ratio:.6f}"])
    
    print(f"Total qtype ratio analysis for period {period_str} completed.")
    print(f"Total queries processed: {total_queries:,}")
    print(f"Results saved to: {output_csv_path}")
    
    # 上位qtypeの簡単な統計を表示
    sorted_by_count = sorted(total_qtype_counts.items(), key=lambda x: x[1], reverse=True)
    print("\n上位10のqtype:")
    for i, (qtype, count) in enumerate(sorted_by_count[:10], 1):
        ratio = count / total_queries
        print(f"{i:2d}. {qtype:>6s}: {count:>10,} ({ratio:6.2%})")


def qtype_ratio_total_by_date_range(start_date, end_date, where):
    """
    日付範囲を指定して全期間のqtype比率を計算する関数
    
    Args:
        start_date: 開始日（例: "2025-01-01"）
        end_date: 終了日（例: "2025-01-31"）
        where: 0=権威サーバー、1=リゾルバー
    """
    from datetime import datetime, timedelta
    
    # 日付文字列をdatetimeオブジェクトに変換
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    # 全期間の集計用辞書
    total_qtype_counts = {}
    total_processed_files = 0
    
    print(f"Processing date range: {start_date} to {end_date}")
    
    # 日付範囲内の各日を処理
    current_dt = start_dt
    while current_dt <= end_dt:
        date_str = current_dt.strftime("%Y-%m-%d")
        year = current_dt.strftime("%Y")
        month = current_dt.strftime("%m")
        day = current_dt.strftime("%d")
        
        # この日のファイルを取得
        daily_files = file_lst(year, month, day, where)
        
        if not daily_files:
            print(f"No files found for {date_str}")
            current_dt += timedelta(days=1)
            continue
        
        # 時間ごとのファイルを処理
        daily_hours = sorted(list(set([os.path.basename(f)[11:13] for f in daily_files])))
        
        for hour_str in daily_hours:
            print(f"処理中: {date_str} {hour_str}:00")
            df = open_reader_safe(year, month, day, hour_str, where)
            if df.empty:
                continue
            
            total_processed_files += 1
            
            # サブドメイン抽出
            df['subdom'] = df['dns.qry.name'].apply(lambda x: extract_subdomain(x) if pd.notnull(x) else None)
            
            # 有効なデータをフィルタリング
            df_sub_filtered = df[df['subdom'].notnull() & df['dns.qry.type'].notnull()].copy()
            
            if df_sub_filtered.empty:
                print(f"  有効なデータなし: {date_str} {hour_str}")
                continue
            
            # 文字列として正規化
            df_sub_filtered['dns.qry.type'] = df_sub_filtered['dns.qry.type'].astype(str).str.strip()
            
            # qtypeでグループ化し、カウント
            qtype_counts_hourly = df_sub_filtered['dns.qry.type'].value_counts().to_dict()
            
            # 全体の集計辞書に累積加算
            for qtype, count in qtype_counts_hourly.items():
                total_qtype_counts[qtype] = total_qtype_counts.get(qtype, 0) + count
        
        current_dt += timedelta(days=1)
    
    # 結果をCSVファイルに書き出し
    output_dir = "/home/shimada/analysis/output-2025/qtype/"
    os.makedirs(output_dir, exist_ok=True)
    
    period_str = f"{start_date}_to_{end_date}"
    output_csv_path = os.path.join(output_dir, f"qtype-total-{where}-{period_str}.csv")
    
    # 総クエリ数を計算
    total_queries = sum(total_qtype_counts.values())
    
    if total_queries == 0:
        print("No valid queries found in the specified period.")
        return
    
    with open(output_csv_path, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['period', 'qtype', 'count', 'ratio'])
        
        # qtypeを文字列としてソート
        sorted_qtypes = sorted(total_qtype_counts.keys())
        
        for qtype in sorted_qtypes:
            count = total_qtype_counts[qtype]
            ratio = count / total_queries
            writer.writerow([period_str, qtype, count, f"{ratio:.6f}"])
    
    print(f"Total qtype ratio analysis for period {period_str} completed.")
    print(f"Total files processed: {total_processed_files}")
    print(f"Total queries processed: {total_queries:,}")
    print(f"Results saved to: {output_csv_path}")
    
    # 上位qtypeの統計を表示
    sorted_by_count = sorted(total_qtype_counts.items(), key=lambda x: x[1], reverse=True)
    print("\n上位10のqtype:")
    for i, (qtype, count) in enumerate(sorted_by_count[:10], 1):
        ratio = count / total_queries
        print(f"{i:2d}. {qtype:>6s}: {count:>10,} ({ratio:6.2%})")