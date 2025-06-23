# クエリ数による集計を行う
# クエリ数->csvファイルへのリスト作成
import csv
import os
import pandas as pd
import re
import glob
import statistics

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
    file_dic = dict()

    time_list = [
        os.path.basename(f).replace('-', '').replace('.csv', '') for f in file_lst
    ]

    for time in time_list:
        month = time[4:6]
        day = time[6:8]
        hour = time[8:10]
        if day not in file_dic.keys():
            file_dic[day] = list()
        file_dic[day].append(hour)
    return file_dic

def open_reader(year, month, day, hour, where):
    file_name = f"{year}-{month}-{day}-{hour}.csv"
    if int(where) == 0:
        file_path = f"/mnt/qnap2/shimada/input/{file_name}"
    else:
        file_path = f"/mnt/qnap2/shimada/resolver/{file_name}"
    df = pd.read_csv(file_path)
    return df

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

def count_query(df, domain_dict):
    df['subdom'] = df['dns.qry.name'].apply(extract_subdomain)
    df_sub = df[df['subdom'].notnull()]
    
    hourly_counts = df_sub['subdom'].value_counts().to_dict()
    for dom, c in hourly_counts.items():
        domain_dict[dom] = domain_dict.get(dom, 0) + c

def write_csv(dic, year, month, day, where):
    csv_file_path = f"/home/shimada/analysis/output-2025/{where}-{year}-{month}-{day}.csv"
    with open(csv_file_path, "w", newline='') as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow(['day', 'domain', 'count'])
        for subdom, c in dic:
            writer.writerow([f"{day}", subdom, c])


def cal_average(dom_dic, total_dic, file_lst):
    all_dom_set = set()
    for dom in dom_dic:
        total_dic[dom] = list()
    

    for file in file_lst:
        df = pd.read_csv(file)
        domain_to_val = dict(zip(df['domain'], df['count']))

        for dom in all_dom_set:
            if dom in domain_to_val:
                total_dic[dom].append(float(domain_to_val[dom]))
            else:
                total_dic[dom].append(0.0)
    
    ave_dic = {dom: statistics.mean(vals) for dom, vals in total_dic.items()}
    ave_dic = dict(sorted(ave_dic.items(), key=lambda item: item[1], reverse=True))

    return ave_dic

def qtype_ratio(year_pattern, month_pattern, day_pattern, where):

    all_files = file_lst(year_pattern, month_pattern, day_pattern, where)
    if not all_files:
        print(f"Warning: No files found for the given patterns (Y:{year_pattern}, M:{month_pattern}, D:{day_pattern}). Skipping analysis.")
        return

    # ファイルリストから日付（YYYY-MM-DD）を抽出し、ユニークな日付のリストを作成
    unique_dates = sorted(list(set([os.path.basename(f)[:10] for f in all_files]))) # 'YYYY-MM-DD'形式

    for date_str in unique_dates:
        current_year = date_str[:4]
        current_month = date_str[5:7]
        current_day = date_str[8:10]

        daily_subdomain_qtype_counts = {} # 各日ごとの集計辞書を初期化

        # この日付に該当するファイルのみを抽出
        daily_files = [f for f in all_files if os.path.basename(f).startswith(date_str)]
        daily_hours = sorted(list(set([os.path.basename(f)[11:13] for f in daily_files]))) # ユニークな時間リスト

        if not daily_hours:
            print(f"No hourly data found for {date_str}. Skipping this date.")
            continue

        for hour_str in daily_hours:
            # print(f"  Processing hour: {hour_str}") # デバッグ用
            df = open_reader(current_year, current_month, current_day, hour_str)
            if df.empty:
                continue

            df['subdom'] = df['dns.qry.name'].apply(extract_subdomain)
            
            # dns.qry.type と subdom の両方が null でない行のみをフィルタリングし、文字列として正規化
            df_sub_filtered = df[df['subdom'].notnull() & df['dns.qry.type'].notnull()].copy() # SettingWithCopyWarning回避のため.copy()

            # subdom と dns.qry.type の値を正規化 (前後の空白を除去)
            df_sub_filtered['subdom'] = df_sub_filtered['subdom'].astype(str).str.strip()
            df_sub_filtered['dns.qry.type'] = df_sub_filtered['dns.qry.type'].astype(str).str.strip()

            if df_sub_filtered.empty:
                continue

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
        output_dir = "/home/shimada/analysis/output-2025"
        os.makedirs(output_dir, exist_ok=True)
        output_csv_path = os.path.join(output_dir, f"{where}-{date_str}-qtype_ratio.csv")

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
