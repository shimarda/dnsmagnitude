# クエリ数による集計を行う
# クエリ数->csvファイルへのリスト作成
import csv
import os
import pandas as pd
import re
import glob

def file_lst(year, month, day):
    path = "/mnt/qnap2/shimada/input/*.csv"
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

def open_reader(year, month, day, hour):
    file_name = f"{year}-{month}-{day}-{hour}.csv"
    file_path = f"/mnt/qnap2/shimada/input/{file_name}"
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

def write_csv(dic, year, month, day):
    csv_file_path = f"/home/shimada/analysis/output-2025/{year}-{month}-{day}.csv"
    with open(csv_file_path, "w", newline='') as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow(['day', 'domain', 'dnsmagnitude'])
        for subdom, c in dic:
            writer.writerow([f"{day}", subdom, c])


def analyze_qtype_ratio(df, year, month, day):

    # サブドメインの抽出
    df['subdom'] = df['dns.qry.name'].apply(extract_subdomain)
    df_sub = df[df['subdom'].notnull()]

    subdomain_qtype_counts_hourly = df_sub.groupby(['subdom', 'dns.qry.type']).size().reset_index(name='count')
    for _, row in subdomain_qtype_counts_hourly.iterrows():
        subdom = row['subdom']
        qtype = row['dns.qry.type']
        count = row['count']
        daily_subdomain_qtype_counts = dict()
        if subdom not in daily_subdomain_qtype_counts:
            daily_subdomain_qtype_counts[subdom] = {}
        daily_subdomain_qtype_counts[subdom][qtype] = daily_subdomain_qtype_counts[subdom].get(qtype, 0) + count

    # # 結果をCSVファイルに書き出し
    # output_dir = "/home/shimada/analysis/output-2025"
    # os.makedirs(output_dir, exist_ok=True)
    # output_csv_path = os.path.join(output_dir, f"{year}-{month}-{day}-qtype_ratio.csv")

    # with open(output_csv_path, "w", newline='', encoding='utf-8') as f:
    #     writer = csv.writer(f)
    #     writer.writerow(['date', 'subdomain', 'qtype', 'count', 'ratio'])

    #     for subdom, qtype_counts in daily_subdomain_qtype_counts.items():
    #         total_queries_for_subdom = sum(qtype_counts.values())
    #         for qtype, count in qtype_counts.items():
    #             ratio = count / total_queries_for_subdom if total_queries_for_subdom > 0 else 0
    #             writer.writerow([f"{year}-{month}-{day}", subdom, qtype, count, f"{ratio:.4f}"])

    # print(f"Qtype ratio analysis for {year}-{month}-{day} completed and saved to {output_csv_path}")