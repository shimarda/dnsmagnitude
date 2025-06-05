import re
import pandas as pd
import os
import glob
import csv
import math
import operator
import argparse

# ファイル名からファイルを開く
def open_reader(file_name):
    # 入力ファイルは /mnt/qnap2/shimada/input/ 配下にあるとする
    file_path = f"/mnt/qnap2/shimada/input/{file_name}"
    df = pd.read_csv(file_path)
    return df

# パターンに合うファイル名リストを取得
def file_lst(year, month, day):
    # 入力ディレクトリのパス
    path = "/mnt/qnap2/shimada/input/*.csv"
    # 例: "2025-02-07-13.csv" のような形式にマッチ
    pattern = re.compile(rf"{year}-{month}-{day}-\d{{2}}\.csv")
    files = sorted(glob.glob(path))
    filtered_files = [file for file in files if pattern.match(os.path.basename(file))]
    return filtered_files

# ファイル名から年月日と時間を抽出して、キーを年月日、値を時間リストとする辞書を返す
def file_time(file_lst):
    """
    ファイル名の例: "2025-02-07-13.csv"  
    キーを "YYYY-MM-DD"、値を [HH, ...] とした辞書を返す。
    """
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

# 一意にしてソートされたリストを作成（※必要に応じて使用）
def sort_lst(reader, lst):
    exist_set = set(lst)
    new_items = {row[0] for row in reader if row[0] not in exist_set}
    lst.extend(new_items)
    lst = sorted(lst, key=operator.itemgetter(0))
    return lst

# サブドメインを抽出する関数
def extract_subdomain(qname):
    suffix = '.tsukuba.ac.jp'
    if isinstance(qname, str):
        qname_lower = qname.lower()
        if qname_lower.endswith(suffix):
            # サフィックスを取り除く
            qname_no_suffix = qname_lower[:-len(suffix)]
            if qname_no_suffix.endswith('.'):
                qname_no_suffix = qname_no_suffix[:-1]
            if qname_no_suffix:
                # ドットで分割し、最後の要素を取得
                subdomain = qname_no_suffix.split('.')[-1]
                return subdomain
    return None

if __name__ == "__main__":
    """IPアドレスの数を計算する
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-y', help='year', required=True)
    parser.add_argument('-m', help='month', required=True)
    parser.add_argument('-d', help='day', required=True)
    args = parser.parse_args()

    year = args.y
    month = args.m
    day = args.d

    # 指定された年月日パターンに合うファイルリストを取得
    files = file_lst(year, month, day)
    # 各ファイルの年月日と時間を辞書形式にまとめる
    file_dict = file_time(files)

    # 各日ごとに処理を行う（キーは "YYYY-MM-DD"）
    for date_key in file_dict.keys():
        uni_src_set = set()
        domain_dict = {}

        # 当該日の各時間（HH）について処理
        for hour in file_dict[date_key]:
            print(f"Processing {date_key}-{hour}")
            # ファイル名例: "2025-02-07-13.csv"
            input_file_name = f"{date_key}-{hour}.csv"
            df = open_reader(input_file_name)

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

        A_tot = len(uni_src_set)
        print(f"A_tot : {A_tot}")

        # サブドメインごとの送信先IP数（dnsmagnitude）の計算
        magnitude_dict = {}
        for domain, src_addrs in domain_dict.items():
            src_addr_count = len(src_addrs)
            if src_addr_count > 0:
                magnitude_dict[domain] = src_addr_count


        # マグニチュードの降順にソート
        mag_dict = dict(sorted(magnitude_dict.items(), key=lambda item: item[1], reverse=True))

        # 結果を CSV ファイルに出力（出力ファイル名は "YYYY-MM-DD.csv"）
        csv_file_path = f"/home/shimada/analysis/output/count/{date_key}.csv"
        with open(csv_file_path, "w", newline='') as f:
            writer = csv.writer(f, delimiter=',')
            writer.writerow(['date', 'domain', 'dnsmagnitude'])
            for subdomain, magnitude in mag_dict.items():
                writer.writerow([date_key, subdomain, str(magnitude)])
