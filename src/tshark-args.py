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
    #権威
    file_path = f"/mnt/qnap2/shimada/input/{file_name}"
    df = pd.read_csv(file_path)
    return df

# パターンに合うファイル名リスト
def file_lst(year, month, day):
    #権威側
    path = "/mnt/qnap2/shimada/input/*.csv"

    pattern = re.compile(rf"{year}-{month}-{day}-\d{{2}}\.csv")
    #files = os.listdir(path)
    files = sorted(glob.glob(path))
    filtered_files = [file for file in files if pattern.match(os.path.basename(file))]

    return filtered_files

# ファイル名リストから時間のみを抽出
def file_time(file_lst):
    time_list = [
        os.path.basename(f).replace('-', '').replace('.csv', '') for f in file_lst
    ]
    return time_list

# 一意にしてソートされたリストを作成
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


#権威サーバーからの応答を使用するため
#カウントするIPアドレスは送信先IPアドレスを用いる
if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument('-y', help='year')
    parser.add_argument('-m', help='month')
    parser.add_argument('-d', help='day')

    args = parser.parse_args()

    year = args.y
    month = args.m
    day = args.d

    # パターンにあうファイルの時間をリストへ
    r = file_lst(year, month, day)
    time_lst = file_time(r)
    # keyに日にち、値に時間
    file_dict = {}

    # 日にちごとに時間リストへ入れる
    for time in time_lst:
        month = time[4:6]
        day = time[6:8]
        hour = time[8:10]
        if day not in file_dict.keys():
            file_dict[day] = []
        file_dict[day].append(hour)

    for day in file_dict.keys():
        uni_src_set = set()
        domain_dict = {}
        A_tot = 0

        #1時間ごとにファイルにアクセス
        for hour in file_dict[day]:
            print(month + day + hour)

            #データフレームを読み込む
            input_file_name = f"{year}-{month}-{day}-{hour}.csv"
            df = open_reader(input_file_name)

            #'src_addr'のユニークなセットを更新A_total
            uni_src_set.update(df['ip.dst'].unique())

            #'qname'からサブドメインを抽出して新しい列を追加
            df['subdomain'] = df['dns.qry.name'].apply(extract_subdomain)

            #サブドメインが存在する行のみ
            df_sub = df[df['subdomain'].notnull()]

            hour_domain_src_addr_dict = df_sub.groupby('subdomain')['ip.dst'].apply(set).to_dict()

            # 結果を更新
            for domain, src_addrs in hour_domain_src_addr_dict.items():
                if domain in domain_dict:
                    domain_dict[domain].update(src_addrs)
                else:
                    domain_dict[domain] = src_addrs

        A_tot = len(uni_src_set)

        # マグニチュードの計算とソート
        domain_magnitude_list = []
        magnitude_dict = {}
        for key in domain_dict.keys():
            src_addr_count = len(domain_dict[key])
            if src_addr_count > 0:
                magnitude = 10 * math.log(src_addr_count) / math.log(A_tot)
                magnitude_dict[key] = magnitude

        # マグニチュードの降順でソート
        mag_dict = dict(sorted(magnitude_dict.items(), key=lambda item: item[1], reverse=True))

        # 結果をCSVファイルに書き込む
        csv_file_path = f"/home/shimada/analysis/output/dns_mag/{year}-{month}-{day}.csv"
        with open(csv_file_path, "a", newline='') as f:
            writer = csv.writer(f, delimiter=',')
            writer.writerow(['day', 'domain', 'dnsmagnitude'])
            for subdomain in mag_dict:
                writer.writerow([f"{day}", subdomain, str(mag_dict[subdomain])])