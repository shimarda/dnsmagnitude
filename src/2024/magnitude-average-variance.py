import argparse
import csv
import glob
import os
import pandas as pd
import re
import statistics

def FindFile(year, month, day, where):
    # dir_path = "/home/shimada/analysis/output/dns_mag/tuika/"
    # dir_path = "/home/shimada/analysis/output/count/"
    dir_path = "/home/shimada/analysis/output/"
    pattern = re.compile(fr"{where}-{year}-{month}-{day}\.csv")
    files = sorted(glob.glob(os.path.join(dir_path, "*.csv")))

    found_file_list = [file for file in files if pattern.search(os.path.basename(file))]
    print("Found files:", found_file_list)

    if not found_file_list:
        print("No CSV files found.")
        return []
    return found_file_list

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-y', help="year", required=True)
    parser.add_argument('-m', help="month", required=True)
    parser.add_argument('-d', help="day", required=True)
    parser.add_argument('-w', help="where")

    args = parser.parse_args()

    year = args.y.zfill(4)
    month = args.m.zfill(2)
    day = args.d.zfill(2)
    where = args.w

    file_list = FindFile(year, month, day, where)
    if not file_list:
        exit()

    total_dict = dict()
    all_domains = set()

    # すべてのCSVファイルを読み込み、全ドメインを収集
    for file in file_list:
        df = pd.read_csv(file)
        all_domains.update(df['domain'].unique())

    # 各ドメインの値をリストに初期化
    for dom in all_domains:
        total_dict[dom] = []

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

    # 平均値のCSV出力
    output_csv = f"/home/shimada/analysis/output/ave-{where}-{year}-{month}-{day}.csv"
    with open(output_csv, "w", newline='') as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow(['domain', 'average'])
        for domain, average in average_dict.items():
            writer.writerow([domain, average])

    # 分散値のCSV出力
    output_csv = f"/home/shimada/analysis/output/distr-{where}-{year}-{month}-{day}.csv"
    with open(output_csv, "w", newline='') as f:
        writer = csv.writer(f, delimiter=',')
        writer.writerow(['domain', 'distribution'])
        for domain, distribution in distribution_dict.items():
            writer.writerow([domain, distribution])

    print("Output files generated successfully.")
