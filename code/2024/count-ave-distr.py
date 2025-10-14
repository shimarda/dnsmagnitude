import func
import argparse
import pandas as pd

def main():
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

    # func.pyの関数を使用してファイルを検索
    file_list = func.find_output_files(year, month, day, where, pattern_prefix="count")
    
    if not file_list:
        print("No CSV files found.")
        exit()

    print("Found files:", file_list)

    # すべてのCSVファイルを読み込み、全ドメインを収集
    all_domains = set()
    for file in file_list:
        df = pd.read_csv(file)
        all_domains.update(df['domain'].unique())

    # func.pyの関数を使用して平均値と分散を計算
    average_dict, distribution_dict = func.calculate_average_distribution(file_list, all_domains)

    # 平均値のCSV出力
    func.write_average_distribution_csv(average_dict, year, month, day, where, "average")

    # 分散値のCSV出力
    func.write_average_distribution_csv(distribution_dict, year, month, day, where, "distribution")

    print("Output files generated successfully.")

if __name__ == "__main__":
    main()
