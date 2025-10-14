import func2024
import argparse
import pandas as pd

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-y', help="year", required=True)
    parser.add_argument('-m', help="month", required=True)
    parser.add_argument('-d', help="day", required=True)
    parser.add_argument('-w', help="where", required=True)
    args = parser.parse_args()

    year = args.y.zfill(4)
    month = args.m.zfill(2)
    day = args.d.zfill(2)
    where = args.w

    # func2024の関数を使用してファイルを検索
    file_list = func2024.find_output_files_by_pattern(where, year, month, day)
    
    if not file_list:
        print("No CSV files found.")
        exit()

    print("Found files:", file_list)

    # すべてのCSVファイルを読み込み、全ドメインを収集
    all_domains = set()
    for file in file_list:
        df = pd.read_csv(file)
        all_domains.update(df['domain'].unique())

    # func2024の関数を使用して平均値と分散を計算
    average_dict, distribution_dict = func2024.calculate_average_distribution_2024(file_list, all_domains)

    # 平均値のCSV出力
    func2024.write_average_distribution_csv_2024(average_dict, year, month, day, where, "average")

    # 分散値のCSV出力
    func2024.write_average_distribution_csv_2024(distribution_dict, year, month, day, where, "distribution")

    print("Output files generated successfully.")

if __name__ == "__main__":
    main()
