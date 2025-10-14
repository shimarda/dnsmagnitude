import func2024
import argparse

def main():
    """IPアドレスの数を計算する"""
    parser = argparse.ArgumentParser()
    parser.add_argument('-y', help='year', required=True)
    parser.add_argument('-m', help='month', required=True)
    parser.add_argument('-d', help='day', required=True)
    args = parser.parse_args()

    year = args.y
    month = args.m
    day = args.d

    # 指定された年月日パターンに合うファイルリストを取得
    files = func2024.file_lst(year, month, day)
    # 各ファイルの年月日と時間を辞書形式にまとめる
    file_dict = func2024.file_time_dict(files)

    # 各日ごとに処理を行う（キーは "YYYY-MM-DD"）
    for date_key in file_dict.keys():
        uni_src_set = set()
        domain_dict = {}

        # 当該日の各時間（HH）について処理
        for hour in file_dict[date_key]:
            print(f"Processing {date_key}-{hour}")
            # ファイル名例: "2025-02-07-13.csv"
            input_file_name = f"{date_key}-{hour}.csv"
            
            # func2024の関数を使用してデータフレームを読み込む
            df = func2024.open_reader_safe(input_file_name)
            
            if df.empty:
                continue
            
            # func2024の関数を使用してマグニチュード計算用データを処理
            func2024.process_magnitude_hourly_data_2024(df, uni_src_set, domain_dict)

        A_tot = len(uni_src_set)
        print(f"A_tot : {A_tot}")

        # func2024の関数を使用してマグニチュードを計算
        mag_dict = func2024.calculate_magnitude_2024(domain_dict, A_tot)

        # func2024の関数を使用して結果をCSVファイルに出力
        func2024.write_magnitude_csv_2024(mag_dict, date_key)

if __name__ == "__main__":
    main()
