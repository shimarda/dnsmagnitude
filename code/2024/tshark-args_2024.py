import func2024
import argparse
import math

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-y', help='year')
    parser.add_argument('-m', help='month')
    parser.add_argument('-d', help='day')
    parser.add_argument('-w', help='0は権威1はリゾルバ')
    args = parser.parse_args()

    year = args.y
    month = args.m
    day = args.d
    where = int(args.w)

    # パターンにあうファイルの時間をリストへ
    r = func2024.file_lst(year, month, day, where)
    time_lst = func2024.file_time(r)
    
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

        # 1時間ごとにファイルにアクセス
        for hour in file_dict[day]:
            print(month + day + hour)
            
            # func2024の関数を使用してデータフレームを読み込む
            df = func2024.open_reader_safe(f"{year}-{month}-{day}-{hour}.csv", where)
            
            if df.empty:
                continue
            
            # func2024の関数を使用してマグニチュード計算用データを処理
            func2024.process_magnitude_hourly_data_2024(df, uni_src_set, domain_dict)

        A_tot = len(uni_src_set)

        # サブドメインごとの送信先IP数（dnsmagnitude）の計算
        magnitude_dict = {}
        for domain, src_addrs in domain_dict.items():
            src_addr_count = len(src_addrs)
            if src_addr_count > 0 and A_tot > 0:  # 0で割らないよう保護
                magnitude = 10 * math.log(src_addr_count) / math.log(A_tot)
                magnitude_dict[domain] = magnitude

        # マグニチュードの降順でソート
        mag_dict = dict(sorted(magnitude_dict.items(), key=lambda item: item[1], reverse=True))

        # func2024の関数を使用してCSVファイルに書き込み
        func2024.write_tshark_args_csv(mag_dict, day, year, month, where)

if __name__ == "__main__":
    main()
