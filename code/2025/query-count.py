"""
    時間指定をして、サブドメインごとのDNSクエリ数を集計する
    クエリのパーセンテージも出力
    
"""
import re
import pandas as pd
import os
import glob
import csv
import math
import operator
import argparse
import io

import func

# サブドメインを抽出する関数
def extract_subdomain(qname):
    suffix = '.tsukuba.ac.jp'
    if isinstance(qname, str):
        qname_lower = qname.lower()
        # dns.qry.name に tsukuba.ac.jp が 1 回だけ出現する場合のみ処理する
        if qname_lower.count(suffix) != 1:
            return None
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

# サブドメイン別のクエリ数を集計
if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='サブドメイン別クエリ数を集計')
    parser.add_argument('-y', help='year', required=True)
    parser.add_argument('-m', help='month', required=True)
    parser.add_argument('-d', help='day', required=True)
    parser.add_argument('-w', help='0は権威1はリゾルバ', required=True)
    parser.add_argument('-o', help='エラーログ出力ファイル', default='error_log.txt')
    parser.add_argument('--start-hour', type=int, default=0, 
                        help='開始時刻(0-23、デフォルト: 0)')
    parser.add_argument('--end-hour', type=int, default=23, 
                        help='終了時刻(0-23、デフォルト: 23)')
    args = parser.parse_args()

    year = args.y
    month = args.m
    day = args.d
    where = int(args.w)
    error_log_file = args.o
    start_hour = args.start_hour
    end_hour = args.end_hour

    # 時間範囲の妥当性チェック
    if not (0 <= start_hour <= 23 and 0 <= end_hour <= 23):
        print("エラー: 時刻は0から23の範囲で指定してください")
        exit(1)
    
    if start_hour > end_hour:
        print("エラー: 開始時刻は終了時刻以下である必要があります")
        exit(1)
    
    print(f"時間範囲: {start_hour:02d}:00 - {end_hour:02d}:59")

    # パターンにあうファイルの時間をリストへ
    r = func.file_lst(year, month, day, where)
    time_lst = func.file_time(r)
    
    # keyに日にち、値に時間
    file_dict = {}

    # 日にちごとに時間リストへ入れる（時間範囲でフィルタリング）
    for time in time_lst:
        month = time[4:6]
        day = time[6:8]
        hour = time[8:10]
        hour_int = int(hour)
        
        # 指定された時間範囲内の時間のみを追加
        if start_hour <= hour_int <= end_hour:
            if day not in file_dict.keys():
                file_dict[day] = []
            file_dict[day].append(hour)

    # 総エラー行数のカウントとエラー行の保存
    total_error_lines = []
    file_error_counts = {}

    for day in file_dict.keys():
        # クエリ数を集計する辞書
        domain_query_count = {}
        total_queries = 0  # 総クエリ数

        # 1時間ごとにファイルにアクセス
        for hour in file_dict[day]:
            print(month + day + hour)
            # データフレームを読み込む
            input_file_name = f"{year}-{month}-{day}-{hour}.csv"
            df = func.open_reader_safe(year, month, day, hour, where)
            
            # 空のデータフレームならスキップ
            if df.empty:
                print(f"空のデータフレーム: {input_file_name} - スキップします")
                continue

            # 'dns.qry.name' からサブドメインを抽出して新しい列を追加
            df['subdomain'] = df['dns.qry.name'].apply(extract_subdomain)

            # サブドメインが存在する行のみを抽出（extract_subdomain が None を返した行はスキップ）
            df_sub = df[df['subdomain'].notnull()]
            
            # この時間のサブドメイン別クエリ数を集計
            hour_query_count = df_sub['subdomain'].value_counts().to_dict()

            # 総クエリ数に加算
            total_queries += len(df_sub)

            # 日ごとの集計に追加
            for subdomain, count in hour_query_count.items():
                if subdomain in domain_query_count:
                    domain_query_count[subdomain] += count
                else:
                    domain_query_count[subdomain] = count

        # クエリ数の降順でソート
        sorted_domain_count = dict(sorted(domain_query_count.items(), 
                                         key=lambda item: item[1], 
                                         reverse=True))
        
        # 結果をCSVファイルに書き込む（時間範囲を含む）
        time_range_str = f"{start_hour:02d}-{end_hour:02d}"
        csv_file_path = f"/home/shimada/analysis/output-time/count-{where}-{year}-{month}-{day}-{time_range_str}.csv"
        
        with open(csv_file_path, "w", newline='') as f:
            writer = csv.writer(f, delimiter=',')
            writer.writerow(['day', 'time_range', 'subdomain', 'query_count', 'percentage'])
            
            for subdomain, count in sorted_domain_count.items():
                # パーセンテージを計算
                percentage = (count / total_queries * 100) if total_queries > 0 else 0
                writer.writerow([f"{day}", time_range_str, subdomain, count, f"{percentage:.2f}"])
        
        print(f"\n=== 集計結果 ===")
        print(f"日付: {year}-{month}-{day}")
        print(f"時間範囲: {time_range_str}")
        print(f"総クエリ数: {total_queries:,}")
        print(f"ユニークなサブドメイン数: {len(domain_query_count)}")
        print(f"結果を保存しました: {csv_file_path}")
        
        # 上位10件を表示
        print(f"\n上位10のサブドメイン:")
        for i, (subdomain, count) in enumerate(list(sorted_domain_count.items())[:10], 1):
            percentage = (count / total_queries * 100) if total_queries > 0 else 0
            print(f"{i:2d}. {subdomain:20s}: {count:>8,} ({percentage:5.2f}%)")
    
    # エラーログの出力
    with open(error_log_file, 'w', encoding='utf-8') as log_file:
        log_file.write("=== エラー行の詳細 ===\n")
        for file_name, line_num, line_content in total_error_lines:
            log_file.write(f"ファイル: {file_name}, 行番号: {line_num}\n")
            log_file.write(f"行内容: {line_content}\n")
            log_file.write("-" * 80 + "\n")
        
        log_file.write("\n=== ファイルごとのエラー行数 ===\n")
        for file_name, count in file_error_counts.items():
            log_file.write(f"{file_name}: {count}行\n")
        
        log_file.write(f"\n総エラー行数: {len(total_error_lines)}\n")
    
    # 総エラー行数の出力
    if total_error_lines:
        print(f"\n総エラー行数: {len(total_error_lines)}")
        print(f"詳細なエラーログは {error_log_file} に保存されました")
    else:
        print("\nエラーなし")