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

# ファイル名からファイルを開く（エラー行の出力を追加）
def open_reader(file_name, where):
    # 権威
    if where == 0:
        file_path = f"/mnt/qnap2/shimada/input/{file_name}"
    else:
        file_path = f"/mnt/qnap2/shimada/resolver/{file_name}"
    
    error_lines = []
    
    try:
        # まず通常に読み込もうとする
        df = pd.read_csv(file_path)
        return df, error_lines
    except Exception as e:
        print(f"通常の読み込みでエラー発生: {file_name}: {str(e)}")
        
        # エラーが発生した場合は、行ごとに読み込んでエラーの行を特定
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)  # ヘッダーを読み飛ばす
            
            # 正常な行を保存するためのリスト
            valid_rows = [header]
            
            # 各行を処理
            for i, row in enumerate(reader, start=1):
                try:
                    # 行が正しくパースできるかチェック
                    if len(row) != len(header):
                        print(f"行 {i+1} が正しくパースされませんでした。列数が一致しません。")
                        print(f"問題のある行: {','.join(row)}")
                        error_lines.append((i+1, ','.join(row)))
                    else:
                        valid_rows.append(row)
                except Exception as row_error:
                    error_line = ','.join(row) if isinstance(row, list) else str(row)
                    print(f"行 {i+1} の処理中にエラー: {str(row_error)}")
                    print(f"問題のある行: {error_line}")
                    error_lines.append((i+1, error_line))
        
        # 有効な行からデータフレームを作成
        if len(valid_rows) > 1:  # ヘッダーに加えて少なくとも1行あるか
            # 一時的なCSV文字列を作成
            csv_data = io.StringIO()
            writer = csv.writer(csv_data)
            writer.writerows(valid_rows)
            csv_data.seek(0)
            
            # 文字列からデータフレームを作成
            df = pd.read_csv(csv_data)
        else:
            # 有効な行がない場合は空のデータフレームを返す
            df = pd.DataFrame(columns=header)
            
        return df, error_lines

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

# 権威サーバーからの応答を使用するため
# カウントするIPアドレスは送信先IPアドレスを用いる
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-y', help='year')
    parser.add_argument('-m', help='month')
    parser.add_argument('-d', help='day')
    parser.add_argument('-w', help='0は権威1はリゾルバ')
    parser.add_argument('-o', help='エラーログ出力ファイル', default='error_log.txt')
    args = parser.parse_args()

    year = args.y
    month = args.m
    day = args.d
    where = int(args.w)
    error_log_file = args.o

    # パターンにあうファイルの時間をリストへ
    r = func.file_lst(year, month, day, where)
    time_lst = func.file_time(r)
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

    # 総エラー行数のカウントとエラー行の保存
    total_error_lines = []
    file_error_counts = {}

    for day in file_dict.keys():
        uni_src_set = set()
        domain_dict = {}
        A_tot = 0

        # 1時間ごとにファイルにアクセス
        for hour in file_dict[day]:
            print(month + day + hour)
            # データフレームを読み込む
            input_file_name = f"{year}-{month}-{day}.csv"
            df, error_lines = func.open_reader_safe(year, month, day, where)
            
            # エラー行の処理
            if error_lines:
                file_error_counts[input_file_name] = len(error_lines)
                for line_num, line_content in error_lines:
                    total_error_lines.append((input_file_name, line_num, line_content))
                print(f"ファイル {input_file_name} でスキップされた行数: {len(error_lines)}")
            
            # 空のデータフレームならスキップ
            if df.empty:
                print(f"空のデータフレーム: {input_file_name} - スキップします")
                continue

            # 'ip.dst' のユニークなセットを更新（A_total）
            uni_src_set.update(df['ip.dst'].unique())

            # 'dns.qry.name' からサブドメインを抽出して新しい列を追加
            df['subdomain'] = df['dns.qry.name'].apply(extract_subdomain)

            # サブドメインが存在する行のみを抽出（extract_subdomain が None を返した行はスキップ）
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
        magnitude_dict = {}
        for key in domain_dict.keys():
            src_addr_count = len(domain_dict[key])
            if src_addr_count > 0 and A_tot > 0:  # 0で割らないよう保護
                magnitude = 10 * math.log(src_addr_count) / math.log(A_tot)
                magnitude_dict[key] = magnitude

        # マグニチュードの降順でソート
        mag_dict = dict(sorted(magnitude_dict.items(), key=lambda item: item[1], reverse=True))
        
        # 結果をCSVファイルに書き込む
        csv_file_path = f"/home/shimada/analysis/output/{where}-{year}-{month}-{day}.csv"
        with open(csv_file_path, "w", newline='') as f:
            writer = csv.writer(f, delimiter=',')
            writer.writerow(['day', 'domain', 'dnsmagnitude'])
            for subdomain in mag_dict:
                writer.writerow([f"{day}", subdomain, str(mag_dict[subdomain])])
    
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
    print(f"総エラー行数: {len(total_error_lines)}")
    print(f"詳細なエラーログは {error_log_file} に保存されました")