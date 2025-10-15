#!/usr/bin/env python3
"""
DNS集計分析ツール (2025年版)

指定された日付のDNSデータを集計し、qtype分析とマグニチュード計算を実行します。
共通処理は func.py で管理されています。

使用方法:
    python3 count.py YYYY MM DD

引数:
    YYYY: 年（4桁）
    MM: 月（2桁、ゼロパディング）
    DD: 日（2桁、ゼロパディング）

例:
    python3 count.py 2025 04 01
"""

import sys
import os
from func import (
    file_lst, safe_read_csv, qtype_ratio, calculate_dns_magnitude,
    write_magnitude_csv, ensure_output_dir, write_error_log
)

def main():
    if len(sys.argv) != 4:
        print(__doc__)
        sys.exit(1)
    
    try:
        year = sys.argv[1]
        month = sys.argv[2].zfill(2)  # ゼロパディング
        day = sys.argv[3].zfill(2)    # ゼロパディング
        
        date_str = f"{year}-{month}-{day}"
        print(f"=== DNS集計分析: {date_str} ===")
        
        # 入力ディレクトリ設定
        input_directory = f"/mnt/qnap3/shimada-dnsmagnitude/logs/{year}/{month}/{day}/"
        
        if not os.path.exists(input_directory):
            print(f"エラー: 入力ディレクトリが見つかりません: {input_directory}")
            sys.exit(1)
        
        # CSVファイルリストを取得
        csv_files = file_lst(input_directory)
        
        if not csv_files:
            print(f"エラー: CSVファイルが見つかりません: {input_directory}")
            sys.exit(1)
        
        print(f"処理対象ファイル数: {len(csv_files)}")
        
        # 出力ディレクトリ設定
        count_output_dir = ensure_output_dir("count")
        magnitude_output_dir = ensure_output_dir("magnitude")
        
        # ファイルごとにqtype比率を計算し、統合データを作成
        all_data = []
        total_error_lines = []
        file_error_counts = {}
        
        for file_path in csv_files:
            file_name = os.path.basename(file_path)
            print(f"処理中: {file_name}")
            
            # CSVファイルを安全に読み込み
            df, error_lines = safe_read_csv(file_path)
            
            if not df.empty:
                all_data.append(df)
            
            # エラー行の記録
            if error_lines:
                file_error_counts[file_name] = len(error_lines)
                total_error_lines.extend([(file_name, line_num, content) for line_num, content in error_lines])
        
        if not all_data:
            print("エラー: 有効なデータが見つかりませんでした")
            sys.exit(1)
        
        print(f"有効ファイル数: {len(all_data)}")
        
        # データを統合
        import pandas as pd
        combined_df = pd.concat(all_data, ignore_index=True)
        print(f"統合データサイズ: {len(combined_df)}行")
        
        # qtype比率を計算
        ratios = qtype_ratio(combined_df)
        if ratios:
            print("\n=== Qtype比率 ===")
            for qtype, ratio in sorted(ratios.items(), key=lambda x: x[1], reverse=True):
                print(f"{qtype}: {ratio:.6f}")
            
            # qtype比率をCSVに保存
            count_csv_path = os.path.join(count_output_dir, f"qtype-ratio-{date_str}.csv")
            with open(count_csv_path, 'w', newline='') as csvfile:
                import csv
                writer = csv.writer(csvfile)
                writer.writerow(['date', 'qtype', 'ratio'])
                for qtype, ratio in sorted(ratios.items(), key=lambda x: x[1], reverse=True):
                    writer.writerow([date_str, qtype, f"{ratio:.6f}"])
            
            print(f"qtype比率をCSVに保存: {count_csv_path}")
        
        # DNS Magnitudeを計算（IPアドレスとqnameカラムが必要）
        if 'ip' in combined_df.columns and 'qname' in combined_df.columns:
            magnitude_dict = calculate_dns_magnitude(combined_df, date_str)
            
            if magnitude_dict:
                print(f"\n=== DNS Magnitude (上位10件) ===")
                for i, (domain, magnitude) in enumerate(list(magnitude_dict.items())[:10], 1):
                    print(f"{i:2d}. {domain:<30} {magnitude:8.6f}")
                
                # MagnitudeをCSVに保存
                magnitude_csv_path = os.path.join(magnitude_output_dir, f"magnitude-{date_str}.csv")
                write_magnitude_csv(magnitude_dict, date_str, magnitude_csv_path)
            else:
                print("DNS Magnitudeの計算に失敗しました")
        else:
            print("警告: DNS Magnitude計算に必要なカラム（ip, qname）が見つかりません")
        
        # エラーログの出力
        if total_error_lines:
            print(f"\n警告: {len(total_error_lines)}行のエラーが発見されました")
            error_log_file = f"error_log_{date_str}.txt"
            write_error_log(total_error_lines, file_error_counts, error_log_file)
        
        print(f"\n=== 処理完了: {date_str} ===")
        
    except KeyboardInterrupt:
        print("\n処理が中断されました")
        sys.exit(1)
    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
