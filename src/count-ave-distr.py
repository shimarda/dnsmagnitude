#!/usr/bin/env python3
"""
DNS qtype平均分布分析ツール (2025年版)

指定された月のDNSデータからqtype比率の平均値を計算し、分布を分析します。
共通処理は func.py で管理されています。

使用方法:
    python3 count-ave-distr.py YYYY MM

引数:
    YYYY: 年（4桁）
    MM: 月（2桁、ゼロパディング）

例:
    python3 count-ave-distr.py 2025 04
"""

import sys
import os
import glob
from func import file_lst, calculate_qtype_average_ratios, ensure_output_dir

def main():
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)
    
    try:
        year = sys.argv[1]
        month = sys.argv[2].zfill(2)  # ゼロパディング
        
        print(f"=== DNS qtype平均分布分析: {year}-{month} ===")
        
        # 入力ディレクトリパターン
        input_base_dir = f"/mnt/qnap3/shimada-dnsmagnitude/logs/{year}/{month}/"
        
        if not os.path.exists(input_base_dir):
            print(f"エラー: 入力ディレクトリが見つかりません: {input_base_dir}")
            sys.exit(1)
        
        # 月内のすべての日のディレクトリを取得
        day_dirs = sorted(glob.glob(os.path.join(input_base_dir, "*/")))
        
        if not day_dirs:
            print(f"エラー: 日別ディレクトリが見つかりません: {input_base_dir}")
            sys.exit(1)
        
        print(f"処理対象日数: {len(day_dirs)}")
        
        # 全CSVファイルを収集
        all_csv_files = []
        for day_dir in day_dirs:
            day_csv_files = file_lst(day_dir)
            all_csv_files.extend(day_csv_files)
            print(f"{os.path.basename(day_dir.rstrip('/'))} : {len(day_csv_files)}ファイル")
        
        if not all_csv_files:
            print("エラー: CSVファイルが見つかりませんでした")
            sys.exit(1)
        
        print(f"総ファイル数: {len(all_csv_files)}")
        
        # 出力ディレクトリ設定
        output_dir = ensure_output_dir("average_distribution")
        output_csv_path = os.path.join(output_dir, f"qtype-average-{year}-{month}.csv")
        
        # qtype平均比率を計算
        print("\nqtype平均比率を計算中...")
        average_ratios = calculate_qtype_average_ratios(all_csv_files, output_csv_path)
        
        if average_ratios:
            print(f"\n=== Qtype平均比率結果 ({year}-{month}) ===")
            for qtype, avg_ratio in sorted(average_ratios.items(), key=lambda x: x[1], reverse=True):
                print(f"{qtype:<10} : {avg_ratio:8.6f}")
            
            print(f"\n結果をCSVに保存: {output_csv_path}")
        else:
            print("qtype平均比率の計算に失敗しました")
        
        print(f"\n=== 処理完了: {year}-{month} ===")
        
    except KeyboardInterrupt:
        print("\n処理が中断されました")
        sys.exit(1)
    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
