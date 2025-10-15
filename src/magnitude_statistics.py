#!/usr/bin/env python3
"""
マグニチュード統計分析ツール

ネットワーク分類別・分析タイプ別のマグニチュード統計（平均、分散など）を計算します。

使用方法:
    python3 magnitude_statistics.py START_DATE END_DATE [input_dir]

引数:
    START_DATE: 開始日 (YYYY-MM-DD形式)
    END_DATE: 終了日 (YYYY-MM-DD形式)
    input_dir: 入力ディレクトリ (デフォルト: output/network_analysis/)

例:
    python3 magnitude_statistics.py 2025-04-01 2025-04-07
    python3 magnitude_statistics.py 2025-04-01 2025-04-30
"""

import sys
import os
from func import process_magnitude_statistics_analysis

def main():
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)
    
    try:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
        input_dir = sys.argv[3] if len(sys.argv) > 3 else None
        
        # 日付形式の簡単な検証
        from datetime import datetime
        try:
            datetime.strptime(start_date, '%Y-%m-%d')
            datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError:
            print("エラー: 日付は YYYY-MM-DD 形式で指定してください")
            sys.exit(1)
        
        print(f"=== マグニチュード統計分析 ===")
        print(f"期間: {start_date} から {end_date}")
        if input_dir:
            print(f"入力ディレクトリ: {input_dir}")
        
        # 統計分析実行
        process_magnitude_statistics_analysis(start_date, end_date, input_dir)
        
    except KeyboardInterrupt:
        print("\n処理が中断されました")
        sys.exit(1)
    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
