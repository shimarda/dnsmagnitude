import sys
import os
import glob
import re
import pandas as pd
import math

# func.pyをインポート
from func import process_network_analysis_files

def main():
    if len(sys.argv) < 4:
        print(__doc__)
        sys.exit(1)
    
    try:
        year = sys.argv[1]
        month = sys.argv[2].zfill(2)  # ゼロパディング
        day = sys.argv[3].zfill(2)    # ゼロパディング
        
        # 入力ディレクトリ（デフォルト値）
        input_dir = sys.argv[4] if len(sys.argv) > 4 else "/mnt/qnap2/shimada/resolver/"
        
        print(f"=== ネットワーク分類DNS Magnitude分析 ===")
        print(f"対象日付: {year}-{month}-{day}")
        print(f"入力ディレクトリ: {input_dir}")
        print(f"※すべてのパケットは応答パケット（rcode=0）として処理されます")
        print(f"")
        
        # 分析実行
        process_network_analysis_files(year, month, day, input_dir)
        
        print(f"\n=== 分析完了 ===")
        print(f"結果は output/network_analysis/ ディレクトリに保存されました")
        
    except KeyboardInterrupt:
        print("\n処理が中断されました")
        sys.exit(1)
    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
