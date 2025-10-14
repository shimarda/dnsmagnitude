import func2024
import argparse
import matplotlib.pyplot as plt
from datetime import datetime

def main():
    parser = argparse.ArgumentParser(description='バーグラフを作成する')
    parser.add_argument('-y', help='year', required=True)
    parser.add_argument('-m', help='month', required=True)
    parser.add_argument('-d', help='day', required=True)
    parser.add_argument('--data-dir', default="/home/shimada/analysis/output/dns_mag/", 
                       help='データディレクトリ（デフォルト: /home/shimada/analysis/output/dns_mag/）')
    parser.add_argument('--output-dir', help='出力ディレクトリ（指定しない場合はfunc2024のoutputディレクトリ）')
    args = parser.parse_args()

    year = args.y
    month = args.m
    day = args.d

    # func2024の関数を使用してグラフ用データを読み込み
    full_data = func2024.load_graph_data(year, month, day, args.data_dir)
    
    if full_data is None:
        print("データの読み込みに失敗しました。")
        return

    # func2024の関数を使用してカラーマップを作成
    unique_domains = full_data["domain"].unique()
    color_map = func2024.create_color_map(unique_domains)

    # 出力ディレクトリの設定
    if args.output_dir:
        output_dir = args.output_dir
    else:
        output_dir = func2024.ensure_output_dir("graphs")

    print(f"グラフデータを処理中... ドメイン数: {len(unique_domains)}")
    print(f"出力ディレクトリ: {output_dir}")
    
    # ここでグラフ作成のロジックを実装
    # （元のgraph-bar.pyの具体的なグラフ作成部分を移植する必要があります）
    
    print("バーグラフ作成が完了しました。")

if __name__ == "__main__":
    main()
