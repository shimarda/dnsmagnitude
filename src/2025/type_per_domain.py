import pandas as pd
import glob
import os
import csv
from collections import defaultdict
import statistics

def calculate_qtype_average_ratios(start_date, end_date, where, output_dir="/home/shimada/analysis/output-2025/qtype/"):
    """
    指定した期間のqtype-{where}-YYYY-MM-DD.csvファイルから
    サブドメインごとのqtype割合の平均を計算する関数
    
    Args:
        start_date: 開始日 (YYYY-MM-DD形式)
        end_date: 終了日 (YYYY-MM-DD形式)
        where: 0=権威サーバー、1=リゾルバー
        output_dir: CSVファイルが格納されているディレクトリ
    """
    from datetime import datetime, timedelta
    
    # 日付文字列をdatetimeオブジェクトに変換
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    # サブドメインごとのqtype別割合を格納する辞書
    # subdomain_qtype_ratios[subdomain][qtype] = [ratio1, ratio2, ...]
    subdomain_qtype_ratios = defaultdict(lambda: defaultdict(list))
    
    processed_files = []
    missing_files = []
    
    print(f"Processing qtype ratio files from {start_date} to {end_date}")
    print(f"Target directory: {output_dir}")
    
    # 日付範囲内の各日を処理
    current_dt = start_dt
    while current_dt <= end_dt:
        date_str = current_dt.strftime("%Y-%m-%d")
        
        # ファイルパスを構築
        csv_file_path = os.path.join(output_dir, f"qtype-{where}-{date_str}.csv")
        
        if not os.path.exists(csv_file_path):
            missing_files.append(csv_file_path)
            print(f"File not found: {csv_file_path}")
            current_dt += timedelta(days=1)
            continue
        
        try:
            # CSVファイルを読み込み
            df = pd.read_csv(csv_file_path)
            processed_files.append(csv_file_path)
            
            print(f"Processing: {csv_file_path}")
            print(f"  Records: {len(df)}")
            
            # 各行を処理してサブドメインごとのqtype割合を記録
            for _, row in df.iterrows():
                subdomain = str(row['subdomain']).strip()
                qtype = str(row['qtype']).strip()
                ratio = float(row['ratio'])
                
                # サブドメインごとのqtype別割合リストに追加
                subdomain_qtype_ratios[subdomain][qtype].append(ratio)
        
        except Exception as e:
            print(f"Error processing {csv_file_path}: {str(e)}")
        
        current_dt += timedelta(days=1)
    
    print(f"\nProcessed {len(processed_files)} files successfully")
    if missing_files:
        print(f"Missing {len(missing_files)} files:")
        for missing_file in missing_files[:5]:  # 最初の5つだけ表示
            print(f"  {missing_file}")
        if len(missing_files) > 5:
            print(f"  ... and {len(missing_files) - 5} more")
    
    if not subdomain_qtype_ratios:
        print("No data found to process.")
        return
    
    # 平均割合を計算
    subdomain_qtype_averages = {}
    
    for subdomain, qtype_ratios in subdomain_qtype_ratios.items():
        subdomain_qtype_averages[subdomain] = {}
        for qtype, ratios in qtype_ratios.items():
            # 平均を計算
            avg_ratio = statistics.mean(ratios)
            subdomain_qtype_averages[subdomain][qtype] = {
                'average': avg_ratio,
                'count': len(ratios),  # 何日分のデータから計算したか
                'std_dev': statistics.stdev(ratios) if len(ratios) > 1 else 0.0
            }
    
    # 結果をCSVファイルに出力
    period_str = f"{start_date}_to_{end_date}"
    output_csv_path = os.path.join(output_dir, f"qtype-average-{where}-{period_str}.csv")
    
    with open(output_csv_path, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['period', 'subdomain', 'qtype', 'average_ratio', 'data_points', 'std_deviation'])
        
        # サブドメインをアルファベット順にソート
        for subdomain in sorted(subdomain_qtype_averages.keys()):
            # qtypeを文字列としてソート
            for qtype in sorted(subdomain_qtype_averages[subdomain].keys()):
                stats = subdomain_qtype_averages[subdomain][qtype]
                writer.writerow([
                    period_str,
                    subdomain,
                    qtype,
                    f"{stats['average']:.6f}",
                    stats['count'],
                    f"{stats['std_dev']:.6f}"
                ])
    
    print(f"\nAverage qtype ratios calculated and saved to: {output_csv_path}")
    print(f"Total subdomains processed: {len(subdomain_qtype_averages)}")
    
    # 統計情報を表示
    total_subdomain_qtype_pairs = sum(len(qtypes) for qtypes in subdomain_qtype_averages.values())
    print(f"Total subdomain-qtype pairs: {total_subdomain_qtype_pairs}")
    
    # サンプル結果を表示（最初の5つのサブドメイン）
    print("\nSample results (first 5 subdomains):")
    sample_count = 0
    for subdomain in sorted(subdomain_qtype_averages.keys())[:5]:
        print(f"\nSubdomain: {subdomain}")
        for qtype in sorted(subdomain_qtype_averages[subdomain].keys())[:3]:  # 各サブドメインの最初の3つのqtype
            stats = subdomain_qtype_averages[subdomain][qtype]
            print(f"  {qtype}: avg={stats['average']:.4f}, points={stats['count']}, std={stats['std_dev']:.4f}")
        if len(subdomain_qtype_averages[subdomain]) > 3:
            print(f"  ... and {len(subdomain_qtype_averages[subdomain]) - 3} more qtypes")


def calculate_qtype_average_ratios_by_pattern(year_pattern, month_pattern, day_pattern, where, output_dir="/home/shimada/analysis/output-2025/qtype/"):
    """
    パターンマッチングによってファイルを選択し、qtype割合の平均を計算する関数
    
    Args:
        year_pattern: 年のパターン（例: "2025", "*"）
        month_pattern: 月のパターン（例: "04", "*"）
        day_pattern: 日のパターン（例: "01", "*"）
        where: 0=権威サーバー、1=リゾルバー
        output_dir: CSVファイルが格納されているディレクトリ
    """
    
    # ファイルパターンを構築
    file_pattern = os.path.join(output_dir, f"qtype-{where}-{year_pattern}-{month_pattern}-{day_pattern}.csv")
    
    # パターンにマッチするファイルを取得
    matching_files = sorted(glob.glob(file_pattern))
    
    if not matching_files:
        print(f"No files found matching pattern: {file_pattern}")
        return
    
    print(f"Found {len(matching_files)} files matching pattern: {file_pattern}")
    
    # サブドメインごとのqtype別割合を格納する辞書
    subdomain_qtype_ratios = defaultdict(lambda: defaultdict(list))
    
    processed_files = []
    
    for csv_file_path in matching_files:
        try:
            # CSVファイルを読み込み
            df = pd.read_csv(csv_file_path)
            processed_files.append(csv_file_path)
            
            print(f"Processing: {os.path.basename(csv_file_path)} (Records: {len(df)})")
            
            # 各行を処理してサブドメインごとのqtype割合を記録
            for _, row in df.iterrows():
                subdomain = str(row['subdomain']).strip()
                qtype = str(row['qtype']).strip()
                ratio = float(row['ratio'])
                
                # サブドメインごとのqtype別割合リストに追加
                subdomain_qtype_ratios[subdomain][qtype].append(ratio)
        
        except Exception as e:
            print(f"Error processing {csv_file_path}: {str(e)}")
    
    print(f"\nSuccessfully processed {len(processed_files)} files")
    
    if not subdomain_qtype_ratios:
        print("No data found to process.")
        return
    
    # 平均割合を計算
    subdomain_qtype_averages = {}
    
    for subdomain, qtype_ratios in subdomain_qtype_ratios.items():
        subdomain_qtype_averages[subdomain] = {}
        for qtype, ratios in qtype_ratios.items():
            # 平均を計算
            avg_ratio = statistics.mean(ratios)
            subdomain_qtype_averages[subdomain][qtype] = {
                'average': avg_ratio,
                'count': len(ratios),  # 何日分のデータから計算したか
                'std_dev': statistics.stdev(ratios) if len(ratios) > 1 else 0.0
            }
    
    # 結果をCSVファイルに出力
    pattern_str = f"{year_pattern}-{month_pattern}-{day_pattern}"
    output_csv_path = os.path.join(output_dir, f"qtype-average-{where}-{pattern_str}.csv")
    
    with open(output_csv_path, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['pattern', 'subdomain', 'qtype', 'average_ratio', 'data_points', 'std_deviation'])
        
        # サブドメインをアルファベット順にソート
        for subdomain in sorted(subdomain_qtype_averages.keys()):
            # qtypeを文字列としてソート
            for qtype in sorted(subdomain_qtype_averages[subdomain].keys()):
                stats = subdomain_qtype_averages[subdomain][qtype]
                writer.writerow([
                    pattern_str,
                    subdomain,
                    qtype,
                    f"{stats['average']:.6f}",
                    stats['count'],
                    f"{stats['std_dev']:.6f}"
                ])
    
    print(f"\nAverage qtype ratios calculated and saved to: {output_csv_path}")
    print(f"Total subdomains processed: {len(subdomain_qtype_averages)}")
    
    # 統計情報を表示
    total_subdomain_qtype_pairs = sum(len(qtypes) for qtypes in subdomain_qtype_averages.values())
    print(f"Total subdomain-qtype pairs: {total_subdomain_qtype_pairs}")
    
    # サンプル結果を表示（最初の5つのサブドメイン）
    print("\nSample results (first 5 subdomains):")
    for subdomain in sorted(subdomain_qtype_averages.keys())[:5]:
        print(f"\nSubdomain: {subdomain}")
        for qtype in sorted(subdomain_qtype_averages[subdomain].keys())[:3]:  # 各サブドメインの最初の3つのqtype
            stats = subdomain_qtype_averages[subdomain][qtype]
            print(f"  {qtype}: avg={stats['average']:.4f}, points={stats['count']}, std={stats['std_dev']:.4f}")
        if len(subdomain_qtype_averages[subdomain]) > 3:
            print(f"  ... and {len(subdomain_qtype_averages[subdomain]) - 3} more qtypes")


# 使用例
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='DNS QType割合平均計算')
    parser.add_argument('-w', required=True, help='0は権威1はリゾルバ')
    parser.add_argument('--start-date', help='開始日 (YYYY-MM-DD形式)')
    parser.add_argument('--end-date', help='終了日 (YYYY-MM-DD形式)')
    parser.add_argument('-y', help='年のパターン (例: 2025, *)')
    parser.add_argument('-m', help='月のパターン (例: 04, *)')
    parser.add_argument('-d', help='日のパターン (例: 01, *)')
    parser.add_argument('--mode', choices=['range', 'pattern'], default='range',
                       help='range: 日付範囲指定, pattern: パターンマッチング')
    parser.add_argument('--output-dir', default='/home/shimada/analysis/output-2025/qtype/',
                       help='CSVファイルが格納されているディレクトリ')
    
    args = parser.parse_args()
    
    if args.mode == 'range':
        if not args.start_date or not args.end_date:
            print("エラー: 日付範囲モードでは --start-date と --end-date が必要です")
            print("使用例: python script.py --mode range --start-date 2025-04-01 --end-date 2025-04-30 -w 0")
            exit(1)
        
        calculate_qtype_average_ratios(args.start_date, args.end_date, args.w, args.output_dir)
    
    elif args.mode == 'pattern':
        if not all([args.y, args.m, args.d]):
            print("エラー: パターンモードでは -y, -m, -d が必要です")
            print("使用例: python script.py --mode pattern -y 2025 -m 04 -d '*' -w 0")
            exit(1)
        
        calculate_qtype_average_ratios_by_pattern(args.y, args.m, args.d, args.w, args.output_dir)