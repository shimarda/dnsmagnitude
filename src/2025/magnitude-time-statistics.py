#!/usr/bin/env python3
"""
時間範囲別DNS Magnitude統計分析ツール

dnsmagnitude-time.pyで生成された時間範囲指定のDNS Magnitudeデータから、
サブドメインごとの平均値、分散、標準偏差などの統計情報を計算します。

入力ファイル形式: {where}-YYYY-MM-DD-HH-HH.csv
  例: 1-2025-04-01-08-18.csv (リゾルバ、2025年4月1日、8時-18時)

出力ファイル形式: magnitude-time-statistics-{where}-{time_range}-{pattern}.csv

使用方法:
    # 日付範囲指定で統計を計算
    python3 magnitude-time-statistics.py --mode range --start-date YYYY-MM-DD --end-date YYYY-MM-DD -w [0|1] --time-range HH-HH
    
    # パターン指定で統計を計算
    python3 magnitude-time-statistics.py --mode pattern -y YYYY -m MM -d DD -w [0|1] --time-range HH-HH

引数:
    --mode: 動作モード (range: 日付範囲, pattern: パターンマッチ)
    --start-date: 開始日 (range モード用)
    --end-date: 終了日 (range モード用)
    -y: 年パターン (pattern モード用、*でワイルドカード)
    -m: 月パターン (pattern モード用、*でワイルドカード)
    -d: 日パターン (pattern モード用、*でワイルドカード)
    -w: サーバータイプ (0=権威サーバー, 1=リゾルバ)
    --time-range: 時間範囲 (例: 08-18)
    --input-dir: 入力ディレクトリ (デフォルト: /home/shimada/analysis/output-time/)
    --output-dir: 出力ディレクトリ (デフォルト: /home/shimada/analysis/output-time/statistics/)

例:
    # 2025年4月の権威サーバー(8-18時)の統計を計算
    python3 magnitude-time-statistics.py --mode range --start-date 2025-04-01 --end-date 2025-04-30 -w 0 --time-range 08-18
    
    # 2025年4月の全データをパターンで処理（リゾルバ、8-18時）
    python3 magnitude-time-statistics.py --mode pattern -y 2025 -m 04 -d '*' -w 1 --time-range 08-18
"""

import pandas as pd
import glob
import os
import csv
from collections import defaultdict
import statistics
from datetime import datetime, timedelta
import argparse
import re

def parse_time_magnitude_filename(filename, where, time_range):
    """
    時間範囲別Magnitudeファイル名から日付を抽出
    
    ファイル名形式: {where}-YYYY-MM-DD-HH-HH.csv
    例: 0-2025-04-01-08-18.csv, 1-2025-04-15-08-18.csv
    """
    pattern = rf'{where}-(\d{{4}})-(\d{{2}})-(\d{{2}})-{re.escape(time_range)}\.csv'
    match = re.match(pattern, os.path.basename(filename))
    if match:
        year, month, day = match.groups()
        return f"{year}-{month}-{day}"
    return None

def get_server_type_label(where):
    """サーバータイプのラベルを取得"""
    return "権威サーバー" if where == 0 else "リゾルバ"

def calculate_time_magnitude_statistics_range(start_date, end_date, where, time_range, 
                                              input_dir, output_dir):
    """
    指定した日付範囲の時間範囲別Magnitudeファイルから統計情報を計算
    
    Args:
        start_date: 開始日 (YYYY-MM-DD形式)
        end_date: 終了日 (YYYY-MM-DD形式)
        where: 0=権威サーバー, 1=リゾルバ
        time_range: 時間範囲 (例: "08-18")
        input_dir: Magnitudeファイルが格納されているディレクトリ
        output_dir: 結果を出力するディレクトリ
    """
    
    # 日付文字列をdatetimeオブジェクトに変換
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    
    # サブドメインごとのMagnitude値を格納
    subdomain_magnitudes = defaultdict(list)
    
    processed_files = []
    missing_files = []
    
    server_type = get_server_type_label(where)
    
    print(f"=== 時間範囲別DNS Magnitude統計分析 ({server_type}) ===")
    print(f"期間: {start_date} から {end_date}")
    print(f"時間範囲: {time_range}")
    print(f"入力ディレクトリ: {input_dir}")
    print(f"サーバータイプ: {server_type} (where={where})")
    
    # 日付範囲内の各日を処理
    current_dt = start_dt
    while current_dt <= end_dt:
        date_str = current_dt.strftime("%Y-%m-%d")
        
        # ファイルパスを構築 (形式: {where}-YYYY-MM-DD-HH-HH.csv)
        csv_file_path = os.path.join(input_dir, f"{where}-{date_str}-{time_range}.csv")
        
        if not os.path.exists(csv_file_path):
            missing_files.append(csv_file_path)
            current_dt += timedelta(days=1)
            continue
        
        try:
            # CSVファイルを読み込み
            df = pd.read_csv(csv_file_path)
            processed_files.append(csv_file_path)
            
            print(f"処理中: {os.path.basename(csv_file_path)} (レコード数: {len(df)})")
            
            # 各行を処理してサブドメインごとのMagnitude値を記録
            for _, row in df.iterrows():
                # カラム名の確認と取得
                if 'domain' in df.columns:
                    subdomain = str(row['domain']).strip()
                elif 'subdomain' in df.columns:
                    subdomain = str(row['subdomain']).strip()
                else:
                    print(f"警告: サブドメイン列が見つかりません")
                    continue
                
                # magnitudeカラムの確認
                if 'dnsmagnitude' in df.columns:
                    magnitude = float(row['dnsmagnitude'])
                elif 'magnitude' in df.columns:
                    magnitude = float(row['magnitude'])
                else:
                    print(f"警告: magnitude列が見つかりません")
                    continue
                
                subdomain_magnitudes[subdomain].append(magnitude)
        
        except Exception as e:
            print(f"エラー: {csv_file_path} の処理中 - {str(e)}")
        
        current_dt += timedelta(days=1)
    
    print(f"\n処理完了: {len(processed_files)}ファイル")
    if missing_files:
        print(f"欠落ファイル: {len(missing_files)}個")
        if len(missing_files) <= 5:
            for f in missing_files:
                print(f"  - {os.path.basename(f)}")
    
    if not subdomain_magnitudes:
        print("処理するデータが見つかりませんでした")
        return
    
    # 統計情報を計算
    subdomain_statistics = {}
    
    for subdomain, magnitudes in subdomain_magnitudes.items():
        if len(magnitudes) == 0:
            continue
        
        # 統計値を計算
        stats = {
            'mean': statistics.mean(magnitudes),
            'variance': statistics.variance(magnitudes) if len(magnitudes) > 1 else 0.0,
            'std_dev': statistics.stdev(magnitudes) if len(magnitudes) > 1 else 0.0,
            'median': statistics.median(magnitudes),
            'min': min(magnitudes),
            'max': max(magnitudes),
            'count': len(magnitudes),
            'q25': pd.Series(magnitudes).quantile(0.25),
            'q75': pd.Series(magnitudes).quantile(0.75)
        }
        
        subdomain_statistics[subdomain] = stats
    
    # 結果を出力ディレクトリに保存
    os.makedirs(output_dir, exist_ok=True)
    period_str = f"{start_date}_to_{end_date}"
    output_csv_path = os.path.join(output_dir, 
                                   f"magnitude-time-statistics-{where}-{time_range}-{period_str}.csv")
    
    with open(output_csv_path, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'server_type', 'time_range', 'period', 'subdomain', 'mean', 'variance', 
            'std_dev', 'median', 'min', 'max', 'q25', 'q75', 'data_points'
        ])
        
        # 平均値の降順でソート
        for subdomain, stats in sorted(subdomain_statistics.items(), 
                                      key=lambda x: x[1]['mean'], reverse=True):
            writer.writerow([
                where,
                time_range,
                period_str,
                subdomain,
                f"{stats['mean']:.6f}",
                f"{stats['variance']:.6f}",
                f"{stats['std_dev']:.6f}",
                f"{stats['median']:.6f}",
                f"{stats['min']:.6f}",
                f"{stats['max']:.6f}",
                f"{stats['q25']:.6f}",
                f"{stats['q75']:.6f}",
                stats['count']
            ])
    
    print(f"\n=== 統計結果サマリー ===")
    print(f"サーバータイプ: {server_type}")
    print(f"時間範囲: {time_range}")
    print(f"処理サブドメイン数: {len(subdomain_statistics)}")
    print(f"結果ファイル: {output_csv_path}")
    
    # 上位10件の統計情報を表示
    print(f"\n=== 上位10サブドメインの統計 (平均値順) ===")
    print(f"{'順位':<4} {'サブドメイン':<20} {'平均':<10} {'分散':<10} {'標準偏差':<10} {'データ数':<8}")
    print("-" * 70)
    
    for rank, (subdomain, stats) in enumerate(
        sorted(subdomain_statistics.items(), key=lambda x: x[1]['mean'], reverse=True)[:10], 1):
        print(f"{rank:<4} {subdomain:<20} {stats['mean']:<10.4f} "
              f"{stats['variance']:<10.4f} {stats['std_dev']:<10.4f} {stats['count']:<8}")
    
    # 分散が大きいサブドメイン（変動が激しい）
    print(f"\n=== 分散が大きい上位5サブドメイン ===")
    print(f"{'順位':<4} {'サブドメイン':<20} {'分散':<10} {'標準偏差':<10} {'平均':<10}")
    print("-" * 60)
    
    for rank, (subdomain, stats) in enumerate(
        sorted(subdomain_statistics.items(), key=lambda x: x[1]['variance'], reverse=True)[:5], 1):
        print(f"{rank:<4} {subdomain:<20} {stats['variance']:<10.4f} "
              f"{stats['std_dev']:<10.4f} {stats['mean']:<10.4f}")

def calculate_time_magnitude_statistics_pattern(year_pattern, month_pattern, day_pattern, 
                                               where, time_range, input_dir, output_dir):
    """
    パターンマッチングによってファイルを選択し、統計情報を計算
    
    Args:
        year_pattern: 年のパターン（例: "2025", "*"）
        month_pattern: 月のパターン（例: "04", "*"）
        day_pattern: 日のパターン（例: "01", "*"）
        where: 0=権威サーバー, 1=リゾルバ
        time_range: 時間範囲 (例: "08-18")
        input_dir: Magnitudeファイルが格納されているディレクトリ
        output_dir: 結果を出力するディレクトリ
    """
    
    # ファイルパターンを構築
    file_pattern = os.path.join(input_dir, 
                               f"{where}-{year_pattern}-{month_pattern}-{day_pattern}-{time_range}.csv")
    
    # パターンにマッチするファイルを取得
    matching_files = sorted(glob.glob(file_pattern))
    
    if not matching_files:
        print(f"パターンに一致するファイルが見つかりません: {file_pattern}")
        return
    
    server_type = get_server_type_label(where)
    
    print(f"=== 時間範囲別DNS Magnitude統計分析 (パターンモード) ===")
    print(f"サーバータイプ: {server_type}")
    print(f"時間範囲: {time_range}")
    print(f"パターン: {where}-{year_pattern}-{month_pattern}-{day_pattern}-{time_range}")
    print(f"一致ファイル数: {len(matching_files)}")
    
    # サブドメインごとのMagnitude値を格納
    subdomain_magnitudes = defaultdict(list)
    processed_files = []
    
    for csv_file_path in matching_files:
        try:
            # CSVファイルを読み込み
            df = pd.read_csv(csv_file_path)
            processed_files.append(csv_file_path)
            
            # ファイル名から日付を取得
            date_str = parse_time_magnitude_filename(csv_file_path, where, time_range)
            print(f"処理中: {os.path.basename(csv_file_path)} (レコード数: {len(df)})")
            
            # 各行を処理
            for _, row in df.iterrows():
                # カラム名の確認と取得
                if 'domain' in df.columns:
                    subdomain = str(row['domain']).strip()
                elif 'subdomain' in df.columns:
                    subdomain = str(row['subdomain']).strip()
                else:
                    continue
                
                # magnitudeカラムの確認
                if 'dnsmagnitude' in df.columns:
                    magnitude = float(row['dnsmagnitude'])
                elif 'magnitude' in df.columns:
                    magnitude = float(row['magnitude'])
                else:
                    continue
                
                subdomain_magnitudes[subdomain].append(magnitude)
        
        except Exception as e:
            print(f"エラー: {csv_file_path} の処理中 - {str(e)}")
    
    print(f"\n処理完了: {len(processed_files)}ファイル")
    
    if not subdomain_magnitudes:
        print("処理するデータが見つかりませんでした")
        return
    
    # 統計情報を計算
    subdomain_statistics = {}
    
    for subdomain, magnitudes in subdomain_magnitudes.items():
        if len(magnitudes) == 0:
            continue
        
        stats = {
            'mean': statistics.mean(magnitudes),
            'variance': statistics.variance(magnitudes) if len(magnitudes) > 1 else 0.0,
            'std_dev': statistics.stdev(magnitudes) if len(magnitudes) > 1 else 0.0,
            'median': statistics.median(magnitudes),
            'min': min(magnitudes),
            'max': max(magnitudes),
            'count': len(magnitudes),
            'q25': pd.Series(magnitudes).quantile(0.25),
            'q75': pd.Series(magnitudes).quantile(0.75)
        }
        
        subdomain_statistics[subdomain] = stats
    
    # 結果を保存
    os.makedirs(output_dir, exist_ok=True)
    pattern_str = f"{year_pattern}-{month_pattern}-{day_pattern}"
    output_csv_path = os.path.join(output_dir, 
                                   f"magnitude-time-statistics-{where}-{time_range}-{pattern_str}.csv")
    
    with open(output_csv_path, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'server_type', 'time_range', 'pattern', 'subdomain', 'mean', 'variance', 
            'std_dev', 'median', 'min', 'max', 'q25', 'q75', 'data_points'
        ])
        
        for subdomain, stats in sorted(subdomain_statistics.items(), 
                                      key=lambda x: x[1]['mean'], reverse=True):
            writer.writerow([
                where,
                time_range,
                pattern_str,
                subdomain,
                f"{stats['mean']:.6f}",
                f"{stats['variance']:.6f}",
                f"{stats['std_dev']:.6f}",
                f"{stats['median']:.6f}",
                f"{stats['min']:.6f}",
                f"{stats['max']:.6f}",
                f"{stats['q25']:.6f}",
                f"{stats['q75']:.6f}",
                stats['count']
            ])
    
    print(f"\n統計結果を保存: {output_csv_path}")
    print(f"サーバータイプ: {server_type}")
    print(f"時間範囲: {time_range}")
    print(f"処理サブドメイン数: {len(subdomain_statistics)}")
    
    # 結果サマリーを表示
    print(f"\n=== 上位10サブドメインの統計 ===")
    for rank, (subdomain, stats) in enumerate(
        sorted(subdomain_statistics.items(), key=lambda x: x[1]['mean'], reverse=True)[:10], 1):
        print(f"{rank:2d}. {subdomain:<20}: 平均={stats['mean']:.4f}, "
              f"分散={stats['variance']:.4f}, データ数={stats['count']}")

def main():
    parser = argparse.ArgumentParser(
        description='時間範囲別DNS Magnitude統計分析ツール',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument('--mode', choices=['range', 'pattern'], default='range',
                       help='動作モード (range: 日付範囲, pattern: パターンマッチ)')
    
    # サーバータイプ（必須）
    parser.add_argument('-w', type=int, required=True, choices=[0, 1],
                       help='サーバータイプ (0=権威サーバー, 1=リゾルバ)')
    
    # 時間範囲（必須）
    parser.add_argument('--time-range', required=True,
                       help='時間範囲 (例: 08-18)')
    
    # 日付範囲モード用
    parser.add_argument('--start-date', help='開始日 (YYYY-MM-DD形式)')
    parser.add_argument('--end-date', help='終了日 (YYYY-MM-DD形式)')
    
    # パターンモード用
    parser.add_argument('-y', help='年のパターン (例: 2025, *)')
    parser.add_argument('-m', help='月のパターン (例: 04, *)')
    parser.add_argument('-d', help='日のパターン (例: 01, *)')
    
    # 共通オプション
    parser.add_argument('--input-dir', 
                       default='/home/shimada/analysis/output-time/',
                       help='時間範囲別Magnitudeファイルが格納されているディレクトリ')
    parser.add_argument('--output-dir', 
                       default='/home/shimada/analysis/output-time/statistics/',
                       help='統計結果を出力するディレクトリ')
    
    args = parser.parse_args()
    
    if args.mode == 'range':
        if not args.start_date or not args.end_date:
            print("エラー: 日付範囲モードでは --start-date と --end-date が必要です")
            print(f"使用例: python magnitude-time-statistics.py --mode range --start-date 2025-04-01 --end-date 2025-04-30 -w {args.w} --time-range 08-18")
            return 1
        
        calculate_time_magnitude_statistics_range(
            args.start_date, args.end_date, args.w, args.time_range,
            args.input_dir, args.output_dir
        )
    
    elif args.mode == 'pattern':
        if not all([args.y, args.m, args.d]):
            print("エラー: パターンモードでは -y, -m, -d が必要です")
            print(f"使用例: python magnitude-time-statistics.py --mode pattern -y 2025 -m 04 -d '*' -w {args.w} --time-range 08-18")
            return 1
        
        calculate_time_magnitude_statistics_pattern(
            args.y, args.m, args.d, args.w, args.time_range,
            args.input_dir, args.output_dir
        )
    
    return 0

if __name__ == "__main__":
    exit(main())
