#!/usr/bin/env python3
"""
各サブドメインのDNS Magnitude標準偏差計算・出力ツール

2025年4月の1か月間の1日単位のDNS Magnitudeの変動を標準偏差で表し、
各サブドメインとその標準偏差を並べてコンソールに出力します。

使用方法:
    # リゾルバのデータを分析
    python3 subdomain_stddev.py -w 1

    # 権威サーバーのデータを分析
    python3 subdomain_stddev.py -w 0

    # カスタム期間と入力ディレクトリを指定
    python3 subdomain_stddev.py -w 1 --start-date 2025-04-01 --end-date 2025-04-30 --input-dir /path/to/data

引数:
    -w: サーバータイプ (0=権威サーバー, 1=リゾルバ) [必須]
    --start-date: 開始日 (YYYY-MM-DD形式, デフォルト: 2025-04-01)
    --end-date: 終了日 (YYYY-MM-DD形式, デフォルト: 2025-04-30)
    --input-dir: 入力ディレクトリ (デフォルト: /home/shimada/analysis/output/)
    --sort-by: ソート基準 (std=標準偏差順[デフォルト], mean=平均順, subdomain=サブドメイン名順)
"""

import pandas as pd
import os
import statistics
from collections import defaultdict
from datetime import datetime, timedelta
import argparse


def get_server_type_label(where):
    """サーバータイプのラベルを取得"""
    return "権威サーバー" if where == 0 else "リゾルバ"


def calculate_subdomain_stddev(start_date, end_date, where, input_dir):
    """
    指定した日付範囲のMagnitudeファイルから各サブドメインの標準偏差を計算

    Args:
        start_date: 開始日 (YYYY-MM-DD形式)
        end_date: 終了日 (YYYY-MM-DD形式)
        where: 0=権威サーバー, 1=リゾルバ
        input_dir: Magnitudeファイルが格納されているディレクトリ

    Returns:
        dict: サブドメインごとの統計情報
    """

    # 日付文字列をdatetimeオブジェクトに変換
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    # サブドメインごとのMagnitude値を格納
    subdomain_magnitudes = defaultdict(list)

    processed_files = []
    missing_files = []

    server_type = get_server_type_label(where)

    print(f"=== 各サブドメインのDNS Magnitude標準偏差計算 ===")
    print(f"サーバータイプ: {server_type}")
    print(f"期間: {start_date} から {end_date}")
    print(f"入力ディレクトリ: {input_dir}")
    print()

    # 日付範囲内の各日を処理
    current_dt = start_dt
    while current_dt <= end_dt:
        date_str = current_dt.strftime("%Y-%m-%d")

        # ファイルパスを構築 (形式: {where}-YYYY-MM-DD.csv)
        csv_file_path = os.path.join(input_dir, f"{where}-{date_str}.csv")

        if not os.path.exists(csv_file_path):
            missing_files.append(csv_file_path)
            current_dt += timedelta(days=1)
            continue

        try:
            # CSVファイルを読み込み
            df = pd.read_csv(csv_file_path)
            processed_files.append(csv_file_path)

            # 各行を処理してサブドメインごとのMagnitude値を記録
            for _, row in df.iterrows():
                # カラム名の確認と取得
                if 'subdomain' in df.columns:
                    subdomain = str(row['subdomain']).strip()
                elif 'domain' in df.columns:
                    subdomain = str(row['domain']).strip()
                else:
                    continue

                # magnitudeカラムの確認
                if 'magnitude' in df.columns:
                    magnitude = float(row['magnitude'])
                elif 'dnsmagnitude' in df.columns:
                    magnitude = float(row['dnsmagnitude'])
                else:
                    continue

                subdomain_magnitudes[subdomain].append(magnitude)

        except Exception as e:
            print(f"エラー: {csv_file_path} の処理中 - {str(e)}")

        current_dt += timedelta(days=1)

    print(f"処理完了ファイル数: {len(processed_files)}/{(end_dt - start_dt).days + 1}")
    if missing_files:
        print(f"欠落ファイル数: {len(missing_files)}")
    print()

    if not subdomain_magnitudes:
        print("処理するデータが見つかりませんでした")
        return {}

    # 統計情報を計算
    subdomain_statistics = {}

    for subdomain, magnitudes in subdomain_magnitudes.items():
        if len(magnitudes) == 0:
            continue

        # 統計値を計算
        stats = {
            'mean': statistics.mean(magnitudes),
            'std_dev': statistics.stdev(magnitudes) if len(magnitudes) > 1 else 0.0,
            'count': len(magnitudes),
            'min': min(magnitudes),
            'max': max(magnitudes)
        }

        subdomain_statistics[subdomain] = stats

    return subdomain_statistics


def print_subdomain_stddev(subdomain_statistics, sort_by='std'):
    """
    サブドメインの標準偏差を整形して出力

    Args:
        subdomain_statistics: サブドメインごとの統計情報
        sort_by: ソート基準 ('std', 'mean', 'subdomain')
    """

    if not subdomain_statistics:
        print("表示するデータがありません")
        return

    # ソート
    if sort_by == 'std':
        sorted_items = sorted(subdomain_statistics.items(),
                            key=lambda x: x[1]['std_dev'], reverse=True)
        sort_label = "標準偏差降順"
    elif sort_by == 'mean':
        sorted_items = sorted(subdomain_statistics.items(),
                            key=lambda x: x[1]['mean'], reverse=True)
        sort_label = "平均降順"
    else:  # subdomain
        sorted_items = sorted(subdomain_statistics.items(),
                            key=lambda x: x[0])
        sort_label = "サブドメイン名順"

    print(f"=== サブドメイン別標準偏差一覧 ({sort_label}) ===")
    print(f"総サブドメイン数: {len(subdomain_statistics)}")
    print()
    print(f"{'順位':<6} {'サブドメイン':<25} {'標準偏差':<12} {'平均':<12} {'データ数':<10}")
    print("-" * 75)

    for rank, (subdomain, stats) in enumerate(sorted_items, 1):
        print(f"{rank:<6} {subdomain:<25} {stats['std_dev']:<12.6f} "
              f"{stats['mean']:<12.6f} {stats['count']:<10}")


def print_summary_statistics(subdomain_statistics):
    """
    統計情報のサマリーを出力

    Args:
        subdomain_statistics: サブドメインごとの統計情報
    """

    if not subdomain_statistics:
        return

    # 全サブドメインの標準偏差のリストを作成
    all_stddevs = [stats['std_dev'] for stats in subdomain_statistics.values()]
    all_means = [stats['mean'] for stats in subdomain_statistics.values()]

    print()
    print("=== 全体統計サマリー ===")
    print(f"標準偏差の平均: {statistics.mean(all_stddevs):.6f}")
    print(f"標準偏差の中央値: {statistics.median(all_stddevs):.6f}")
    print(f"標準偏差の最大値: {max(all_stddevs):.6f}")
    print(f"標準偏差の最小値: {min(all_stddevs):.6f}")
    print()
    print(f"Magnitude平均の平均: {statistics.mean(all_means):.6f}")
    print(f"Magnitude平均の中央値: {statistics.median(all_means):.6f}")
    print()

    # 標準偏差が大きいTop 10
    print("=== 標準偏差が大きい上位10サブドメイン（変動が激しい） ===")
    top10_std = sorted(subdomain_statistics.items(),
                       key=lambda x: x[1]['std_dev'], reverse=True)[:10]

    for rank, (subdomain, stats) in enumerate(top10_std, 1):
        print(f"{rank:2}. {subdomain:<25} 標準偏差={stats['std_dev']:.6f}, "
              f"平均={stats['mean']:.6f}, "
              f"範囲=[{stats['min']:.4f}, {stats['max']:.4f}]")

    print()

    # 標準偏差が小さいTop 10
    print("=== 標準偏差が小さい上位10サブドメイン（安定している） ===")
    bottom10_std = sorted(subdomain_statistics.items(),
                          key=lambda x: x[1]['std_dev'])[:10]

    for rank, (subdomain, stats) in enumerate(bottom10_std, 1):
        print(f"{rank:2}. {subdomain:<25} 標準偏差={stats['std_dev']:.6f}, "
              f"平均={stats['mean']:.6f}, "
              f"範囲=[{stats['min']:.4f}, {stats['max']:.4f}]")


def main():
    parser = argparse.ArgumentParser(
        description='各サブドメインのDNS Magnitude標準偏差計算・出力ツール',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )

    # サーバータイプ（必須）
    parser.add_argument('-w', type=int, required=True, choices=[0, 1],
                       help='サーバータイプ (0=権威サーバー, 1=リゾルバ)')

    # 日付範囲
    parser.add_argument('--start-date', default='2025-04-01',
                       help='開始日 (YYYY-MM-DD形式, デフォルト: 2025-04-01)')
    parser.add_argument('--end-date', default='2025-04-30',
                       help='終了日 (YYYY-MM-DD形式, デフォルト: 2025-04-30)')

    # 入力ディレクトリ
    parser.add_argument('--input-dir',
                       default='/home/shimada/analysis/output/',
                       help='Magnitudeファイルが格納されているディレクトリ')

    # ソート順
    parser.add_argument('--sort-by', choices=['std', 'mean', 'subdomain'],
                       default='std',
                       help='ソート基準 (std=標準偏差順, mean=平均順, subdomain=サブドメイン名順)')

    args = parser.parse_args()

    # 標準偏差を計算
    subdomain_statistics = calculate_subdomain_stddev(
        args.start_date, args.end_date, args.w, args.input_dir
    )

    # 結果を出力
    print_subdomain_stddev(subdomain_statistics, args.sort_by)
    print_summary_statistics(subdomain_statistics)

    return 0


if __name__ == "__main__":
    exit(main())
