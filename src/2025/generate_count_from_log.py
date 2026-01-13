#!/usr/bin/env python3
"""
processed_files.log に記載された日付のうち、業務時間クエリカウントデータが
まだ生成されていないものを検出し、生成するスクリプト

使用方法:
    # ドライラン（対象日を表示のみ）
    python3 generate_count_from_log.py --dry-run
    
    # 実行
    python3 generate_count_from_log.py
"""

import os
import re
import glob
import subprocess
import argparse
from datetime import datetime

# パス設定
PROCESSED_LOG = "/home/shimada/dns-dashboard/analysis/logs/processed_files.log"
OUTPUT_DIR_TIME = "/home/shimada/analysis/output-time"
QUERY_COUNT_SCRIPT = "/home/shimada/dns-dashboard/src/2025/query-count.py"

def get_dates_from_log():
    """processed_files.log から日付を抽出"""
    dates = set()
    
    if not os.path.exists(PROCESSED_LOG):
        print(f"ログファイルが見つかりません: {PROCESSED_LOG}")
        return dates
    
    with open(PROCESSED_LOG, 'r') as f:
        for line in f:
            m = re.search(r'dump-(\d{8})\d{4}\.gz', line)
            if m:
                date_str = m.group(1)
                try:
                    dt = datetime.strptime(date_str, "%Y%m%d")
                    dates.add(dt.strftime("%Y-%m-%d"))
                except ValueError:
                    continue
    
    return dates

def get_dates_with_count_data():
    """業務時間クエリカウントデータがある日付を取得"""
    dates = {}
    
    for f in glob.glob(os.path.join(OUTPUT_DIR_TIME, "count-*-08-18.csv")):
        basename = os.path.basename(f)
        m = re.match(r"^count-(\d)-(\d{4}-\d{2}-\d{2})-08-18\.csv$", basename)
        if m:
            where = int(m.group(1))
            date_str = m.group(2)
            if date_str not in dates:
                dates[date_str] = []
            dates[date_str].append(where)
    
    return dates

def find_missing():
    """欠落しているデータを検索"""
    log_dates = get_dates_from_log()
    count_data = get_dates_with_count_data()
    
    missing = []
    
    for date_str in sorted(log_dates):
        for where in [0, 1]:
            if date_str not in count_data or where not in count_data[date_str]:
                missing.append((date_str, where))
    
    return missing

def generate_count_data(date_str, where, dry_run=False):
    """指定日の業務時間クエリカウントデータを生成"""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")
    
    cmd = [
        "python3", QUERY_COUNT_SCRIPT,
        "-y", year,
        "-m", month,
        "-d", day,
        "-w", str(where),
        "--start-hour", "8",
        "--end-hour", "18"
    ]
    
    where_name = "権威" if where == 0 else "リゾルバ"
    print(f"[{where_name}] {date_str}: ", end="", flush=True)
    
    if dry_run:
        print("(ドライラン)")
        return True
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if result.returncode == 0:
            print("✓ 完了")
            return True
        else:
            print(f"✗ エラー")
            return False
    except subprocess.TimeoutExpired:
        print("✗ タイムアウト")
        return False
    except Exception as e:
        print(f"✗ 例外: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='processed_files.log から欠落業務時間クエリカウントデータを生成')
    parser.add_argument('--dry-run', action='store_true', help='実行せずに対象日を表示')
    parser.add_argument('--limit', type=int, help='処理する最大件数')
    args = parser.parse_args()
    
    print("=" * 60)
    print("業務時間クエリカウントデータ生成（processed_files.log ベース）")
    print("=" * 60)
    
    missing = find_missing()
    
    if args.limit:
        missing = missing[:args.limit]
    
    print(f"\n対象件数: {len(missing)} 件")
    
    if not missing:
        print("すべて生成済みです。")
        return
    
    if args.dry_run:
        print("\n[ドライラン] 対象日:")
        for date_str, where in missing:
            where_name = "権威" if where == 0 else "リゾルバ"
            print(f"  - {date_str} ({where_name})")
        return
    
    print("\n生成開始...")
    success = 0
    failed = 0
    
    for date_str, where in missing:
        if generate_count_data(date_str, where):
            success += 1
        else:
            failed += 1
    
    print(f"\n完了: 成功 {success}, 失敗 {failed}")

if __name__ == "__main__":
    main()
