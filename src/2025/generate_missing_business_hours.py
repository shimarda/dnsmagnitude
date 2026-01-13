#!/usr/bin/env python3
"""
業務時間（8-18時）データが欠落している日を検出し、
既存の終日データがある日について業務時間データを生成するスクリプト

使用方法:
    # ドライラン（実行せず欠落日を表示）
    python3 generate_missing_business_hours.py --dry-run
    
    # 実際に生成
    python3 generate_missing_business_hours.py
    
    # 権威サーバー(0)のみ
    python3 generate_missing_business_hours.py --where 0
    
    # リゾルバ(1)のみ
    python3 generate_missing_business_hours.py --where 1
"""

import os
import re
import glob
import subprocess
import argparse
from datetime import datetime

# パス設定
OUTPUT_DIR = "/home/shimada/analysis/output"
OUTPUT_DIR_TIME = "/home/shimada/analysis/output-time"
DNSMAGNITUDE_SCRIPT = "/home/shimada/dns-dashboard/src/2025/dnsmagnitude-time.py"

def get_dates_with_daily_data():
    """終日データがある日付を取得"""
    dates = {}  # {date_str: [where_values]}
    
    for f in glob.glob(os.path.join(OUTPUT_DIR, "*.csv")):
        basename = os.path.basename(f)
        # パターン: 0-2025-01-01.csv または 1-2025-01-01.csv
        m = re.match(r"^(\d)-(\d{4}-\d{2}-\d{2})\.csv$", basename)
        if m:
            where = int(m.group(1))
            date_str = m.group(2)
            if date_str not in dates:
                dates[date_str] = []
            dates[date_str].append(where)
    
    return dates

def get_dates_with_business_data():
    """業務時間データがある日付を取得"""
    dates = {}  # {date_str: [where_values]}
    
    for f in glob.glob(os.path.join(OUTPUT_DIR_TIME, "*-08-18.csv")):
        basename = os.path.basename(f)
        # パターン: 0-2025-01-01-08-18.csv
        m = re.match(r"^(\d)-(\d{4}-\d{2}-\d{2})-08-18\.csv$", basename)
        if m:
            where = int(m.group(1))
            date_str = m.group(2)
            if date_str not in dates:
                dates[date_str] = []
            dates[date_str].append(where)
    
    return dates

def find_missing_dates(where_filter=None):
    """欠落している日付とwhereの組み合わせを検出"""
    daily_data = get_dates_with_daily_data()
    business_data = get_dates_with_business_data()
    
    missing = []
    
    for date_str, where_list in sorted(daily_data.items()):
        for where in where_list:
            if where_filter is not None and where != where_filter:
                continue
            
            # 業務時間データがない場合
            if date_str not in business_data or where not in business_data[date_str]:
                missing.append((date_str, where))
    
    return missing

def generate_business_data(date_str, where, dry_run=False):
    """指定日の業務時間データを生成"""
    # 日付をパース
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")
    
    cmd = [
        "python3", DNSMAGNITUDE_SCRIPT,
        "-y", year,
        "-m", month,
        "-d", day,
        "-w", str(where),
        "--start-hour", "8",
        "--end-hour", "18"
    ]
    
    where_name = "権威" if where == 0 else "リゾルバ"
    print(f"[{where_name}] {date_str}: ", end="")
    
    if dry_run:
        print("(ドライラン - スキップ)")
        return True
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if result.returncode == 0:
            print("✓ 完了")
            return True
        else:
            print(f"✗ エラー: {result.stderr[:200]}")
            return False
    except subprocess.TimeoutExpired:
        print("✗ タイムアウト")
        return False
    except Exception as e:
        print(f"✗ 例外: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='欠落している業務時間データを生成')
    parser.add_argument('--dry-run', action='store_true', 
                        help='実行せずに対象日を表示のみ')
    parser.add_argument('--where', type=int, choices=[0, 1],
                        help='0=権威サーバーのみ, 1=リゾルバのみ')
    parser.add_argument('--limit', type=int, default=None,
                        help='処理する最大件数')
    args = parser.parse_args()
    
    print("=" * 60)
    print("業務時間（8-18時）データ生成スクリプト")
    print("=" * 60)
    
    # 欠落日を検出
    missing = find_missing_dates(args.where)
    
    if args.limit:
        missing = missing[:args.limit]
    
    print(f"\n対象件数: {len(missing)} 件")
    
    if not missing:
        print("すべての日付で業務時間データが生成済みです。")
        return
    
    if args.dry_run:
        print("\n[ドライラン] 以下の日付が対象です:")
        for date_str, where in missing:
            where_name = "権威" if where == 0 else "リゾルバ"
            print(f"  - {date_str} ({where_name})")
        print(f"\n実行するには --dry-run を外してください。")
        return
    
    print("\n生成開始...")
    success = 0
    failed = 0
    
    for date_str, where in missing:
        if generate_business_data(date_str, where, dry_run=args.dry_run):
            success += 1
        else:
            failed += 1
    
    print("\n" + "=" * 60)
    print(f"完了: 成功 {success} 件, 失敗 {failed} 件")
    print("=" * 60)

if __name__ == "__main__":
    main()
