# import func
# import argparse


# # メイン処理
# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(description='DNS QType分析')
#     parser.add_argument('-y', help='year')
#     parser.add_argument('-m', help='month')
#     parser.add_argument('-d', help='day')
#     parser.add_argument('-w', help='0は権威1はリゾルバ')
#     args = parser.parse_args()
    
#     year = args.y
#     month = args.m
#     day = args.d
#     where = args.w
    
#     func.qtype_ratio(year, month, day, where)

import func
import argparse


# メイン処理
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='DNS QType全体分析')
    parser.add_argument('-y', help='year (例: 2025)')
    parser.add_argument('-m', help='month (例: 01)')
    parser.add_argument('-d', help='day (例: 15)')
    parser.add_argument('-w', help='0は権威1はリゾルバ')
    parser.add_argument('--start-date', help='開始日 (YYYY-MM-DD形式、例: 2025-01-01)')
    parser.add_argument('--end-date', help='終了日 (YYYY-MM-DD形式、例: 2025-01-31)')
    parser.add_argument('--mode', choices=['single', 'range'], default='single',
                       help='single: 単一日付パターン, range: 日付範囲指定')
    args = parser.parse_args()
    
    where = args.w
    
    if args.mode == 'range':
        # 日付範囲指定モード
        if not args.start_date or not args.end_date:
            print("エラー: 日付範囲モードでは --start-date と --end-date の両方が必要です")
            print("使用例: python script.py --mode range --start-date 2025-01-01 --end-date 2025-01-31 -w 0")
            exit(1)
        
        if not where:
            print("エラー: -w (0は権威1はリゾルバ) が必要です")
            exit(1)
        
        print(f"日付範囲モード: {args.start_date} から {args.end_date}")
        func.qtype_ratio_total_by_date_range(args.start_date, args.end_date, where)
    
    else:
        # 単一日付パターンモード
        if not all([args.y, args.m, args.d, args.w]):
            print("エラー: -y, -m, -d, -w の全てが必要です")
            print("使用例: python script.py -y 2025 -m 01 -d 15 -w 0")
            exit(1)
        
        year = args.y
        month = args.m
        day = args.d
        
        print(f"単一日付パターンモード: {year}-{month}-{day}")
        func.qtype_ratio_total(year, month, day, where)