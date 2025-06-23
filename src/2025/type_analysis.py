import func
import argparse


# メイン処理
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='DNS QType分析')
    parser.add_argument('-y', help='year')
    parser.add_argument('-m', help='month')
    parser.add_argument('-d', help='day')
    parser.add_argument('-w', help='0は権威1はリゾルバ')
    args = parser.parse_args()
    
    year = args.y
    month = args.m
    day = args.d
    where = args.w
    
    func.qtype_ratio(year, month, day, where)