import func
import argparse

def main():
    parser = argparse.ArgumentParser(description='DNS QType比率分析（日別）')
    parser.add_argument('-y', help='year')
    parser.add_argument('-m', help='month')
    parser.add_argument('-d', help='day')
    parser.add_argument('-w', help='0は権威1はリゾルバ')
    args = parser.parse_args()
    
    year = args.y
    month = args.m
    day = args.d
    where = args.w
    
    # func.pyの関数を使用してqtype比率分析を実行
    func.qtype_ratio(year, month, day, where)

if __name__ == "__main__":
    main()
