import func2024
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-y', help='year', required=True)
    parser.add_argument('-m', help='month', required=True)
    parser.add_argument('-d', help='day', required=True)
    args = parser.parse_args()

    year = args.y
    month = args.m
    day = args.d

    # func2024の関数を使用して送信先IPアドレス数をカウント
    count = func2024.count_dst_ip(year, month, day)
    
    if count is not None:
        print(f"Unique destination IP count: {count}")

if __name__ == "__main__":
    main()
