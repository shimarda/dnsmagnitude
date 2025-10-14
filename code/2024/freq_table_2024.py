import func2024
import argparse

def main():
    parser = argparse.ArgumentParser(description='度数分布表を作成する')
    parser.add_argument('file_path', help='CSVファイルのパス')
    parser.add_argument('--threshold', type=float, default=5.0, help='閾値（デフォルト: 5.0）')
    args = parser.parse_args()

    # func2024の関数を使用して度数分布表を作成
    freq_table, percent_above_threshold = func2024.make_frequency_table(args.file_path, args.threshold)
    
    if freq_table is not None:
        print("\n=== 度数分布表（降順） ===")
        print(freq_table)
        print(f"\n閾値{args.threshold}以上の割合: {percent_above_threshold:.2f}%")
    else:
        print("度数分布表の作成に失敗しました。")

if __name__ == "__main__":
    main()
