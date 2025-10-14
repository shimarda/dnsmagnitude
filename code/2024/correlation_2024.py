import func2024
import argparse
import pandas as pd
import matplotlib.pyplot as plt

def main():
    parser = argparse.ArgumentParser(description='平均値と分散の相関分析')
    parser.add_argument('average_file', help='平均値CSVファイルのパス')
    parser.add_argument('distribution_file', help='分散CSVファイルのパス')
    parser.add_argument('--output-file', help='出力ファイル名（指定しない場合は表示のみ）')
    parser.add_argument('--xlim', type=int, default=10, help='X軸の最大値（デフォルト: 10）')
    parser.add_argument('--ylim', type=int, default=5, help='Y軸の最大値（デフォルト: 5）')
    args = parser.parse_args()

    # CSVファイルを読み込み
    df_avg = pd.read_csv(args.average_file)         # domain, average
    df_dist = pd.read_csv(args.distribution_file)   # domain, distribution

    # on='domain' でマージ
    df_merged = pd.merge(df_avg, df_dist, on='domain', how='inner')

    # 結合結果を確認
    print("=== Merged DataFrame ===")
    print(df_merged)

    fig, ax = plt.subplots(figsize=(8, 6))

    # 散布図の作成 
    ax.scatter(df_merged['average'], df_merged['distribution'],
               color='blue', alpha=0.1)

    # 各点にドメイン名を表示
    # 少しオフセットしてテキストを表示することで、点とテキストが重なりにくくなります。
    for idx, row in df_merged.iterrows():
        ax.text(row['average'] + 0.05, row['distribution'] + 0.05,
                str(row['domain']),
                fontsize=8, color='black')

    # タイトル・軸ラベル
    ax.set_title('Distribution Analysis')
    ax.set_xlabel('Average')
    ax.set_ylabel('Variance')

    # 軸範囲を設定
    ax.set_xlim(0, args.xlim)
    ax.set_ylim(0, args.ylim)

    # 余白を調整して下端がギリギリにならないようにする
    fig.subplots_adjust(bottom=0.15, top=0.95, left=0.10, right=0.95)

    # 目盛りのパディングを設定 (縦軸の文字と軸の間隔)
    ax.tick_params(axis='y', pad=5)

    ax.grid(True)

    # 相関係数の計算
    corr_pearson = df_merged['average'].corr(df_merged['distribution'], method='pearson')
    print(f"Pearsonの相関係数: {corr_pearson:.4f}")

    # 出力ファイルが指定されている場合は保存、そうでなければ表示
    if args.output_file:
        # func2024の出力ディレクトリに保存
        output_dir = func2024.ensure_output_dir("graphs")
        output_path = f"{output_dir}/{args.output_file}"
        plt.savefig(output_path)
        print(f"グラフを保存しました: {output_path}")
    else:
        plt.show()

if __name__ == "__main__":
    main()
