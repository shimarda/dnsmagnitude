import pandas as pd
import matplotlib.pyplot as plt
import argparse

def make(average_file, distribution_file, output_file=None):
    # === CSVファイルを読み込み ===
    df_avg = pd.read_csv(average_file)         # domain, average
    df_dist = pd.read_csv(distribution_file)   # domain, distribution

    # === on='domain' でマージ ===
    df_merged = pd.merge(df_avg, df_dist, on='domain', how='inner')

    # === 結合結果を確認 ===
    print("=== Merged DataFrame ===")
    print(df_merged)

    # === 散布図の作成 ===
    plt.figure(figsize=(8, 6))
    plt.scatter(df_merged['average'], df_merged['distribution'], color='blue', alpha=0.7)
    
    # ドメイン名をラベルとして描画していた部分を削除
    # for i, row in df_merged.iterrows():
    #     plt.text(row['average'], row['distribution'], row['domain'],
    #              fontsize=9, ha='right', va='bottom')

    plt.title("correlation")
    plt.xlabel("Average")
    plt.ylabel("Distribution")
    plt.grid(True)

    # === 相関係数の計算 ===
    corr_pearson = df_merged['average'].corr(df_merged['distribution'], method='pearson')
    print(f"Pearsonの相関係数: {corr_pearson:.4f}")

    # === ファイル保存 (output_file が指定されている場合) ===
    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"プロットを保存しました: {output_file}")

    # === プロット表示 ===
    plt.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', help="平均値が入ったCSVファイルパス", required=True)
    parser.add_argument('-d', help="分散値が入ったCSVファイルパス", required=True)
    parser.add_argument('-o', '--output', help="保存先の画像ファイル名 (例: plot.png)")

    args = parser.parse_args()

    average_file = args.a
    distribution_file = args.d
    output_file = args.output  # ここにファイル名が入る

    make(average_file, distribution_file, output_file)
