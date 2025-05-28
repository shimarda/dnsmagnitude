import pandas as pd
import matplotlib.pyplot as plt
import argparse

def make(average_file, distribution_file, output_file=None):

    df_avg = pd.read_csv(average_file)         # CSVは domain, average の列を持つ
    df_dist = pd.read_csv(distribution_file)     # CSVは domain, distribution の列を持つ

    df_merged = pd.merge(df_avg, df_dist, on='domain', how='inner')
    print("=== Merged DataFrame ===")
    print(df_merged)

    fig, ax = plt.subplots(figsize=(8, 6))

    counts, xedges, yedges, im = ax.hist2d(
        df_merged['average'],
        df_merged['distribution'],
        bins=[30, 30],
        range=[[0, 10], [0, 5]],
        cmap='Spectral_r'
    )
    cb = plt.colorbar(im, ax=ax)
    cb.set_label("Count")

    ax.scatter(df_merged['average'], df_merged['distribution'], color='blue', alpha=0.1)


    # for idx, row in df_merged.iterrows():
    #     ax.text(row['average'] + 0.05, row['distribution'] + 0.05,
    #             str(row['domain']),
    #             fontsize=8, color='black')

    ax.set_title("Heatmap")
    ax.set_xlabel("Average")
    ax.set_ylabel("Distribution")
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 5)

    # === Pearsonの相関係数を計算して出力 ===
    corr_pearson = df_merged['average'].corr(df_merged['distribution'], method='pearson')
    print(f"Pearsonの相関係数: {corr_pearson:.4f}")

    plt.tight_layout()

    # === 出力ファイルが指定されている場合、ファイルに保存 ===
    if output_file:
        fig.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"プロットを保存しました: {output_file}")

    plt.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', help="平均値が入ったCSVファイルパス", required=True)
    parser.add_argument('-d', help="分散値が入ったCSVファイルパス", required=True)
    parser.add_argument('-o', '--output', help="保存先の画像ファイル名 (例: plot.png)")
    parser.add_argument('-v', '--verbose', help="冗長な出力を表示")
    args = parser.parse_args()

    average_file = args.a
    distribution_file = args.d
    output_file = args.output

    make(average_file, distribution_file, output_file)
