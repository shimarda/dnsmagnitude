import pandas as pd
import matplotlib.pyplot as plt
import argparse

def make(average_file, distribution_file, output_file=None):

    df_avg = pd.read_csv(average_file)
    df_dist = pd.read_csv(distribution_file)   # distribution.csv: domain,distribution

    # ドメインをキーにマージ（共通のドメインがある場合）
    df_merged = pd.merge(df_avg, df_dist, on='domain', how='inner')


    plt.figure(figsize=(8, 6))

    counts, xedges, yedges, im = plt.hist2d(
        df_merged['average'],
        df_merged['distribution'],
        bins=[30, 30],
        range=[[0, 10], [0, 1]],
        cmap='Spectral_r'      # カラーマップ
    )

    plt.xlabel("Average")
    plt.ylabel("Distribution")
    plt.title("Heatmap")

    cb = plt.colorbar(im)
    cb.set_label("Count")

    plt.tight_layout()
    plt.show()


    plt.savefig(output_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('-a', help="平均値が入ったCSVファイルパス", required=True)
    parser.add_argument('-d', help="分散値が入ったCSVファイルパス", required=True)
    parser.add_argument('-o', '--output', help="保存先の画像ファイル名 (例: plot.png)")
    parser.add_argument('-v', '--verbose', help="冗長な出力を表示")
    
    args = parser.parse_args()

    average_file = args.a
    distribution_file = args.d
    output_file = args.output  # ここにファイル名が入る
    visualize = args.verbose

    make(average_file, distribution_file, output_file)
