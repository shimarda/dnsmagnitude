import pandas as pd
import matplotlib.pyplot as plt
import matplotlib as mpl
import argparse

def make(average_file, distribution_file, output_file=None):

    df_avg = pd.read_csv(average_file)
    df_dist = pd.read_csv(distribution_file)   # distribution.csv: domain, distribution

    # ドメインをキーにマージ（共通のドメインがある場合）
    df_merged = pd.merge(df_avg, df_dist, on='domain', how='inner')

    plt.figure(figsize=(8, 6))

    # カラーマップのコピーを作成して、under値（vmin未満の値）の色を白に設定
    cmap = plt.cm.get_cmap('Spectral_r').copy()
    cmap.set_under('white')

    # ヒートマップ作成：vmin=0.001を指定することで、0のセルがunder扱いになり白に
    counts, xedges, yedges, im = plt.hist2d(
        df_merged['average'],
        df_merged['distribution'],
        bins=[30, 30],
        range=[[0, 10], [0, 5]],
        cmap=cmap,
        vmin=0.001  # 0はこの値より小さいので、under色（白）になる
    )

    plt.xlabel("Average", fontsize=24)
    plt.ylabel("Distribution", fontsize=24)
    plt.title("Heatmap", fontsize=24)
    plt.tick_params(labelsize=24)

    cb = plt.colorbar(im)
    cb.set_label("Count")

    plt.tight_layout()

    # 保存前にshow()を呼び出すと図が消えるので、保存は先に行うか
    if output_file is not None:
        plt.savefig(output_file)
    
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
