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

    # === フィギュア & 軸オブジェクトを作成 ===
    fig, ax = plt.subplots(figsize=(8, 6))

    # === 散布図の作成 ===
    # 透明度(alpha)を低く設定することで、点が重なるほど濃く表示されます。
    ax.scatter(df_merged['average'], df_merged['distribution'],
               color='blue', alpha=0.1)

    # === 各点にドメイン名を表示 ===
    # 少しオフセットしてテキストを表示することで、点とテキストが重なりにくくなります。
    for idx, row in df_merged.iterrows():
        ax.text(row['average'] + 0.05, row['distribution'] + 0.05,
                str(row['domain']),
                fontsize=8, color='black')

    # === タイトル・軸ラベル ===
    ax.set_title("correlation")
    ax.set_xlabel("Average")
    ax.set_ylabel("Distribution")

    # === 軸範囲を設定 ===
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 5)

    # === 余白を調整して下端がギリギリにならないようにする ===
    fig.subplots_adjust(bottom=0.15, top=0.95, left=0.10, right=0.95)

    # === 目盛りのパディングを設定 (縦軸の文字と軸の間隔) ===
    ax.tick_params(axis='y', pad=5)

    # === グリッドの表示 ===
    ax.grid(True)

    # === 相関係数の計算 ===
    corr_pearson = df_merged['average'].corr(df_merged['distribution'], method='pearson')
    print(f"Pearsonの相関係数: {corr_pearson:.4f}")

    # === ファイル保存 (output_file が指定されている場合) ===
    if output_file:
        fig.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"プロットを保存しました: {output_file}")

    # === プロット表示 ===
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
    output_file = args.output  # ここにファイル名が入る
    visualize = args.verbose

    make(average_file, distribution_file, output_file)
