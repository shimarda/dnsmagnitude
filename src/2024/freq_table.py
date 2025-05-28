#!/usr/bin/env python3
import pandas as pd
import numpy as np
import math
import argparse

def make_table(file_path):
    """
    CSVファイル (file_path) から distribution 列を読み込み、
    階級幅を1とした度数分布表を作成する。
    ビンは大きい方(右端)から順に表示し、
    もし最小値が0未満なら0に丸める（負の区間を防止）。
    また、平均が5以上の行が全体の何パーセントかも計算する。
    """
    # CSV読み込み（average 列を float として読み込む）
    df = pd.read_csv(file_path, dtype={'average': float}, low_memory=False)
    
    # 追加: 平均が5以上の行の割合を計算
    total_rows = len(df)
    if total_rows > 0:
        count_ge5 = (df['average'] >= 5).sum()
        percent_ge5 = count_ge5 / total_rows * 100
        print(f"平均が5以上のものは {percent_ge5:.2f}% です。")
    else:
        print("CSVファイルにデータがありません。")
    
    # 最小値・最大値の取得
    min_val = df['average'].min()
    max_val = df['average'].max()

    # 最小値が0未満なら0に補正
    if min_val < 0:
        min_val = 0.0

    # 階級幅を1にするため、min_val を下方向に切り捨て、max_val を上方向に切り上げた端点からビンを作成
    min_edge = 0 if min_val < 0 else math.floor(min_val)
    max_edge = math.ceil(max_val)
    bin_edges = np.arange(min_edge, max_edge + 1, 1)  # 1刻みのビン端点
    print(f"bin_edges: {bin_edges}")

    # ※ pd.cut の対象列が 'dnsmagnitude' となっていましたが、CSVで読み込んだのは 'average' 列なので、
    #    ここは 'average' 列を使うように修正（必要に応じて）
    df['bin'] = pd.cut(df['average'], bins=bin_edges, include_lowest=True)

    # 度数分布表の作成（昇順→降順に並べ替え）
    freq_table_asc = df['bin'].value_counts(sort=False)
    freq_table_desc = freq_table_asc.sort_index(ascending=False)
    freq_values_desc = freq_table_desc.values
    total_count = freq_values_desc.sum()

    # 大きい区間から順に積み上げる累積度数の計算（必要に応じて利用）
    cum_freq_desc = freq_values_desc.cumsum()
    rel_freq_desc = freq_values_desc / total_count
    cum_rel_freq_desc = rel_freq_desc.cumsum()

    rows = []
    for i, interval in enumerate(freq_table_desc.index):
        left_edge = interval.left
        right_edge = interval.right
        freq = freq_values_desc[i]
        cumf = cum_freq_desc[i]
        relf = rel_freq_desc[i]
        cumrelf = cum_rel_freq_desc[i]

        # 表示上、左端が0未満なら0に補正
        if left_edge < 0:
            left_edge = 0.0

        rows.append({
            "bin_range": f"[{left_edge:.4f}, {right_edge:.4f})",
            "freq": freq,
            # 必要に応じて累積度数や相対度数も追加可能:
            # "cum_freq": cumf,
            # "rel_freq": relf,
            # "cum_rel_freq": cumrelf,
        })

    dist_df = pd.DataFrame(rows)
    print(dist_df.to_string(index=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="度数分布表を作成し、大きいビンから順に表示する（負の値は0に補正）"
    )
    parser.add_argument('-f', '--file', required=True, help="入力CSVファイルのパス")
    args = parser.parse_args()

    make_table(args.file)
