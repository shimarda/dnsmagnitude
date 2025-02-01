#!/usr/bin/env python3
import pandas as pd
import numpy as np
import math
import argparse

def make_table(file_path):
    """
    CSVファイル (file_path) から distribution 列を読み込み、
    スタージェスの式に基づいてビン数を決定し、
    度数分布表を作成する。
    ビンは大きい方(右端)から順に表示し、
    もし最小値が0未満なら0に丸める（負の区間を防止）。
    """
    # ▼ 1. CSV読み込み
    #    distribution 列を float で扱う (変換できない場合は NaN になる)
    df = pd.read_csv(file_path, dtype={'distribution': float}, low_memory=False)

    # ▼ 2. 最小値・最大値を取得
    min_val = df['distribution'].min()
    max_val = df['distribution'].max()

    # 「分散は負にならない前提」なら、最小値が0より小さければ0に補正
    if min_val < 0:
        min_val = 0.0

    # ▼ 3. データ個数に基づいてビン数を決定（スタージェスの式）
    n = len(df)
    bins = math.ceil(math.log2(n) + 1)
    print(f"データ個数: {n}, 使用するビン数: {bins}")

    # ▼ 4. ビン境界 (等間隔) の作成
    bin_edges = np.linspace(min_val, max_val, bins + 1)

    # ▼ 5. pd.cut でビン区分
    df['bin'] = pd.cut(df['distribution'], bins=bin_edges, include_lowest=True)

    # ▼ 6. 昇順(小→大)の区間順で度数を集計
    freq_table_asc = df['bin'].value_counts(sort=False)

    # ▼ 7. 降順(大→小)に並べ替えて度数・累積度数・相対度数を計算
    freq_table_desc = freq_table_asc.sort_index(ascending=False)

    freq_values_desc = freq_table_desc.values
    total_count = freq_values_desc.sum()

    # 大きい区間から順に積み上げる累積度数
    cum_freq_desc = freq_values_desc.cumsum()
    # 相対度数
    rel_freq_desc = freq_values_desc / total_count
    # 累積相対度数
    cum_rel_freq_desc = rel_freq_desc.cumsum()

    rows = []
    for i, interval in enumerate(freq_table_desc.index):
        left_edge = interval.left
        right_edge = interval.right
        freq = freq_values_desc[i]
        cumf = cum_freq_desc[i]
        relf = rel_freq_desc[i]
        cumrelf = cum_rel_freq_desc[i]

        # 表示上、0 未満は 0 にそろえてしまう (念のため)
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
