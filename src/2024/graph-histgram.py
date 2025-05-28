#!/usr/bin/env python3
import pandas as pd
import numpy as np
import math
import argparse
import matplotlib.pyplot as plt

def make_histogram(file_path, output_file):
    """
    CSVファイル (file_path) から average 列を読み込み、
    階級幅を1としたヒストグラムを作成する。
    ヒストグラムは横軸に平均値の区間、縦軸に度数を表示する。
    もし最小値が0未満なら0に丸める（負の区間を防止）。
    """
    # CSV読み込み（average 列を float として読み込む）
    df = pd.read_csv(file_path, dtype={'average': float}, low_memory=False)

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

    # ヒストグラムの作成
    plt.figure(figsize=(8, 6))
    plt.hist(df['average'], bins=bin_edges, edgecolor='black')
    plt.xlabel("Average", fontsize=16)
    plt.ylabel("Frequency", fontsize=16)
    plt.title("Histogram", fontsize=16)
    plt.xticks(bin_edges)  # x軸にビンの境界値を表示
    plt.grid(axis='y', alpha=0.75)
    plt.tick_params(labelsize=16)

    # 出力ファイルが指定されていれば保存
    if output_file:
        plt.savefig(output_file, bbox_inches='tight')
        print(f"ヒストグラムを {output_file} として保存しました。")
    
    plt.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="ヒストグラムを作成し、保存する（average 列、階級幅1、負の値は0に補正）"
    )
    parser.add_argument('-f', '--file', required=True, help="入力CSVファイルのパス")
    parser.add_argument('-o', '--output', default=None, help="保存する画像ファイルのパス（例: histogram.png）")
    args = parser.parse_args()

    make_histogram(args.file, args.output)
