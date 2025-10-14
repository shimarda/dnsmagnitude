#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
日次CSV（クエリ数 / DNS Magnitude）から月次統計を作成し、相関・散布図・箱ひげ図・ヒートマップを出力。

入力ファイルの形式：
- クエリ数: {count_dir}/count-{where}-YYYY-MM-DD.csv  （列: day,domain,count）
- Magnitude: {mag_dir}/{where}-YYYY-MM-DD.csv          （列: day,domain,dnsmagnitude）
  where: 0 = 権威DNS, 1 = リゾルバ

出力：
- corr_{where}.txt（Pearson, Spearman）
- scatter_queries_vs_magnitude_{where}.png
- boxplot_magnitude_mean_auth_vs_resolver.png
- heatmap_magnitude_top{N}.png
"""

import os
import argparse
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# ======== I/O ========

def read_daily_counts_for_range(count_dir: str, where: int, start_date: str, end_date: str) -> pd.DataFrame:
    """
    指定期間の count-{where}-YYYY-MM-DD.csv を集約
    出力カラム: ['date','subdomain','count']
    """
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt   = datetime.strptime(end_date,   "%Y-%m-%d")

    rows = []
    cur = start_dt
    missing = 0
    while cur <= end_dt:
        ymd = cur.strftime("%Y-%m-%d")
        path = os.path.join(count_dir, f"count-{where}-{ymd}.csv")
        if not os.path.exists(path):
            missing += 1
            cur += timedelta(days=1)
            continue
        try:
            df = pd.read_csv(path)
            # 想定列: day,domain,count
            # 余分な列があっても domain,count を使う
            if "domain" not in df.columns or "count" not in df.columns:
                raise ValueError(f"必要列がありません: {path}")
            tmp = df[["domain", "count"]].copy()
            tmp["date"] = ymd
            tmp.rename(columns={"domain":"subdomain"}, inplace=True)
            rows.append(tmp[["date","subdomain","count"]])
        except Exception as e:
            print(f"[WARN] 読み込み失敗: {path} -> {e}")
        cur += timedelta(days=1)

    if missing > 0:
        print(f"[INFO] countファイル欠落: {missing}日分（where={where})")
    if not rows:
        return pd.DataFrame(columns=["date","subdomain","count"])
    out = pd.concat(rows, ignore_index=True)
    out["count"] = pd.to_numeric(out["count"], errors="coerce")
    out = out.dropna(subset=["subdomain","count"])
    return out


def read_daily_magnitude_for_range(mag_dir: str, where: int, start_date: str, end_date: str) -> pd.DataFrame:
    """
    指定期間の {where}-YYYY-MM-DD.csv を集約
    出力カラム: ['date','subdomain','magnitude']
    """
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt   = datetime.strptime(end_date,   "%Y-%m-%d")

    rows = []
    cur = start_dt
    missing = 0
    while cur <= end_dt:
        ymd = cur.strftime("%Y-%m-%d")
        path = os.path.join(mag_dir, f"{where}-{ymd}.csv")
        if not os.path.exists(path):
            missing += 1
            cur += timedelta(days=1)
            continue
        try:
            df = pd.read_csv(path)
            # 想定列: day,domain,dnsmagnitude
            if "domain" not in df.columns or "dnsmagnitude" not in df.columns:
                raise ValueError(f"必要列がありません: {path}")
            tmp = df[["domain", "dnsmagnitude"]].copy()
            tmp["date"] = ymd
            tmp.rename(columns={"domain":"subdomain","dnsmagnitude":"magnitude"}, inplace=True)
            rows.append(tmp[["date","subdomain","magnitude"]])
        except Exception as e:
            print(f"[WARN] 読み込み失敗: {path} -> {e}")
        cur += timedelta(days=1)

    if missing > 0:
        print(f"[INFO] magnitudeファイル欠落: {missing}日分（where={where})")
    if not rows:
        return pd.DataFrame(columns=["date","subdomain","magnitude"])
    out = pd.concat(rows, ignore_index=True)
    out["magnitude"] = pd.to_numeric(out["magnitude"], errors="coerce")
    out = out.dropna(subset=["subdomain","magnitude"])
    return out


# ======== 集計 ========

def summarize_monthly_counts(df_counts: pd.DataFrame) -> pd.DataFrame:
    """
    日次 count をサブドメイン単位で平均・分散に要約
    出力: ['subdomain','q_mean','q_var','days']
    """
    if df_counts.empty:
        return pd.DataFrame(columns=["subdomain","q_mean","q_var","days"])
    g = df_counts.groupby("subdomain")["count"]
    out = pd.DataFrame({
        "q_mean": g.mean(),
        "q_var": g.var(ddof=1),   # 不偏分散
        "days": g.count()
    }).reset_index()
    return out


def summarize_monthly_magnitude(df_mag: pd.DataFrame) -> pd.DataFrame:
    """
    日次 magnitude をサブドメイン単位で平均・分散に要約
    出力: ['subdomain','mag_mean','mag_var','days']
    """
    if df_mag.empty:
        return pd.DataFrame(columns=["subdomain","mag_mean","mag_var","days"])
    g = df_mag.groupby("subdomain")["magnitude"]
    out = pd.DataFrame({
        "mag_mean": g.mean(),
        "mag_var": g.var(ddof=1),
        "days": g.count()
    }).reset_index()
    return out


# ======== 可視化 ========

def save_corr_and_scatter(df_merge: pd.DataFrame, where: int, period_str: str, out_dir: str,
                          xlim: tuple = None, ylim: tuple = (0, 10)):
    """
    x=q_mean, y=mag_mean の散布図と Pearson/Spearman 相関を保存
    """
    if df_merge.empty:
        print(f"[INFO] 相関/散布図: データ無し（where={where}）")
        return

    pearson  = df_merge["q_mean"].corr(df_merge["mag_mean"], method="pearson")
    spearman = df_merge["q_mean"].corr(df_merge["mag_mean"], method="spearman")

    with open(os.path.join(out_dir, f"corr_{where}.txt"), "w", encoding="utf-8") as f:
        f.write(f"期間: {period_str}\n")
        f.write(f"where={where}（0=権威,1=リゾルバ）\n")
        f.write(f"データ点: {len(df_merge)}\n")
        f.write(f"Pearson r:  {pearson:.4f}\n")
        f.write(f"Spearman ρ: {spearman:.4f}\n")

    plt.figure(figsize=(7,5))
    plt.scatter(df_merge["q_mean"].values, df_merge["mag_mean"].values, s=12)
    plt.xlabel("Query mean (平均クエリ数/日)")
    plt.ylabel("DNS Magnitude mean (人気度指標)")
    plt.title(f"where={where}  Query mean vs Magnitude mean ({period_str})")
    plt.grid(True, linestyle="--", alpha=0.4)

    # 軸を固定
    if xlim:
        plt.xlim(xlim)
    if ylim:
        plt.ylim(ylim)

    out_path = os.path.join(out_dir, f"scatter_queries_vs_magnitude_{where}.png")
    plt.tight_layout()
    plt.savefig(out_path, dpi=160)
    plt.close()




def save_boxplot_mag(mag0: pd.DataFrame, mag1: pd.DataFrame, period_str: str, out_dir: str):
    """
    権威 vs リゾルバの Magnitude平均の箱ひげ図
    """
    if mag0.empty or mag1.empty:
        print("[INFO] 箱ひげ図: 片方が空データのためスキップ")
        return
    plt.figure(figsize=(6,5))
    data = [mag0["mag_mean"].dropna().values, mag1["mag_mean"].dropna().values]
    plt.boxplot(data, labels=["Authoritative(0)", "Resolver(1)"], showfliers=False)
    plt.ylabel("DNS Magnitude mean")
    plt.title(f"Distribution of Magnitude mean ({period_str})")
    plt.grid(True, axis="y", linestyle="--", alpha=0.4)
    out_box = os.path.join(out_dir, f"boxplot_magnitude_mean_auth_vs_resolver.png")
    plt.tight_layout()
    plt.savefig(out_box, dpi=160)
    plt.close()


def save_heatmap_mag(mag0: pd.DataFrame, mag1: pd.DataFrame, topn: int, period_str: str, out_dir: str):
    """
    共通サブドメインについて、mag_mean の平均が高い上位Nを 2行×N列のヒートマップで表示
    """
    both = pd.merge(
        mag0[["subdomain","mag_mean"]].rename(columns={"mag_mean":"mag_mean_0"}),
        mag1[["subdomain","mag_mean"]].rename(columns={"mag_mean":"mag_mean_1"}),
        on="subdomain", how="inner"
    )
    if both.empty:
        print("[INFO] ヒートマップ: 共通サブドメインがないためスキップ")
        return
    both["mag_mean_avg"] = (both["mag_mean_0"] + both["mag_mean_1"]) / 2.0
    top = both.sort_values("mag_mean_avg", ascending=False).head(topn)

    heat = np.vstack([top["mag_mean_0"].values, top["mag_mean_1"].values])

    plt.figure(figsize=(max(10, topn*0.35), 3.8))
    im = plt.imshow(heat, aspect="auto")
    plt.colorbar(im, fraction=0.046, pad=0.04)
    plt.yticks([0,1], ["Authoritative(0)","Resolver(1)"])
    plt.xticks(range(len(top)), top["subdomain"].tolist(), rotation=90)
    plt.title(f"DNS Magnitude mean Heatmap (Top {topn}, {period_str})")
    plt.tight_layout()
    out_heat = os.path.join(out_dir, f"heatmap_magnitude_top{topn}.png")
    plt.savefig(out_heat, dpi=160)
    plt.close()


# ======== メイン ========

def main():
    parser = argparse.ArgumentParser(description="日次CSVから月次統計を作成して相関/散布図/箱ひげ/ヒートマップを出力")
    parser.add_argument("--count-dir", required=True, help="日次クエリ数CSVのディレクトリ（例: /home/shimada/analysis/output-2025）")
    parser.add_argument("--mag-dir",   required=True, help="日次Magnitude CSVのディレクトリ（例: /home/shimada/analysis/output）")
    parser.add_argument("--start-date", default="2025-04-01", help="開始日 YYYY-MM-DD（デフォルト: 2025-04-01）")
    parser.add_argument("--end-date",   default="2025-04-30", help="終了日 YYYY-MM-DD（デフォルト: 2025-04-30）")
    parser.add_argument("--out-dir",    default="./figures",  help="図・相関テキストの出力先（デフォルト: ./figures）")
    parser.add_argument("--topn",       type=int, default=30, help="ヒートマップの上位件数（デフォルト: 30）")
    args = parser.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    period_str = f"{args.start_date}_to_{args.end_date}"

    # 0=権威, 1=リゾルバ をそれぞれ集計
    results = {}
    for where in (0, 1):
        counts = read_daily_counts_for_range(args.count_dir, where, args.start_date, args.end_date)
        mags   = read_daily_magnitude_for_range(args.mag_dir, where, args.start_date, args.end_date)

        qsum = summarize_monthly_counts(counts)
        msum = summarize_monthly_magnitude(mags)

        # 結合（相関・散布用）
        merged = pd.merge(msum[["subdomain","mag_mean","mag_var"]],
                          qsum[["subdomain","q_mean","q_var"]],
                          on="subdomain", how="inner")
        results[where] = dict(qsum=qsum, msum=msum, merged=merged)

    # 軸範囲を両方のデータから決定
    all_q = pd.concat([results[0]["merged"]["q_mean"], results[1]["merged"]["q_mean"]])
    xmin, xmax = all_q.min(), all_q.max()
    xlim = (0, xmax * 1.05)  # 5%余裕を持たせる
    ylim = (0, 10)           # Magnitudeは0〜10に固定

    # 相関・散布図（共通スケールで出力）
    for where in (0, 1):
        save_corr_and_scatter(results[where]["merged"], where=where,
                              period_str=period_str, out_dir=args.out_dir,
                              xlim=xlim, ylim=ylim)

    # 箱ひげ図（Magnitude平均の分布: 権威 vs リゾルバ）
    save_boxplot_mag(results[0]["msum"], results[1]["msum"], period_str=period_str, out_dir=args.out_dir)

    # ヒートマップ（共通サブドメインの上位N）
    save_heatmap_mag(results[0]["msum"], results[1]["msum"], args.topn, period_str=period_str, out_dir=args.out_dir)

    print("完了：図と相関テキストを保存しました ->", os.path.abspath(args.out_dir))

if __name__ == "__main__":
    main()
