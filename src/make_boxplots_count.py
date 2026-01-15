#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monthly boxplots of Query Count (mean and standard deviation) per domain

Input: Daily CSV files in a directory
  - count-0-YYYY-MM-DD.csv  … Authoritative DNS
  - count-1-YYYY-MM-DD.csv  … Resolver DNS
CSV format:
  day,domain,count

Output:
  month_boxplot_count_mean.pdf      … Boxplot of monthly mean (Auth/Reso side by side)
  month_boxplot_count_std.pdf       … Boxplot of monthly std deviation (Auth/Reso side by side)
"""

import argparse
import glob
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

def load_month_df(base_dir: Path, typ: int, year: int, month: int) -> pd.DataFrame:
    """Load daily count CSVs for a given month and concatenate them"""
    pattern = f"count-{typ}-{year:04d}-{month:02d}-*.csv"
    paths = sorted(glob.glob(str(base_dir / pattern)))
    if not paths:
        print(f"[WARN] No files matched: {pattern}")
        return pd.DataFrame(columns=["day", "domain", "count"])

    dfs = []
    for p in paths:
        try:
            df = pd.read_csv(p)
            if "count" in df.columns:
                df["count"] = pd.to_numeric(df["count"], errors="coerce")
            else:
                raise ValueError(f"count column missing in {p}")
            if "domain" not in df.columns:
                raise ValueError(f"domain column missing in {p}")
            dfs.append(df[["day", "domain", "count"]].dropna())
        except Exception as e:
            print(f"[WARN] Failed to read {p}: {e}")
    if not dfs:
        return pd.DataFrame(columns=["day", "domain", "count"])
    return pd.concat(dfs, ignore_index=True)

def compute_domain_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Compute monthly mean and std dev for each domain"""
    if df.empty:
        return pd.DataFrame(columns=["domain", "mean", "std"])
    g = df.groupby("domain")["count"]
    out = pd.DataFrame({
        "domain": g.mean().index,
        "mean": g.mean().values,
        "std": g.std(ddof=1).values
    })
    return out

def plot_two_boxplots_side_by_side(
    data_left, data_right, labels=("Authoritative", "Resolver"),
    title="", ylabel="", ylimit=None, outfile="plot.png"
):
    """Draw two side-by-side boxplots"""
    fig, ax = plt.subplots(figsize=(8, 5))
    
    # 箱ひげ図の幅を広げて間隔を詰める
    bp = ax.boxplot([data_left, data_right], labels=labels, showfliers=True, widths=0.6)

    # Bold font for title, ylabel, and x-axis labels
    ax.set_title(title, fontweight="bold", fontsize=14)
    ax.set_ylabel(ylabel, fontweight="bold", fontsize=24)
    
    # x軸ラベルのフォントサイズと太字を設定
    for lbl in ax.get_xticklabels():
        lbl.set_fontweight("bold")
        lbl.set_fontsize(24)

    # y軸を対数スケールに設定
    ax.set_yscale("log")
    
    if ylimit is not None:
        ax.set_ylim(ylimit[0], ylimit[1])
    
    # x軸の範囲を狭めて箱ひげ図の間隔を詰める
    ax.set_xlim(0.5, 2.5)

    ax.grid(axis="y", linestyle=":", alpha=0.6)
    fig.tight_layout()
    fig.savefig(outfile, dpi=200)
    plt.close(fig)
    print(f"[INFO] saved: {outfile}")

def main():
    parser = argparse.ArgumentParser(
        description="Generate monthly boxplots of Query Count (mean & std)"
    )
    parser.add_argument("--base-dir", required=True, help="Directory containing count CSVs (e.g., /home/shimada/analysis/output-2025)")
    parser.add_argument("--year", type=int, default=2025, help="Year (YYYY)")
    parser.add_argument("--month", type=int, default=4, help="Month (MM)")
    parser.add_argument("--out-dir", default=".", help="Output directory")
    args = parser.parse_args()

    base_dir = Path(args.base_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load authoritative (0) and resolver (1)
    df_auth = load_month_df(base_dir, 0, args.year, args.month)
    df_reso = load_month_df(base_dir, 1, args.year, args.month)

    stats_auth = compute_domain_stats(df_auth)
    stats_reso = compute_domain_stats(df_reso)

    # ---- Export domain stats to CSV (4 files) ----
    
    # 1. Authoritative - Mean
    stats_auth_mean = stats_auth[["domain", "mean"]].sort_values(by="mean", ascending=False)
    csv_auth_mean = out_dir / f"domain_stats_auth_mean_{args.year}-{args.month:02d}.csv"
    stats_auth_mean.to_csv(csv_auth_mean, index=False)
    print(f"[INFO] saved: {csv_auth_mean}")

    # 2. Authoritative - Std
    stats_auth_std = stats_auth[["domain", "std"]].sort_values(by="std", ascending=False)
    csv_auth_std = out_dir / f"domain_stats_auth_std_{args.year}-{args.month:02d}.csv"
    stats_auth_std.to_csv(csv_auth_std, index=False)
    print(f"[INFO] saved: {csv_auth_std}")

    # 3. Resolver - Mean
    stats_reso_mean = stats_reso[["domain", "mean"]].sort_values(by="mean", ascending=False)
    csv_reso_mean = out_dir / f"domain_stats_reso_mean_{args.year}-{args.month:02d}.csv"
    stats_reso_mean.to_csv(csv_reso_mean, index=False)
    print(f"[INFO] saved: {csv_reso_mean}")

    # 4. Resolver - Std
    stats_reso_std = stats_reso[["domain", "std"]].sort_values(by="std", ascending=False)
    csv_reso_std = out_dir / f"domain_stats_reso_std_{args.year}-{args.month:02d}.csv"
    stats_reso_std.to_csv(csv_reso_std, index=False)
    print(f"[INFO] saved: {csv_reso_std}")

    # ---- Mean boxplot ----
    means_auth = stats_auth["mean"].dropna().values
    means_reso = stats_reso["mean"].dropna().values
    plot_two_boxplots_side_by_side(
        data_left=means_auth,
        data_right=means_reso,
        labels=("Authoritative", "Resolver"),
        # title=f"Domain-wise Monthly Mean of Query Count ({args.year}-{args.month:02d})",
        ylabel="Mean",
        ylimit=None,  # Query countはスケールが大きいので自動
        outfile=str(out_dir / "month_boxplot_count_mean.pdf"),
    )

    # ---- Std dev boxplot ----
    stds_auth = stats_auth["std"].dropna().values
    stds_reso = stats_reso["std"].dropna().values
    plot_two_boxplots_side_by_side(
        data_left=stds_auth,
        data_right=stds_reso,
        labels=("Authoritative", "Resolver"),
        # title=f"Domain-wise Monthly Std Dev of Query Count ({args.year}-{args.month:02d})",
        ylabel="Std Dev",
        ylimit=None,
        outfile=str(out_dir / "month_boxplot_count_std.pdf"),
    )

if __name__ == "__main__":
    main()
