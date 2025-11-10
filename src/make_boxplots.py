#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Monthly boxplots of DNS Magnitude (mean and standard deviation) per domain

Input: Daily CSV files in a directory
  - 0-YYYY-MM-DD.csv  … Authoritative DNS
  - 1-YYYY-MM-DD.csv  … Resolver DNS
CSV format:
  day,domain,dnsmagnitude

Output:
  month_boxplot_mean.png      … Boxplot of monthly mean (y-axis fixed 0-10, Auth/Reso side by side)
  month_boxplot_std.png       … Boxplot of monthly std deviation (Auth/Reso side by side)
"""

import argparse
import glob
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

def load_month_df(base_dir: Path, typ: int, year: int, month: int) -> pd.DataFrame:
    """Load daily CSVs for a given month and concatenate them"""
    pattern = f"{typ}-{year:04d}-{month:02d}-*.csv"
    paths = sorted(glob.glob(str(base_dir / pattern)))
    if not paths:
        print(f"[WARN] No files matched: {pattern}")
        return pd.DataFrame(columns=["day", "domain", "dnsmagnitude"])

    dfs = []
    for p in paths:
        try:
            df = pd.read_csv(p)
            if "dnsmagnitude" in df.columns:
                df["dnsmagnitude"] = pd.to_numeric(df["dnsmagnitude"], errors="coerce")
            else:
                raise ValueError(f"dnsmagnitude column missing in {p}")
            if "domain" not in df.columns:
                raise ValueError(f"domain column missing in {p}")
            dfs.append(df[["day", "domain", "dnsmagnitude"]].dropna())
        except Exception as e:
            print(f"[WARN] Failed to read {p}: {e}")
    if not dfs:
        return pd.DataFrame(columns=["day", "domain", "dnsmagnitude"])
    return pd.concat(dfs, ignore_index=True)

def compute_domain_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Compute monthly mean and std dev for each domain"""
    if df.empty:
        return pd.DataFrame(columns=["domain", "mean", "std"])
    g = df.groupby("domain")["dnsmagnitude"]
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
    ax.boxplot([data_left, data_right], labels=labels, showfliers=True)

    # Bold font for title, ylabel, and x-axis labels
    ax.set_title(title, fontweight="bold", fontsize=14)
    ax.set_ylabel(ylabel, fontweight="bold", fontsize=12)
    for lbl in ax.get_xticklabels():
        lbl.set_fontweight("bold")

    if ylimit is not None:
        ax.set_ylim(ylimit[0], ylimit[1])

    ax.grid(axis="y", linestyle=":", alpha=0.6)
    fig.tight_layout()
    fig.savefig(outfile, dpi=200)
    plt.close(fig)
    print(f"[INFO] saved: {outfile}")

def main():
    parser = argparse.ArgumentParser(
        description="Generate monthly boxplots of DNS Magnitude (mean & std)"
    )
    parser.add_argument("--base-dir", required=True, help="Directory containing CSVs (e.g., /home/shimada/analysis/output)")
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

    # ---- Mean boxplot ----
    means_auth = stats_auth["mean"].dropna().values
    means_reso = stats_reso["mean"].dropna().values
    plot_two_boxplots_side_by_side(
        data_left=means_auth,
        data_right=means_reso,
        labels=("Authoritative", "Resolver"),
        title=f"Domain-wise Monthly Mean of DNS Magnitude ({args.year}-{args.month:02d})",
        ylabel="DNS Magnitude (monthly mean)",
        ylimit=(0, 10),
        outfile=str(out_dir / "month_boxplot_mean.pdf"),
    )

    # ---- Std dev boxplot ----
    stds_auth = stats_auth["std"].dropna().values
    stds_reso = stats_reso["std"].dropna().values
    plot_two_boxplots_side_by_side(
        data_left=stds_auth,
        data_right=stds_reso,
        labels=("Authoritative", "Resolver"),
        title=f"Domain-wise Monthly Std Dev of DNS Magnitude ({args.year}-{args.month:02d})",
        ylabel="DNS Magnitude (monthly std dev)",
        ylimit=None,
        outfile=str(out_dir / "month_boxplot_std.pdf"),
    )

if __name__ == "__main__":
    main()
