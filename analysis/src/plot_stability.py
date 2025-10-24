#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日次の DNS Magnitude CSV（例: /home/shimada/analysis/output/0-2025-04-01.csv など）
を直接読み込み、権威DNS(0)・リゾルバ(1)それぞれについて
- サブドメイン別の安定性統計（mean, std, var, median, q25, q75, min, max, count, CV）
- 可視化（TopNバー, 権威vsリゾルバの散布図, 権威・リゾルバの箱ひげ, 日次ヒートマップ）
を出力します。

前提のCSV構造（自動判定あり）:
  - カラム名: "domain" または "subdomain"
  - カラム名: "dnsmagnitude" または "magnitude"
  - カラム名: "day"（例: "01"）※無くてもOK（ファイル名から日付抽出）

入力ファイル名の想定:
  - {where}-{YYYY}-{MM}-{DD}.csv  （where は 0=権威, 1=リゾルバ）
    例: 0-2025-04-01.csv, 1-2025-04-14.csv

使い方例:
  1) 2025年4月の権威・リゾルバをまとめて解析
     python3 plot_stability.py \
       --auth-glob "/home/shimada/analysis/output/0-2025-04-*.csv" \
       --resolver-glob "/home/shimada/analysis/output/1-2025-04-*.csv" \
       --outdir ./figs --topn 20 --min-days 5

  2) リゾルバだけを解析
     python3 plot_stability.py \
       --resolver-glob "/home/shimada/analysis/output/1-2025-04-*.csv" \
       --outdir ./figs_resolver

出力（--outdir 配下）:
  - stats_where0.csv / stats_where1.csv（集計テーブル）
  - diff_mean_auth_vs_resolver.csv（共通サブドメインの平均差）
  - bar_topN_mean_where{0,1}.png       （平均のTopN）
  - bar_topN_std_where{0,1}.png        （標準偏差のTopN＝変動大）
  - boxplot_std_auth_vs_resolver.png   （std分布の箱ひげ：権威vsリゾルバ）
  - scatter_mean_auth_vs_resolver.png  （共通サブドメインの平均 Magnitude 散布図）
  - heatmap_daily_topN_where{0,1}.png  （日次ヒートマップ：平均上位Nドメイン）
"""

import argparse
import glob
import os
import re
import math
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# ==== ユーティリティ ====

FILENAME_RE = re.compile(r'(?P<where>[01])-(?P<y>\d{4})-(?P<m>\d{2})-(?P<d>\d{2})\.csv$')

def parse_date_from_filename(path):
    """ファイル名から (where, YYYY-MM-DD) を抽出。失敗時は (None, None)"""
    m = FILENAME_RE.search(os.path.basename(path))
    if not m:
        return None, None
    where = int(m.group('where'))
    date_str = f"{m.group('y')}-{m.group('m')}-{m.group('d')}"
    return where, date_str

def load_daily_glob(glob_pattern, expected_where):
    """
    グロブに一致する日次CSVをすべて読み込み、縦結合して返す。
    返すDataFrame: columns = [date, subdomain, magnitude, where]
    """
    if not glob_pattern:
        return pd.DataFrame(columns=['date','subdomain','magnitude','where'])

    frames = []
    for path in sorted(glob.glob(glob_pattern)):
        where_in_name, date_str = parse_date_from_filename(path)
        # where の整合性チェック（指定と一致しないファイルは無視）
        if expected_where is not None and where_in_name is not None and where_in_name != expected_where:
            continue

        try:
            df = pd.read_csv(path, dtype=str)
        except Exception as e:
            print(f"[WARN] 読み込み失敗: {path}: {e}")
            continue

        # カラム名の揺れを吸収
        # サブドメイン名
        if 'subdomain' in df.columns:
            sd_col = 'subdomain'
        elif 'domain' in df.columns:
            sd_col = 'domain'
        else:
            print(f"[WARN] サブドメイン列が見つかりません: {path}")
            continue

        # Magnitude
        if 'dnsmagnitude' in df.columns:
            mag_col = 'dnsmagnitude'
        elif 'magnitude' in df.columns:
            mag_col = 'magnitude'
        else:
            print(f"[WARN] Magnitude列が見つかりません: {path}")
            continue

        # 値クレンジング
        tmp = pd.DataFrame({
            'subdomain': df[sd_col].astype(str).str.strip(),
            'magnitude': pd.to_numeric(df[mag_col], errors='coerce')
        })
        tmp = tmp.dropna(subset=['subdomain','magnitude'])
        tmp['subdomain'] = tmp['subdomain'].str.lower()

        # 日付
        if date_str is None:
            # ファイル名から取れない場合は 'day' カラムを見る（YYYY-MM は不明なので欠損扱い）
            if 'day' in df.columns:
                tmp['date'] = df['day'].astype(str).str.zfill(2)
            else:
                tmp['date'] = np.nan
        else:
            tmp['date'] = date_str

        tmp['where'] = expected_where if expected_where is not None else where_in_name
        frames.append(tmp)

    if not frames:
        return pd.DataFrame(columns=['date','subdomain','magnitude','where'])

    out = pd.concat(frames, ignore_index=True)
    # date が欠損している行は落とす
    out = out.dropna(subset=['date'])
    return out[['date','subdomain','magnitude','where']]

def agg_stats(df, min_days=1):
    """
    df: columns=[date, subdomain, magnitude, where]
    -> where, subdomain ごとに統計量を集計（applyは使わず、安定なaggで計算）
    """
    if df.empty:
        return pd.DataFrame(columns=[
            'where','subdomain','count','mean','std','var','median','q25','q75','min','max','cv'
        ])

    # 数値化を念のためもう一度
    df = df.copy()
    df['magnitude'] = pd.to_numeric(df['magnitude'], errors='coerce')
    df = df.dropna(subset=['magnitude'])

    # named aggregation（将来も安定）
    stats = df.groupby(['where', 'subdomain'], as_index=False).agg(
        count=('magnitude', 'count'),
        mean =('magnitude', 'mean'),
        std  =('magnitude', 'std'),   # ddof=1
        var  =('magnitude', 'var'),   # ddof=1
        median=('magnitude', 'median'),
        q25  =('magnitude', lambda s: s.quantile(0.25)),
        q75  =('magnitude', lambda s: s.quantile(0.75)),
        minv =('magnitude', 'min'),
        maxv =('magnitude', 'max'),
    )

    # 列名を既存の下流処理に合わせて微修正
    stats = stats.rename(columns={'minv': 'min', 'maxv': 'max'})

    # 最低観測日数でフィルタ
    stats = stats[stats['count'] >= min_days].copy()

    # 変動係数 CV = std/mean
    stats['cv'] = stats.apply(
        lambda r: (r['std'] / r['mean']) if (pd.notna(r['std']) and pd.notna(r['mean']) and r['mean'] != 0) else np.nan,
        axis=1
    )

    return stats


def ensure_outdir(d):
    os.makedirs(d, exist_ok=True)

# ==== プロット ====

def plot_bar_topN(stats, where, metric, topn, outdir, title_prefix):
    """
    metric: 'mean' or 'std'
    """
    sub = stats[stats['where']==where].copy()
    if sub.empty:
        return None
    sub = sub.sort_values(metric, ascending=False).head(topn)
    sub = sub.iloc[::-1]  # 横棒を下から上へ

    plt.figure(figsize=(8, max(4, 0.35*len(sub))))
    plt.barh(sub['subdomain'], sub[metric])
    plt.xlabel(metric)
    plt.ylabel('subdomain')
    plt.title(f"{title_prefix} where={where} Top{topn} by {metric}")
    fname = os.path.join(outdir, f"bar_top{topn}_{metric}_where{where}.png")
    plt.tight_layout()
    plt.savefig(fname, dpi=150)
    plt.close()
    return fname

def plot_box_std(stats0, stats1, outdir):
    """
    権威(0)とリゾルバ(1)の std 分布を箱ひげで比較
    """
    if stats0.empty or stats1.empty:
        return None
    data = [stats0['std'].dropna().values, stats1['std'].dropna().values]
    labels = ['authoritative(0)', 'resolver(1)']

    plt.figure(figsize=(6,5))
    plt.boxplot(data, labels=labels, showfliers=True)
    plt.ylabel('std of magnitude')
    plt.title('Std distribution: authoritative vs resolver')
    fname = os.path.join(outdir, "boxplot_std_auth_vs_resolver.png")
    plt.tight_layout()
    plt.savefig(fname, dpi=150)
    plt.close()
    return fname

def plot_scatter_mean(stats, outdir, annotate_topk=0):
    """
    共通サブドメインの mean(権威) vs mean(リゾルバ) 散布図
    """
    s0 = stats[stats['where']==0][['subdomain','mean']].rename(columns={'mean':'mean_auth'})
    s1 = stats[stats['where']==1][['subdomain','mean']].rename(columns={'mean':'mean_resolv'})
    merged = pd.merge(s0, s1, on='subdomain', how='inner')
    if merged.empty:
        return None

    x = merged['mean_auth'].values
    y = merged['mean_resolv'].values

    plt.figure(figsize=(6,6))
    plt.scatter(x, y, s=20)
    # y=x の基準線
    lim_min = min(np.nanmin(x), np.nanmin(y))
    lim_max = max(np.nanmax(x), np.nanmax(y))
    plt.plot([lim_min, lim_max], [lim_min, lim_max], linestyle='--')
    plt.xlabel('Mean Magnitude (Authoritative)')
    plt.ylabel('Mean Magnitude (Resolver)')
    plt.title('Mean Magnitude: Authoritative vs Resolver')

    # 差の大きい上位を注釈（任意）
    if annotate_topk and annotate_topk > 0:
        merged['diff'] = (merged['mean_resolv'] - merged['mean_auth']).abs()
        lab = merged.sort_values('diff', ascending=False).head(annotate_topk)
        for _, r in lab.iterrows():
            plt.annotate(r['subdomain'], (r['mean_auth'], r['mean_resolv']), xytext=(3,3), textcoords='offset points', fontsize=8)

    fname = os.path.join(outdir, "scatter_mean_auth_vs_resolver.png")
    plt.tight_layout()
    plt.savefig(fname, dpi=150)
    plt.close()
    return fname

def plot_heatmap_daily(df, where, topn, outdir, title_prefix):
    """
    whereごとに、平均値TopNのサブドメインについて、日次(列)×サブドメイン(行)のヒートマップ
    """
    dsub = df[df['where']==where].copy()
    if dsub.empty:
        return None

    # TopN by mean を選定
    means = dsub.groupby('subdomain', as_index=False)['magnitude'].mean()
    top = means.sort_values('magnitude', ascending=False).head(topn)['subdomain'].tolist()

    dsub = dsub[dsub['subdomain'].isin(top)].copy()
    # ピボット：行=subdomain, 列=date, 値=magnitude
    mat = dsub.pivot_table(index='subdomain', columns='date', values='magnitude', aggfunc='mean')

    if mat.empty:
        return None

    # 列（日付）を時系列順に
    try:
        mat = mat.reindex(sorted(mat.columns, key=lambda s: pd.to_datetime(s)), axis=1)
    except Exception:
        mat = mat.reindex(sorted(mat.columns), axis=1)

    plt.figure(figsize=(max(6, 0.45*mat.shape[1]), max(4, 0.35*mat.shape[0])))
    plt.imshow(mat.values, aspect='auto', interpolation='nearest')
    plt.colorbar(label='magnitude')
    plt.yticks(ticks=np.arange(mat.shape[0]), labels=mat.index)
    # 横軸は間引いて表示
    xticks = np.arange(mat.shape[1])
    step = max(1, mat.shape[1] // 12)
    plt.xticks(ticks=xticks[::step], labels=mat.columns[::step], rotation=45, ha='right')
    plt.xlabel('date')
    plt.ylabel('subdomain')
    plt.title(f"{title_prefix} where={where} Top{topn} (daily magnitude)")
    fname = os.path.join(outdir, f"heatmap_daily_top{topn}_where{where}.png")
    plt.tight_layout()
    plt.savefig(fname, dpi=150)
    plt.close()
    return fname

# ==== メイン ====

def main():
    ap = argparse.ArgumentParser(description="日次のDNS Magnitude CSV（生ファイル）から安定性と比較図を作る")
    ap.add_argument('--auth-glob', default="", help='権威DNS(0)のCSVグロブ（例: "/home/shimada/analysis/output/0-2025-04-*.csv"）')
    ap.add_argument('--resolver-glob', default="", help='リゾルバ(1)のCSVグロブ（例: "/home/shimada/analysis/output/1-2025-04-*.csv"）')
    ap.add_argument('--outdir', required=True, help='図と集計CSVの出力先ディレクトリ')
    ap.add_argument('--topn', type=int, default=20, help='TopN（棒グラフ・ヒートマップ）')
    ap.add_argument('--min-days', type=int, default=5, help='統計計算に使う最小日数（countの閾値）')
    ap.add_argument('--annotate-topk', type=int, default=0, help='散布図で注釈する「差の大きい上位K」件（0で注釈なし）')
    args = ap.parse_args()

    ensure_outdir(args.outdir)

    # 読み込み
    df0 = load_daily_glob(args.auth_glob, expected_where=0) if args.auth_glob else pd.DataFrame(columns=['date','subdomain','magnitude','where'])
    df1 = load_daily_glob(args.resolver_glob, expected_where=1) if args.resolver_glob else pd.DataFrame(columns=['date','subdomain','magnitude','where'])

    df = pd.concat([df0, df1], ignore_index=True)
    if df.empty:
        print("[ERROR] 入力が空です。--auth-glob または --resolver-glob を指定してください。")
        return 1

    # 統計計算
    stats = agg_stats(df, min_days=args.min_days)
    if stats.empty:
        print("[ERROR] 統計テーブルが空です（min-days が大きすぎる/データが無い可能性）。")
        return 1

    # 出力: 集計CSV
    stats0 = stats[stats['where']==0].copy()
    stats1 = stats[stats['where']==1].copy()
    if not stats0.empty:
        stats0.to_csv(os.path.join(args.outdir, "stats_where0.csv"), index=False)
    if not stats1.empty:
        stats1.to_csv(os.path.join(args.outdir, "stats_where1.csv"), index=False)

    # 図: 権威/リゾルバ それぞれ TopN（mean / std）
    if not stats0.empty:
        plot_bar_topN(stats, where=0, metric='mean', topn=args.topn, outdir=args.outdir, title_prefix='Top by mean')
        plot_bar_topN(stats, where=0, metric='std',  topn=args.topn, outdir=args.outdir, title_prefix='Top by std (variability)')
    if not stats1.empty:
        plot_bar_topN(stats, where=1, metric='mean', topn=args.topn, outdir=args.outdir, title_prefix='Top by mean')
        plot_bar_topN(stats, where=1, metric='std',  topn=args.topn, outdir=args.outdir, title_prefix='Top by std (variability)')

    # 図: 箱ひげ（std分布の比較）
    if not stats0.empty and not stats1.empty:
        plot_box_std(stats0, stats1, args.outdir)

    # 図: 散布図（共通サブドメインの mean 比較）
    if not stats0.empty and not stats1.empty:
        plot_scatter_mean(stats, args.outdir, annotate_topk=args.annotate_topk)

        # 差分表も出力
        m0 = stats0[['subdomain','mean']].rename(columns={'mean':'mean_auth'})
        m1 = stats1[['subdomain','mean']].rename(columns={'mean':'mean_resolv'})
        diff = pd.merge(m0, m1, on='subdomain', how='inner')
        if not diff.empty:
            diff['delta_resolv_minus_auth'] = diff['mean_resolv'] - diff['mean_auth']
            diff['abs_delta'] = diff['delta_resolv_minus_auth'].abs()
            diff['ratio_resolv_div_auth'] = diff['mean_resolv'] / diff['mean_auth']
            diff = diff.sort_values('abs_delta', ascending=False)
            diff.to_csv(os.path.join(args.outdir, "diff_mean_auth_vs_resolver.csv"), index=False)

    # 図: 日次ヒートマップ（平均上位Nドメイン）
    if not df0.empty:
        plot_heatmap_daily(df0, where=0, topn=args.topn, outdir=args.outdir, title_prefix='Daily heatmap (authoritative)')
    if not df1.empty:
        plot_heatmap_daily(df1, where=1, topn=args.topn, outdir=args.outdir, title_prefix='Daily heatmap (resolver)')

    print(f"[DONE] 出力先: {args.outdir}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
