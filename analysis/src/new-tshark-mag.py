#!/usr/bin/env python3
"""
DNS Magnitude 計算ツール（tshark 形式）

- A_tot はその日の全行から unique(ip.dst)
- サブドメイン抽出は厳密（lower、末尾ドット除去、suffix 1回、末尾一致）
- 出力は day,domain,dnsmagnitude（降順）
- 日次 CSV があれば日次を優先、無ければ 00–23 を探索

使い方:
  python3 new-tshark-mag.py -y 2025 -m 04 -d 01 -w 0
"""

import os
import re
import io
import csv
import math
import argparse
import pandas as pd
from collections import defaultdict

# --- パス解決 ---

def daily_path(year, month, day, where):
    base = "/mnt/qnap2/shimada/input" if where == 0 else "/mnt/qnap2/shimada/resolver"
    return os.path.join(base, f"{year}-{month}-{day}.csv")

def hourly_path(year, month, day, hour, where):
    base = "/mnt/qnap2/shimada/input" if where == 0 else "/mnt/qnap2/shimada/resolver"
    return os.path.join(base, f"{year}-{month}-{day}-{hour}.csv")

# --- ロバストリーダ ---

def read_csv_robust(path):
    """まず素直に pd.read_csv、失敗したら1行ずつ救済。返り値: (df, error_lines)"""
    errors = []
    try:
        return pd.read_csv(path), errors
    except Exception as e:
        print(f"通常読み込み失敗: {os.path.basename(path)}: {e}")
        try:
            with open(path, 'r', encoding='utf-8') as f:
                r = csv.reader(f)
                header = next(r)
                ok = [header]
                for i, row in enumerate(r, start=2):  # 1-based + ヘッダ
                    try:
                        if len(row) == len(header):
                            ok.append(row)
                        else:
                            errors.append((i, ','.join(row)))
                    except Exception as rexc:
                        errors.append((i, ','.join(row) if isinstance(row, list) else str(row)))
                if len(ok) > 1:
                    s = io.StringIO()
                    w = csv.writer(s)
                    w.writerows(ok)
                    s.seek(0)
                    return pd.read_csv(s), errors
                else:
                    return pd.DataFrame(columns=header), errors
        except Exception as fe:
            print(f"救済読み込みも失敗: {path}: {fe}")
            return pd.DataFrame(), errors

# --- サブドメイン抽出 ---

def extract_subdomain(qname: str):
    suffix = '.tsukuba.ac.jp'
    if not isinstance(qname, str):
        return None
    q = qname.lower().rstrip('.')
    if q.count(suffix) != 1:
        return None
    if not q.endswith(suffix):
        return None
    body = q[: -len(suffix)]
    if not body:
        return None
    parts = body.split('.')
    if not parts:
        return None
    top = parts[-1]
    return top or None

# --- Magnitude 計算 ---

def compute_magnitude(domain_ip_dict, A_tot):
    if A_tot <= 0:
        return {}
    out = {}
    lnA = math.log(A_tot)
    for dom, s in domain_ip_dict.items():
        c = len(s)
        if c > 0:
            out[dom] = 10 * math.log(c) / lnA
    return dict(sorted(out.items(), key=lambda x: x[1], reverse=True))

# --- 出力 ---

def write_output(mag_dict, year, month, day, where, out_dir="/home/shimada/analysis/output"):
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"{where}-{year}-{month}-{day}.csv")
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['day', 'domain', 'dnsmagnitude'])
        for dom, mag in mag_dict.items():
            w.writerow([day, dom, f"{mag:.6f}"])
    print(f"結果を保存: {path}")
    return path

# --- メイン ---

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('-y', required=True)
    ap.add_argument('-m', required=True)
    ap.add_argument('-d', required=True)
    ap.add_argument('-w', required=True, type=int, choices=[0, 1])  # 0=権威,1=リゾルバ
    ap.add_argument('-o', default='error_log.txt')
    args = ap.parse_args()

    y = args.y
    m = args.m.zfill(2)
    d = args.d.zfill(2)
    w = args.w

    print("=== DNS Magnitude 計算開始 ===")
    print(f"対象: {y}-{m}-{d}  where={w}")

    # 日次優先
    paths = []
    dpath = daily_path(y, m, d, w)
    if os.path.exists(dpath):
        paths = [dpath]
    else:
        for h in range(24):
            p = hourly_path(y, m, d, f"{h:02d}", w)
            if os.path.exists(p):
                paths.append(p)

    if not paths:
        print("対象ファイルが見つかりません")
        return 1

    errors_total = []
    file_error_counts = {}

    # 全行から A_tot を取るための ip.dst 集合
    all_ipdst = set()
    domain_ip = defaultdict(set)

    for p in sorted(paths):
        print(f"読み込み: {os.path.basename(p)}")
        df, errs = read_csv_robust(p)
        if errs:
            file_error_counts[os.path.basename(p)] = len(errs)
            for ln, content in errs:
                errors_total.append((os.path.basename(p), ln, content))

        if df.empty:
            continue
        if 'ip.dst' not in df.columns or 'dns.qry.name' not in df.columns:
            continue

        # A_tot 用（全行）
        all_ipdst.update(df['ip.dst'].dropna().unique())

        # サブドメイン抽出 → 集計
        df = df.copy()
        df['subdomain'] = df['dns.qry.name'].apply(extract_subdomain)
        v = df[df['subdomain'].notna()].copy()
        if v.empty:
            continue
        g = v.groupby('subdomain')['ip.dst'].apply(lambda s: set(s.dropna())).to_dict()
        for dom, s in g.items():
            domain_ip[dom].update(s)

    A_tot = len(all_ipdst)
    print(f"A_tot (unique ip.dst): {A_tot}")
    if A_tot == 0 or not domain_ip:
        print("有効データがありません")
        return 1

    mag = compute_magnitude(domain_ip, A_tot)

    # 上位10表示
    print("\n=== Top 10 ===")
    for i, (dom, mval) in enumerate(list(mag.items())[:10], 1):
        print(f"{i:2d}. {dom:20} {mval:8.6f} (IPs={len(domain_ip[dom])})")

    write_output(mag, y, m, d, w)

    # エラーログ
    if errors_total:
        with open(args.o, 'w', encoding='utf-8') as f:
            f.write("=== エラー行の詳細 ===\n")
            for fname, ln, content in errors_total:
                f.write(f"ファイル: {fname}, 行番号: {ln}\n")
                f.write(f"行内容: {content}\n")
                f.write("-"*80 + "\n")
            f.write("\n=== ファイルごとのエラー行数 ===\n")
            for k, v in file_error_counts.items():
                f.write(f"{k}: {v}行\n")
            f.write(f"\n総エラー行数: {len(errors_total)}\n")
        print(f"エラーログ: {args.o}")
    else:
        print("エラー行なし")

    print("=== 完了 ===")
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
