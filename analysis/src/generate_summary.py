#!/usr/bin/env python3
"""
日次統計サマリー生成スクリプト
JSONとMarkdown形式で保存
"""

import sys
import os
import json
import pandas as pd
from datetime import datetime

OUTPUT_DIR = "/home/shimada/analysis/output"
SUMMARY_DIR = "/home/shimada/analysis/summaries"

def generate_summary(date_str):
    """指定日の統計サマリーを生成"""
    os.makedirs(SUMMARY_DIR, exist_ok=True)
    
    summary = {
        'date': date_str,
        'generated_at': datetime.now().isoformat(),
        'authoritative': {},
        'resolver': {}
    }
    
    for where, label in [(0, 'authoritative'), (1, 'resolver')]:
        filepath = os.path.join(OUTPUT_DIR, f"{where}-{date_str}.csv")
        
        if not os.path.exists(filepath):
            print(f"[WARN] {filepath} が見つかりません")
            continue
        
        try:
            df = pd.read_csv(filepath)
            
            # カラム名正規化
            if 'domain' in df.columns:
                df = df.rename(columns={'domain': 'subdomain'})
            if 'dnsmagnitude' in df.columns:
                df = df.rename(columns={'dnsmagnitude': 'magnitude'})
            
            # 統計計算
            stats = {
                'total_domains': len(df),
                'mean_magnitude': float(df['magnitude'].mean()),
                'median_magnitude': float(df['magnitude'].median()),
                'max_magnitude': float(df['magnitude'].max()),
                'min_magnitude': float(df['magnitude'].min()),
                'std_magnitude': float(df['magnitude'].std()),
                'top_10_domains': df.nlargest(10, 'magnitude')[['subdomain', 'magnitude']].to_dict('records')
            }
            
            summary[label] = stats
            print(f"✓ {label}: {stats['total_domains']} domains")
            
        except Exception as e:
            print(f"[ERROR] {label} 処理エラー: {e}")
    
    # JSON保存
    json_path = os.path.join(SUMMARY_DIR, f"summary-{date_str}.json")
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print(f"✓ JSON保存: {json_path}")
    
    # Markdown保存
    md_path = os.path.join(SUMMARY_DIR, f"summary-{date_str}.md")
    with open(md_path, 'w', encoding='utf-8') as f:
        f.write(f"# DNS Magnitude 統計レポート\n\n")
        f.write(f"**日付**: {date_str}  \n")
        f.write(f"**生成時刻**: {summary['generated_at']}  \n\n")
        
        for label in ['authoritative', 'resolver']:
            if not summary[label]:
                continue
            
            s = summary[label]
            server_type = "権威サーバー" if label == "authoritative" else "リゾルバ"
            
            f.write(f"## {server_type}\n\n")
            f.write(f"- **総ドメイン数**: {s['total_domains']}\n")
            f.write(f"- **平均Magnitude**: {s['mean_magnitude']:.4f}\n")
            f.write(f"- **中央値**: {s['median_magnitude']:.4f}\n")
            f.write(f"- **最大値**: {s['max_magnitude']:.4f}\n")
            f.write(f"- **標準偏差**: {s['std_magnitude']:.4f}\n\n")
            
            f.write(f"### Top 10 ドメイン\n\n")
            f.write("| 順位 | サブドメイン | Magnitude |\n")
            f.write("|------|--------------|----------|\n")
            for i, domain in enumerate(s['top_10_domains'], 1):
                f.write(f"| {i} | {domain['subdomain']} | {domain['magnitude']:.6f} |\n")
            f.write("\n")
    
    print(f"✓ Markdown保存: {md_path}")
    return summary

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python3 generate_summary.py YYYY-MM-DD")
        sys.exit(1)
    
    date_str = sys.argv[1]
    summary = generate_summary(date_str)
    
    print(f"\n=== サマリー生成完了: {date_str} ===")