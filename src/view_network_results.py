#!/usr/bin/env python3
"""
ネットワーク分析結果表示ツール

ネットワーク分析の結果を表示し、統計情報を提供します。

使用方法:
    python3 view_network_results.py YYYY-MM-DD [query|response] [top_n]

引数:
    YYYY-MM-DD: 対象日付
    analysis_type: 分析タイプ（query または response）デフォルト: query
    top_n: 表示する上位件数 デフォルト: 10

例:
    python3 view_network_results.py 2025-04-01 query 20
    python3 view_network_results.py 2025-04-01 response
"""

import sys
import os
import csv
import glob
from func import ensure_output_dir

def load_network_results(date_str, analysis_type, network_type):
    """指定された条件の結果CSVを読み込み"""
    output_dir = ensure_output_dir("network_analysis")
    pattern = f"magnitude-{network_type}-{analysis_type}-{date_str}.csv"
    file_path = os.path.join(output_dir, pattern)
    
    if not os.path.exists(file_path):
        return []
    
    results = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                results.append({
                    'subdomain': row['subdomain'],
                    'magnitude': float(row['magnitude'])
                })
    except Exception as e:
        print(f"ファイル読み込みエラー ({file_path}): {str(e)}")
        return []
    
    return results

def display_network_summary(date_str, analysis_type, top_n=10):
    """ネットワーク分析結果のサマリーを表示"""
    print(f"=== ネットワーク分析結果: {date_str} ({analysis_type}) ===\n")
    
    network_types = ['internal', 'external', 'other']
    network_names = {
        'internal': '学内者 (133.51.112.0/20)',
        'external': '学外者 (133.51.192.0/21)',
        'other': 'その他'
    }
    
    all_results = {}
    total_domains = 0
    
    for network_type in network_types:
        results = load_network_results(date_str, analysis_type, network_type)
        all_results[network_type] = results
        total_domains += len(results)
        
        print(f"--- {network_names[network_type]} ---")
        if not results:
            print("データが見つかりませんでした\n")
            continue
        
        print(f"ドメイン数: {len(results)}")
        
        # 統計情報
        magnitudes = [r['magnitude'] for r in results]
        if magnitudes:
            print(f"平均Magnitude: {sum(magnitudes)/len(magnitudes):.6f}")
            print(f"最大Magnitude: {max(magnitudes):.6f}")
            print(f"最小Magnitude: {min(magnitudes):.6f}")
        
        # 上位表示
        print(f"\n上位 {min(top_n, len(results))} ドメイン:")
        for i, result in enumerate(results[:top_n], 1):
            print(f"  {i:2d}. {result['subdomain']:<30} {result['magnitude']:8.6f}")
        print()
    
    # 全体サマリー
    print(f"=== 全体サマリー ===")
    print(f"総ドメイン数: {total_domains}")
    for network_type in network_types:
        count = len(all_results[network_type])
        percentage = (count / total_domains * 100) if total_domains > 0 else 0
        print(f"{network_names[network_type]}: {count}ドメイン ({percentage:.1f}%)")

def compare_network_types(date_str, analysis_type, top_n=10):
    """ネットワークタイプ間での比較分析"""
    print(f"\n=== ネットワーク比較分析: {date_str} ({analysis_type}) ===\n")
    
    # 全ネットワークの結果を統合
    all_domains = {}
    
    for network_type in ['internal', 'external', 'other']:
        results = load_network_results(date_str, analysis_type, network_type)
        for result in results:
            domain = result['subdomain']
            if domain not in all_domains:
                all_domains[domain] = {}
            all_domains[domain][network_type] = result['magnitude']
    
    # 複数ネットワークで出現するドメインを抽出
    multi_network_domains = []
    for domain, network_data in all_domains.items():
        if len(network_data) > 1:
            multi_network_domains.append((domain, network_data))
    
    if not multi_network_domains:
        print("複数ネットワークで出現するドメインが見つかりませんでした")
        return
    
    print(f"複数ネットワークで出現するドメイン数: {len(multi_network_domains)}")
    print(f"\n上位 {min(top_n, len(multi_network_domains))} ドメイン:")
    print(f"{'ドメイン':<30} {'学内':<10} {'学外':<10} {'その他':<10}")
    print("-" * 65)
    
    # Magnitudeの合計でソート
    sorted_domains = sorted(multi_network_domains, 
                          key=lambda x: sum(x[1].values()), reverse=True)
    
    for domain, network_data in sorted_domains[:top_n]:
        internal = f"{network_data.get('internal', 0):.4f}" if 'internal' in network_data else "-"
        external = f"{network_data.get('external', 0):.4f}" if 'external' in network_data else "-"
        other = f"{network_data.get('other', 0):.4f}" if 'other' in network_data else "-"
        print(f"{domain:<30} {internal:<10} {external:<10} {other:<10}")

def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    try:
        date_str = sys.argv[1]
        analysis_type = sys.argv[2] if len(sys.argv) > 2 else "query"
        top_n = int(sys.argv[3]) if len(sys.argv) > 3 else 10
        
        if analysis_type not in ["query", "response"]:
            print("分析タイプは 'query' または 'response' を指定してください")
            sys.exit(1)
        
        # 結果表示
        display_network_summary(date_str, analysis_type, top_n)
        compare_network_types(date_str, analysis_type, top_n)
        
    except KeyboardInterrupt:
        print("\n処理が中断されました")
        sys.exit(1)
    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
