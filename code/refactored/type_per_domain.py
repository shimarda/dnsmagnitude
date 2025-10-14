#!/usr/bin/env python3
"""
ドメイン別Type分析ツール (2025年版)

指定された日付のDNSデータからドメイン別のqtype分析を実行します。
共通処理は func.py で管理されています。

使用方法:
    python3 type_per_domain.py YYYY MM DD

引数:
    YYYY: 年（4桁）
    MM: 月（2桁、ゼロパディング）
    DD: 日（2桁、ゼロパディング）

例:
    python3 type_per_domain.py 2025 04 01
"""

import sys
import os
import pandas as pd
from func import (
    file_lst, safe_read_csv, extract_subdomain,
    ensure_output_dir, write_error_log
)

def analyze_domain_qtype_distribution(df, date_str):
    """ドメイン別qtype分布を分析"""
    if df.empty or 'qtype' not in df.columns or 'qname' not in df.columns:
        return {}
    
    # サブドメインを抽出
    df['subdomain'] = df['qname'].apply(extract_subdomain)
    
    # 有効なサブドメインのみをフィルタ
    valid_df = df[df['subdomain'].notna() & df['qtype'].notna()].copy()
    
    if valid_df.empty:
        print("有効なドメイン・qtypeデータが見つかりませんでした")
        return {}
    
    results = {}
    
    # ドメイン別に分析
    for subdomain in valid_df['subdomain'].unique():
        domain_df = valid_df[valid_df['subdomain'] == subdomain].copy()
        
        if domain_df.empty:
            continue
        
        # qtypeごとの件数を集計
        qtype_counts = domain_df['qtype'].value_counts()
        total_count = len(domain_df)
        
        # qtype分布を計算
        qtype_distribution = {}
        for qtype, count in qtype_counts.items():
            qtype_distribution[qtype] = {
                'count': count,
                'ratio': count / total_count
            }
        
        results[subdomain] = {
            'total_queries': total_count,
            'unique_qtypes': len(qtype_counts),
            'qtype_distribution': qtype_distribution,
            'top_qtype': qtype_counts.index[0] if len(qtype_counts) > 0 else None,
            'top_qtype_ratio': qtype_counts.iloc[0] / total_count if len(qtype_counts) > 0 else 0
        }
    
    return results

def write_domain_type_analysis_csv(analysis_results, date_str, output_dir):
    """ドメイン別type分析結果をCSVに書き込み"""
    import csv
    
    # ドメイン統計CSV
    domain_stats_csv_path = os.path.join(output_dir, f"domain-stats-{date_str}.csv")
    with open(domain_stats_csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['date', 'subdomain', 'total_queries', 'unique_qtypes', 'top_qtype', 'top_qtype_ratio'])
        
        for subdomain, stats in sorted(analysis_results.items(), key=lambda x: x[1]['total_queries'], reverse=True):
            writer.writerow([
                date_str, subdomain, stats['total_queries'], 
                stats['unique_qtypes'], stats['top_qtype'],
                f"{stats['top_qtype_ratio']:.6f}"
            ])
    
    # ドメイン別qtype分布CSV
    distribution_csv_path = os.path.join(output_dir, f"domain-qtype-distribution-{date_str}.csv")
    with open(distribution_csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['date', 'subdomain', 'qtype', 'count', 'ratio'])
        
        for subdomain, stats in analysis_results.items():
            for qtype, qtype_stats in stats['qtype_distribution'].items():
                writer.writerow([
                    date_str, subdomain, qtype, 
                    qtype_stats['count'], f"{qtype_stats['ratio']:.6f}"
                ])
    
    print(f"ドメイン統計をCSVに保存: {domain_stats_csv_path}")
    print(f"ドメイン別qtype分布をCSVに保存: {distribution_csv_path}")

def main():
    if len(sys.argv) != 4:
        print(__doc__)
        sys.exit(1)
    
    try:
        year = sys.argv[1]
        month = sys.argv[2].zfill(2)  # ゼロパディング
        day = sys.argv[3].zfill(2)    # ゼロパディング
        
        date_str = f"{year}-{month}-{day}"
        print(f"=== ドメイン別Type分析: {date_str} ===")
        
        # 入力ディレクトリ設定
        input_directory = f"/mnt/qnap3/shimada-dnsmagnitude/logs/{year}/{month}/{day}/"
        
        if not os.path.exists(input_directory):
            print(f"エラー: 入力ディレクトリが見つかりません: {input_directory}")
            sys.exit(1)
        
        # CSVファイルリストを取得
        csv_files = file_lst(input_directory)
        
        if not csv_files:
            print(f"エラー: CSVファイルが見つかりません: {input_directory}")
            sys.exit(1)
        
        print(f"処理対象ファイル数: {len(csv_files)}")
        
        # 出力ディレクトリ設定
        output_dir = ensure_output_dir("type_per_domain")
        
        # データを読み込みと統合
        all_data = []
        total_error_lines = []
        file_error_counts = {}
        
        for file_path in csv_files:
            file_name = os.path.basename(file_path)
            print(f"処理中: {file_name}")
            
            # CSVファイルを安全に読み込み
            df, error_lines = safe_read_csv(file_path)
            
            if not df.empty:
                all_data.append(df)
            
            # エラー行の記録
            if error_lines:
                file_error_counts[file_name] = len(error_lines)
                total_error_lines.extend([(file_name, line_num, content) for line_num, content in error_lines])
        
        if not all_data:
            print("エラー: 有効なデータが見つかりませんでした")
            sys.exit(1)
        
        print(f"有効ファイル数: {len(all_data)}")
        
        # データを統合
        combined_df = pd.concat(all_data, ignore_index=True)
        print(f"統合データサイズ: {len(combined_df)}行")
        
        # ドメイン別qtype分析
        analysis_results = analyze_domain_qtype_distribution(combined_df, date_str)
        
        if analysis_results:
            print(f"\n=== ドメイン別Type分析結果（上位20ドメイン） ===")
            sorted_domains = sorted(analysis_results.items(), key=lambda x: x[1]['total_queries'], reverse=True)
            
            for rank, (subdomain, stats) in enumerate(sorted_domains[:20], 1):
                print(f"{rank:2d}. {subdomain:<30} : {stats['total_queries']:8d}件")
                print(f"    ユニークqtype: {stats['unique_qtypes']:2d}, 主要qtype: {stats['top_qtype']:<10} ({stats['top_qtype_ratio']:6.3f}%)")
                
                # 上位3qtypeを表示
                sorted_qtypes = sorted(stats['qtype_distribution'].items(), 
                                     key=lambda x: x[1]['count'], reverse=True)
                for i, (qtype, qtype_stats) in enumerate(sorted_qtypes[:3]):
                    print(f"      {i+1}. {qtype:<8} : {qtype_stats['count']:6d}件 ({qtype_stats['ratio']:6.3f}%)")
                print()
            
            # 結果をCSVに保存
            write_domain_type_analysis_csv(analysis_results, date_str, output_dir)
            
            print(f"総分析ドメイン数: {len(analysis_results)}")
        else:
            print("ドメイン別Type分析の実行に失敗しました")
        
        # エラーログの出力
        if total_error_lines:
            print(f"警告: {len(total_error_lines)}行のエラーが発見されました")
            error_log_file = f"type_per_domain_error_log_{date_str}.txt"
            write_error_log(total_error_lines, file_error_counts, error_log_file)
        
        print(f"\n=== 処理完了: {date_str} ===")
        
    except KeyboardInterrupt:
        print("\n処理が中断されました")
        sys.exit(1)
    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
