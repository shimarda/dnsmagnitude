#!/usr/bin/env python3
"""
DNS Type分析ツール (2025年版)

指定された日付のDNSデータからqtype別の詳細分析を実行します。
共通処理は func.py で管理されています。

使用方法:
    python3 type_analysis.py YYYY MM DD

引数:
    YYYY: 年（4桁）
    MM: 月（2桁、ゼロパディング）
    DD: 日（2桁、ゼロパディング）

例:
    python3 type_analysis.py 2025 04 01
"""

import sys
import os
import pandas as pd
from func import (
    file_lst, safe_read_csv, qtype_ratio, extract_subdomain,
    ensure_output_dir, write_error_log
)

def analyze_qtype_details(df, date_str):
    """qtype別の詳細分析"""
    if df.empty or 'qtype' not in df.columns:
        return {}
    
    results = {}
    
    # 有効なqtypeのみを取得
    valid_df = df[df['qtype'].notna()].copy()
    
    if valid_df.empty:
        return results
    
    # qtype別に分析
    for qtype in valid_df['qtype'].unique():
        qtype_df = valid_df[valid_df['qtype'] == qtype].copy()
        
        # 基本統計
        qtype_stats = {
            'count': len(qtype_df),
            'ratio': len(qtype_df) / len(valid_df),
            'unique_domains': 0,
            'top_domains': []
        }
        
        # ドメイン分析（qnameカラムが存在する場合）
        if 'qname' in qtype_df.columns:
            qtype_df['subdomain'] = qtype_df['qname'].apply(extract_subdomain)
            valid_subdomain_df = qtype_df[qtype_df['subdomain'].notna()]
            
            if not valid_subdomain_df.empty:
                domain_counts = valid_subdomain_df['subdomain'].value_counts()
                qtype_stats['unique_domains'] = len(domain_counts)
                qtype_stats['top_domains'] = domain_counts.head(5).to_dict()
        
        results[qtype] = qtype_stats
    
    return results

def write_type_analysis_csv(analysis_results, date_str, output_dir):
    """type分析結果をCSVに書き込み"""
    import csv
    
    # 基本統計CSV
    stats_csv_path = os.path.join(output_dir, f"qtype-stats-{date_str}.csv")
    with open(stats_csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['date', 'qtype', 'count', 'ratio', 'unique_domains'])
        
        for qtype, stats in sorted(analysis_results.items(), key=lambda x: x[1]['count'], reverse=True):
            writer.writerow([
                date_str, qtype, stats['count'], 
                f"{stats['ratio']:.6f}", stats['unique_domains']
            ])
    
    # 上位ドメインCSV
    domains_csv_path = os.path.join(output_dir, f"qtype-top-domains-{date_str}.csv")
    with open(domains_csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['date', 'qtype', 'subdomain', 'count', 'rank'])
        
        for qtype, stats in analysis_results.items():
            for rank, (subdomain, count) in enumerate(stats['top_domains'].items(), 1):
                writer.writerow([date_str, qtype, subdomain, count, rank])
    
    print(f"Type分析統計をCSVに保存: {stats_csv_path}")
    print(f"Type別上位ドメインをCSVに保存: {domains_csv_path}")

def main():
    if len(sys.argv) != 4:
        print(__doc__)
        sys.exit(1)
    
    try:
        year = sys.argv[1]
        month = sys.argv[2].zfill(2)  # ゼロパディング
        day = sys.argv[3].zfill(2)    # ゼロパディング
        
        date_str = f"{year}-{month}-{day}"
        print(f"=== DNS Type分析: {date_str} ===")
        
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
        output_dir = ensure_output_dir("type_analysis")
        
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
        
        # qtype詳細分析
        analysis_results = analyze_qtype_details(combined_df, date_str)
        
        if analysis_results:
            print(f"\n=== Qtype詳細分析結果 ===")
            for qtype, stats in sorted(analysis_results.items(), key=lambda x: x[1]['count'], reverse=True):
                print(f"{qtype:<10} : {stats['count']:8d}件 ({stats['ratio']:6.3f}%) ユニークドメイン: {stats['unique_domains']}")
                
                # 上位ドメイン表示
                if stats['top_domains']:
                    print(f"           上位ドメイン:")
                    for rank, (domain, count) in enumerate(list(stats['top_domains'].items())[:3], 1):
                        print(f"             {rank}. {domain} ({count}件)")
                print()
            
            # 結果をCSVに保存
            write_type_analysis_csv(analysis_results, date_str, output_dir)
        else:
            print("Type分析の実行に失敗しました")
        
        # エラーログの出力
        if total_error_lines:
            print(f"警告: {len(total_error_lines)}行のエラーが発見されました")
            error_log_file = f"type_analysis_error_log_{date_str}.txt"
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
