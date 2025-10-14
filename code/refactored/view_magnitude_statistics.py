#!/usr/bin/env python3
"""
マグニチュード統計結果表示ツール

統計分析の結果を表示し、ネットワークタイプ・分析タイプ間の比較を行います。

使用方法:
    python3 view_magnitude_statistics.py START_DATE END_DATE [top_n]

引数:
    START_DATE: 開始日 (YYYY-MM-DD形式)
    END_DATE: 終了日 (YYYY-MM-DD形式)  
    top_n: 表示する上位件数 (デフォルト: 10)

例:
    python3 view_magnitude_statistics.py 2025-04-01 2025-04-07
    python3 view_magnitude_statistics.py 2025-04-01 2025-04-30 20
"""

import sys
import os
import pandas as pd
import glob
from func import ensure_output_dir

def load_statistics_results(start_date, end_date, network_type, analysis_type):
    """統計結果CSVを読み込み"""
    output_dir = ensure_output_dir("magnitude_statistics")
    pattern = f"magnitude-stats-{network_type}-{analysis_type}-{start_date}-to-{end_date}.csv"
    file_path = os.path.join(output_dir, pattern)
    
    if not os.path.exists(file_path):
        return pd.DataFrame()
    
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        print(f"ファイル読み込みエラー ({file_path}): {str(e)}")
        return pd.DataFrame()

def display_statistics_summary(start_date, end_date, top_n=10):
    """統計結果のサマリーを表示"""
    print(f"=== マグニチュード統計結果サマリー: {start_date} から {end_date} ===\n")
    
    combinations = [
        ('internal', 'query', '学内クエリ'),
        ('internal', 'response', '学内レスポンス'),
        ('external', 'query', '学外クエリ'),
        ('external', 'response', '学外レスポンス'),
        ('other', 'query', 'その他クエリ'),
        ('other', 'response', 'その他レスポンス')
    ]
    
    all_results = {}
    
    for network_type, analysis_type, description in combinations:
        df = load_statistics_results(start_date, end_date, network_type, analysis_type)
        all_results[(network_type, analysis_type)] = df
        
        print(f"--- {description} ---")
        if df.empty:
            print("データが見つかりませんでした\n")
            continue
        
        print(f"ドメイン数: {len(df)}")
        
        # 全体統計
        total_count = df['count'].sum()
        avg_mean = df['mean'].mean()
        avg_std = df['std'].mean()
        
        print(f"総データ数: {total_count}")
        print(f"平均マグニチュード（全ドメイン平均）: {avg_mean:.6f}")
        print(f"標準偏差（全ドメイン平均）: {avg_std:.6f}")
        
        # 上位ドメイン表示
        top_domains = df.nlargest(min(top_n, len(df)), 'mean')
        print(f"\n上位 {len(top_domains)} ドメイン:")
        print(f"{'順位':<4} {'ドメイン':<20} {'平均':<8} {'分散':<8} {'標準偏差':<8} {'件数':<6}")
        print("-" * 70)
        
        for i, (_, row) in enumerate(top_domains.iterrows(), 1):
            print(f"{i:<4} {row['subdomain']:<20} {row['mean']:<8.4f} "
                  f"{row['var']:<8.4f} {row['std']:<8.4f} {row['count']:<6.0f}")
        print()
    
    return all_results

def compare_network_analysis_types(all_results, top_n=10):
    """ネットワークタイプ・分析タイプ間の比較"""
    print(f"=== ネットワーク・分析タイプ間比較 ===\n")
    
    # クエリ vs レスポンス比較
    print("--- クエリ vs レスポンス比較 ---")
    network_types = ['internal', 'external', 'other']
    
    for network_type in network_types:
        query_df = all_results.get((network_type, 'query'), pd.DataFrame())
        response_df = all_results.get((network_type, 'response'), pd.DataFrame())
        
        print(f"\n{network_type.upper()} ネットワーク:")
        if not query_df.empty and not response_df.empty:
            # 共通ドメインを抽出
            common_domains = set(query_df['subdomain']) & set(response_df['subdomain'])
            
            if common_domains:
                print(f"  共通ドメイン数: {len(common_domains)}")
                
                # 上位共通ドメインの比較
                query_top = query_df[query_df['subdomain'].isin(common_domains)].nlargest(5, 'mean')
                response_top = response_df[response_df['subdomain'].isin(common_domains)].nlargest(5, 'mean')
                
                print(f"  クエリ上位5ドメインの平均マグニチュード: {query_top['mean'].mean():.4f}")
                print(f"  レスポンス上位5ドメインの平均マグニチュード: {response_top['mean'].mean():.4f}")
            else:
                print(f"  共通ドメインが見つかりませんでした")
        else:
            print(f"  データが不足しています")
    
    # 学内 vs 学外比較
    print(f"\n--- 学内 vs 学外比較 ---")
    analysis_types = ['query', 'response']
    
    for analysis_type in analysis_types:
        internal_df = all_results.get(('internal', analysis_type), pd.DataFrame())
        external_df = all_results.get(('external', analysis_type), pd.DataFrame())
        
        print(f"\n{analysis_type.upper()}:")
        if not internal_df.empty and not external_df.empty:
            print(f"  学内ドメイン数: {len(internal_df)}")
            print(f"  学外ドメイン数: {len(external_df)}")
            print(f"  学内平均マグニチュード: {internal_df['mean'].mean():.4f}")
            print(f"  学外平均マグニチュード: {external_df['mean'].mean():.4f}")
            
            # 分散の比較
            print(f"  学内平均分散: {internal_df['var'].mean():.4f}")
            print(f"  学外平均分散: {external_df['var'].mean():.4f}")
        else:
            print(f"  データが不足しています")

def generate_summary_report(all_results, start_date, end_date):
    """サマリーレポートを生成"""
    print(f"\n=== 期間サマリーレポート ({start_date} - {end_date}) ===")
    
    # 全データの統計
    total_domains = 0
    total_measurements = 0
    
    print(f"{'分析タイプ':<20} {'ドメイン数':<10} {'測定数':<10} {'平均マグニチュード':<15}")
    print("-" * 60)
    
    for (network_type, analysis_type), df in all_results.items():
        if not df.empty:
            domain_count = len(df)
            measurement_count = df['count'].sum()
            avg_magnitude = df['mean'].mean()
            
            total_domains += domain_count
            total_measurements += measurement_count
            
            label = f"{network_type}-{analysis_type}"
            print(f"{label:<20} {domain_count:<10} {measurement_count:<10.0f} {avg_magnitude:<15.6f}")
    
    print("-" * 60)
    print(f"{'総計':<20} {total_domains:<10} {total_measurements:<10.0f}")

def main():
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)
    
    try:
        start_date = sys.argv[1]
        end_date = sys.argv[2]
        top_n = int(sys.argv[3]) if len(sys.argv) > 3 else 10
        
        # 日付形式の簡単な検証
        from datetime import datetime
        try:
            datetime.strptime(start_date, '%Y-%m-%d')
            datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError:
            print("エラー: 日付は YYYY-MM-DD 形式で指定してください")
            sys.exit(1)
        
        # 統計結果表示
        all_results = display_statistics_summary(start_date, end_date, top_n)
        compare_network_analysis_types(all_results, top_n)
        generate_summary_report(all_results, start_date, end_date)
        
    except KeyboardInterrupt:
        print("\n処理が中断されました")
        sys.exit(1)
    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
