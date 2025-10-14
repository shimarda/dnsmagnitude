#!/usr/bin/env python3
"""
新フォーマット対応: 既存スクリプト互換実行ツール

従来のスクリプトを新しいCSVフォーマット（クエリ+レスポンス）に対応させます。
パケットタイプを指定して分析が可能です。

使用方法:
    python3 run_analysis_v2.py <analysis_type> <year> <month> <day> <where> [packet_type]

引数:
    analysis_type: count, qtype, qtype_total のいずれか
    year: 年（4桁）
    month: 月（2桁、例: 04）
    day: 日（2桁、例: 01）
    where: 0=権威サーバー、1=リゾルバー
    packet_type: query または response（デフォルト: query）

例:
    # 問い合わせパケットのカウント分析
    python3 run_analysis_v2.py count 2025 04 01 1 query
    
    # 応答パケット（rcode=0のみ）のQtype分析
    python3 run_analysis_v2.py qtype 2025 04 01 1 response
    
    # 問い合わせパケットの期間Qtype分析
    python3 run_analysis_v2.py qtype_total 2025 04 * 1 query
"""

import sys
import os
from func import (
    file_lst, file_time, count_query, write_csv, 
    qtype_ratio, qtype_ratio_total,
    open_reader_safe
)

def run_count_analysis(year, month, day, where, packet_type="query"):
    """カウント分析実行"""
    files = file_lst(year, month, day, where)
    if not files:
        print(f"対象ファイルが見つかりません: {year}-{month}-{day}")
        return
    
    domain_dict = {}
    time_list = file_time(files)
    
    for file_path, time_str in zip(files, time_list):
        print(f"処理中: {os.path.basename(file_path)} ({packet_type})")
        
        # 時間文字列から年月日時を抽出
        hour = time_str[-2:]  # 最後の2文字が時間
        
        # パケットタイプ指定でデータ読み込み
        df = open_reader_safe(year, month, day, hour, where, packet_type)
        if df.empty:
            continue
        
        count_query(df, domain_dict, packet_type)
    
    # 結果をソートして出力
    sorted_domains = sorted(domain_dict.items(), key=lambda x: x[1], reverse=True)
    write_csv(sorted_domains, year, month, day, where, packet_type)
    
    print(f"カウント分析完了: {len(domain_dict)}ドメイン")
    print(f"総クエリ数: {sum(domain_dict.values()):,}")

def main():
    if len(sys.argv) < 6:
        print(__doc__)
        sys.exit(1)
    
    analysis_type = sys.argv[1]
    year = sys.argv[2]
    month = sys.argv[3]
    day = sys.argv[4]
    where = sys.argv[5]
    packet_type = sys.argv[6] if len(sys.argv) > 6 else "query"
    
    # パケットタイプ検証
    if packet_type not in ["query", "response"]:
        print("パケットタイプは 'query' または 'response' を指定してください")
        sys.exit(1)
    
    print(f"=== 新フォーマット対応DNS分析 ===")
    print(f"分析タイプ: {analysis_type}")
    print(f"対象日付: {year}-{month}-{day}")
    print(f"パケットタイプ: {packet_type}")
    print(f"データソース: {'権威サーバー' if where == '0' else 'リゾルバー'}")
    
    if packet_type == "query":
        print("📤 問い合わせパケット (dns.flags.response=0) を分析")
    else:
        print("📥 応答パケット (dns.flags.response=1, rcode=0) を分析")
    
    print("")
    
    try:
        if analysis_type == "count":
            run_count_analysis(year, month, day, where, packet_type)
            
        elif analysis_type == "qtype":
            qtype_ratio(year, month, day, where, packet_type)
            
        elif analysis_type == "qtype_total":
            qtype_ratio_total(year, month, day, where, packet_type)
            
        else:
            print(f"不正な分析タイプ: {analysis_type}")
            print("有効な分析タイプ: count, qtype, qtype_total")
            sys.exit(1)
            
    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
