# import func
# import argparse
# import os

# def file_time(file_lst):
#     file_dic = dict()

#     time_list = [
#         os.path.basename(f).replace('-', '').replace('.csv', '') for f in file_lst
#     ]

#     for time in time_list:
#         month = time[4:6]
#         day = time[6:8]
#         hour = time[8:10]
#         if day not in file_dic.keys():
#             file_dic[day] = list()
#         file_dic[day].append(hour)
#     return file_dic

# if __name__ == "__main__":

#     parser = argparse.ArgumentParser()
#     parser.add_argument('-y', help='year')
#     parser.add_argument('-m', help='month')
#     parser.add_argument('-d', help='day')
#     parser.add_argument('-w', help='0なら権威 1ならリゾルバ')

#     args = parser.parse_args()

#     year = args.y
#     month = args.m
#     day = args.d
#     where = args.w

#     lst = func.file_lst(year, month, day, where)
#     file_dic = file_time(lst)

#     for day in file_dic.keys():
#         dom_dic = dict()

#         for hour in file_dic[day]:
#             print(month+day+hour)

#             df = func.open_reader_safe(year, month, day, hour, where)
#             func.count_query(df, dom_dic)
#         dom_dic = sorted(dom_dic.items(), key=lambda item: item[1], reverse=True)
#     # csvへの書き込み
#     # funcで作成
#         file_path = f"/home/shimada/analysis/output-2025/count-{where}-{year}-{month}-{day}.csv"
#         func.write_csv(dom_dic, year, month, day, where)

import func
import argparse
import os
import pandas as pd
import logging

def setup_logging():
    """ログ設定を初期化"""
    log_dir = "/home/shimada/analysis/logs"
    os.makedirs(log_dir, exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(os.path.join(log_dir, 'dns_analysis.log')),
            logging.StreamHandler()
        ]
    )

def file_time(file_lst):
    """ファイルリストから日付と時間の辞書を作成"""
    file_dic = dict()

    time_list = [
        os.path.basename(f).replace('-', '').replace('.csv', '') for f in file_lst
    ]

    for time in time_list:
        try:
            month = time[4:6]
            day = time[6:8]
            hour = time[8:10]
            if day not in file_dic.keys():
                file_dic[day] = list()
            file_dic[day].append(hour)
        except IndexError as e:
            logging.warning(f"ファイル名の形式が不正です: {time}, エラー: {e}")
            continue
    
    return file_dic

def safe_count_query(df, dom_dic):
    """エラーハンドリングを強化したクエリカウント関数"""
    try:
        if df.empty:
            logging.warning("空のDataFrameが渡されました")
            return
        
        # 必要な列が存在するかチェック
        if 'dns.qry.name' not in df.columns:
            logging.error("dns.qry.name列が見つかりません")
            return
        
        # サブドメイン抽出処理
        df['subdom'] = df['dns.qry.name'].apply(lambda x: func.extract_subdomain(x) if pd.notnull(x) else None)
        
        # 有効な行のみフィルタリング
        df_sub = df[df['subdom'].notnull()]
        
        if df_sub.empty:
            logging.info("有効なサブドメインが見つかりませんでした")
            return
        
        # カウント処理
        hourly_counts = df_sub['subdom'].value_counts().to_dict()
        for dom, c in hourly_counts.items():
            dom_dic[dom] = dom_dic.get(dom, 0) + c
            
        logging.info(f"処理完了: {len(hourly_counts)}個のサブドメインを処理しました")
        
    except Exception as e:
        logging.error(f"count_query処理中にエラーが発生しました: {e}")
        # エラーが発生してもプログラムは継続

def safe_open_reader(year, month, day, hour, where):
    """エラーハンドリングを強化したファイル読み込み関数"""
    try:
        # 元のopen_reader_safe関数を呼び出し
        df = func.open_reader_safe(year, month, day, hour, where)
        
        if df.empty:
            logging.warning(f"空のDataFrameが返されました: {year}-{month}-{day}-{hour}")
            return pd.DataFrame()
        
        return df
        
    except FileNotFoundError as e:
        logging.error(f"ファイルが見つかりません: {year}-{month}-{day}-{hour}, エラー: {e}")
        return pd.DataFrame()
    except pd.errors.EmptyDataError as e:
        logging.error(f"空のCSVファイルです: {year}-{month}-{day}-{hour}, エラー: {e}")
        return pd.DataFrame()
    except pd.errors.ParserError as e:
        logging.error(f"CSVパースエラー: {year}-{month}-{day}-{hour}, エラー: {e}")
        return pd.DataFrame()
    except Exception as e:
        logging.error(f"予期しないエラーが発生しました: {year}-{month}-{day}-{hour}, エラー: {e}")
        return pd.DataFrame()

def safe_write_csv(dom_dic, year, month, day, where):
    """エラーハンドリングを強化したCSV書き込み関数"""
    try:
        if not dom_dic:
            logging.warning(f"書き込むデータがありません: {year}-{month}-{day}")
            return False
        
        # 出力ディレクトリの作成
        output_dir = "/home/shimada/analysis/output-2025"
        os.makedirs(output_dir, exist_ok=True)
        
        # 元のwrite_csv関数を呼び出し
        func.write_csv(dom_dic, year, month, day, where)
        
        logging.info(f"CSVファイルが正常に書き込まれました: count-{where}-{year}-{month}-{day}.csv")
        return True
        
    except PermissionError as e:
        logging.error(f"ファイル書き込み権限エラー: {e}")
        return False
    except Exception as e:
        logging.error(f"CSV書き込み中にエラーが発生しました: {e}")
        return False

if __name__ == "__main__":
    # ログ設定
    setup_logging()
    
    parser = argparse.ArgumentParser(description='DNSログ解析スクリプト')
    parser.add_argument('-y', required=True, help='年')
    parser.add_argument('-m', required=True, help='月')
    parser.add_argument('-d', help='日（省略可）')
    parser.add_argument('-w', required=True, help='0なら権威 1ならリゾルバ')

    args = parser.parse_args()

    year = args.y
    month = args.m
    day = args.d
    where = args.w

    logging.info(f"処理開始: {year}-{month}-{day if day else 'ALL'}, where={where}")

    try:
        # ファイルリスト取得
        lst = func.file_lst(year, month, day, where)
        
        if not lst:
            logging.error("対象ファイルが見つかりませんでした")
            exit(1)
        
        logging.info(f"{len(lst)}個のファイルが見つかりました")
        
        # ファイル時間辞書作成
        file_dic = file_time(lst)
        
        if not file_dic:
            logging.error("有効なファイルが見つかりませんでした")
            exit(1)

        # 各日付の処理
        processed_days = 0
        failed_days = 0
        
        for current_day in sorted(file_dic.keys()):
            try:
                logging.info(f"日付 {current_day} の処理を開始します")
                dom_dic = dict()
                processed_hours = 0
                failed_hours = 0

                for hour in sorted(file_dic[current_day]):
                    try:
                        logging.info(f"処理中: {month}{current_day}{hour}")

                        # データフレーム読み込み（エラーハンドリング付き）
                        df = safe_open_reader(year, month, current_day, hour, where)
                        
                        if df.empty:
                            logging.warning(f"スキップ: {month}{current_day}{hour} - データがありません")
                            failed_hours += 1
                            continue

                        # クエリカウント（エラーハンドリング付き）
                        safe_count_query(df, dom_dic)
                        processed_hours += 1
                        
                    except Exception as e:
                        logging.error(f"時間 {hour} の処理中にエラーが発生しました: {e}")
                        failed_hours += 1
                        continue

                # 1日分の処理が完了したらCSVに書き込み
                if dom_dic:
                    # 降順でソート
                    dom_dic = sorted(dom_dic.items(), key=lambda item: item[1], reverse=True)
                    
                    # CSV書き込み（エラーハンドリング付き）
                    if safe_write_csv(dom_dic, year, month, current_day, where):
                        processed_days += 1
                        logging.info(f"日付 {current_day} の処理完了: {processed_hours}時間処理, {failed_hours}時間失敗")
                    else:
                        failed_days += 1
                        logging.error(f"日付 {current_day} のCSV書き込みに失敗しました")
                else:
                    logging.warning(f"日付 {current_day}: 有効なデータがありませんでした")
                    failed_days += 1
                    
            except Exception as e:
                logging.error(f"日付 {current_day} の処理中に予期しないエラーが発生しました: {e}")
                failed_days += 1
                continue

        # 処理結果サマリー
        logging.info(f"全体処理完了: {processed_days}日処理成功, {failed_days}日処理失敗")
        
        if processed_days == 0:
            logging.error("すべての処理が失敗しました")
            exit(1)
        
    except Exception as e:
        logging.error(f"プログラム実行中に致命的なエラーが発生しました: {e}")
        exit(1)

    logging.info("プログラム正常終了")