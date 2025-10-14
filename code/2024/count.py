import func
import argparse
import os
import pandas as pd

def main():
    # ログ設定
    func.setup_logging("count_analysis")
    
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

    func.logging.info(f"処理開始: {year}-{month}-{day if day else 'ALL'}, where={where}")

    try:
        # ファイルリスト取得
        lst = func.file_lst(year, month, day, where)
        
        if not lst:
            func.logging.error("対象ファイルが見つかりませんでした")
            exit(1)
        
        func.logging.info(f"{len(lst)}個のファイルが見つかりました")
        
        # ファイル時間辞書作成
        file_dic = func.file_time_dict(lst)
        
        if not file_dic:
            func.logging.error("有効なファイルが見つかりませんでした")
            exit(1)

        # 各日付の処理
        processed_days = 0
        failed_days = 0
        
        for current_day in sorted(file_dic.keys()):
            try:
                func.logging.info(f"日付 {current_day} の処理を開始します")
                dom_dic = dict()
                processed_hours = 0
                failed_hours = 0

                for hour in sorted(file_dic[current_day]):
                    try:
                        func.logging.info(f"処理中: {month}{current_day}{hour}")

                        # データフレーム読み込み（エラーハンドリング付き）
                        df = safe_open_reader(year, month, current_day, hour, where)
                        
                        if df.empty:
                            func.logging.warning(f"スキップ: {month}{current_day}{hour} - データがありません")
                            failed_hours += 1
                            continue

                        # クエリカウント（エラーハンドリング付き）
                        func.safe_count_query(df, dom_dic)
                        processed_hours += 1
                        
                    except Exception as e:
                        func.logging.error(f"時間 {hour} の処理中にエラーが発生しました: {e}")
                        failed_hours += 1
                        continue

                # 1日分の処理が完了したらCSVに書き込み
                if dom_dic:
                    # 降順でソート
                    dom_dic = sorted(dom_dic.items(), key=lambda item: item[1], reverse=True)
                    
                    # CSV書き込み（エラーハンドリング付き）
                    if func.safe_write_csv(dom_dic, year, month, current_day, where):
                        processed_days += 1
                        func.logging.info(f"日付 {current_day} の処理完了: {processed_hours}時間処理, {failed_hours}時間失敗")
                    else:
                        failed_days += 1
                        func.logging.error(f"日付 {current_day} のCSV書き込みに失敗しました")
                else:
                    func.logging.warning(f"日付 {current_day}: 有効なデータがありませんでした")
                    failed_days += 1
                    
            except Exception as e:
                func.logging.error(f"日付 {current_day} の処理中に予期しないエラーが発生しました: {e}")
                failed_days += 1
                continue

        # 処理結果サマリー
        func.logging.info(f"全体処理完了: {processed_days}日処理成功, {failed_days}日処理失敗")
        
        if processed_days == 0:
            func.logging.error("すべての処理が失敗しました")
            exit(1)
        
    except Exception as e:
        func.logging.error(f"プログラム実行中に致命的なエラーが発生しました: {e}")
        exit(1)

    func.logging.info("プログラム正常終了")

def safe_open_reader(year, month, day, hour, where):
    """エラーハンドリングを強化したファイル読み込み関数"""
    try:
        # func.pyのopen_reader_safe関数を呼び出し
        df = func.open_reader_safe(year, month, day, hour, where)
        
        if df.empty:
            func.logging.warning(f"空のDataFrameが返されました: {year}-{month}-{day}-{hour}")
            return pd.DataFrame()
        
        return df
        
    except FileNotFoundError as e:
        func.logging.error(f"ファイルが見つかりません: {year}-{month}-{day}-{hour}, エラー: {e}")
        return pd.DataFrame()
    except pd.errors.EmptyDataError as e:
        func.logging.error(f"空のCSVファイルです: {year}-{month}-{day}-{hour}, エラー: {e}")
        return pd.DataFrame()
    except pd.errors.ParserError as e:
        func.logging.error(f"CSVパースエラー: {year}-{month}-{day}-{hour}, エラー: {e}")
        return pd.DataFrame()
    except Exception as e:
        func.logging.error(f"予期しないエラーが発生しました: {year}-{month}-{day}-{hour}, エラー: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    main()
