#!/usr/bin/env python3
"""
DNS Magnitude 簡易ダッシュボード
最小限の機能で素早く動作確認できるシステム
"""

from flask import Flask, render_template, jsonify, request
import pandas as pd
import glob
import os
from datetime import datetime, timedelta
import json

app = Flask(__name__)

# 設定
# 複数の出力先を検索する（既存環境に合わせて両方を参照する）
OUTPUT_DIRS = [
    "/home/shimada/analysis/output"
]
OUTPUT_DIR_TIME = "/home/shimada/analysis/output-time"  # 業務時間データ
DAYS_TO_SHOW = None  # None = 全件表示、数値を設定すれば件数制限

# 時間モード設定
TIME_MODES = {
    'daily': '1日（終日）',
    'business': '1日（業務時間 8-18時）',
    'weekly': '1週間'
}

def get_available_dates():
    """利用可能な日付一覧を取得

    - 複数の出力ディレクトリとファイル名パターンに対応する。
    - ファイル名中の YYYY-MM-DD 形式の日付を抽出してユニーク化する。
    """
    # ファイル名から日付を抽出して集合化し、日付そのものの降順で返す
    import re
    date_set = set()

    for outdir in OUTPUT_DIRS:
        try:
            csv_files = glob.glob(os.path.join(outdir, "*.csv"))
        except Exception:
            continue

        for f in csv_files:
            basename = os.path.basename(f)
            try:
                m = re.search(r"(\d{4}-\d{2}-\d{2})", basename)
                if m:
                    date_set.add(m.group(1))
            except Exception:
                continue

    # 日付文字列を datetime に変換して降順ソート（最新日付が先）
    parsed = []
    for d in date_set:
        try:
            dt = datetime.strptime(d, "%Y-%m-%d")
            parsed.append((dt, d))
        except Exception:
            continue

    parsed.sort(key=lambda x: x[0], reverse=True)
    return [d for _, d in parsed]

def load_magnitude_data(date_str, where=0):
    """指定日のMagnitudeデータを読み込み

    複数のディレクトリとファイル名バリエーションを試す。見つかった最初のファイルを返す。
    """
    # 各ディレクトリでいくつかのパターンを試す
    filepath = None
    for outdir in OUTPUT_DIRS:
        try:
            # 典型的なファイル名
            patterns = [
                os.path.join(outdir, f"{where}-{date_str}.csv"),
                os.path.join(outdir, f"*{where}-{date_str}*.csv")
            ]
            for p in patterns:
                matches = glob.glob(p)
                if matches:
                    # 最も新しいファイルを採用
                    matches.sort(key=lambda x: os.path.getmtime(x), reverse=True)
                    filepath = matches[0]
                    break
        except Exception:
            continue
        if filepath:
            break

    if filepath is None:
        return None

    try:
        df = pd.read_csv(filepath)
        # カラム名の正規化
        if 'domain' in df.columns:
            df = df.rename(columns={'domain': 'subdomain'})
        if 'dnsmagnitude' in df.columns:
            df = df.rename(columns={'dnsmagnitude': 'magnitude'})
        return df
    except Exception as e:
        print(f"Error loading {filepath}: {e}")
        return None

def load_magnitude_data_with_mode(date_str, where=0, time_mode='daily'):
    """指定日のMagnitudeデータを時間モードに応じて読み込み
    
    Args:
        date_str: 日付文字列 (YYYY-MM-DD)
        where: 0=権威サーバー, 1=リゾルバ
        time_mode: 'daily', 'business', 'weekly'
    """
    if time_mode == 'daily':
        return load_magnitude_data(date_str, where)
    
    elif time_mode == 'business':
        # 業務時間(08-18)データを読み込む
        filepath = os.path.join(OUTPUT_DIR_TIME, f"{where}-{date_str}-08-18.csv")
        if not os.path.exists(filepath):
            # フォールバック: 通常の日次データ
            return load_magnitude_data(date_str, where)
        
        try:
            df = pd.read_csv(filepath)
            if 'domain' in df.columns:
                df = df.rename(columns={'domain': 'subdomain'})
            if 'dnsmagnitude' in df.columns:
                df = df.rename(columns={'dnsmagnitude': 'magnitude'})
            
            # クエリカウントデータを読み込んでマージ
            count_filepath = os.path.join(OUTPUT_DIR_TIME, f"count-{where}-{date_str}-08-18.csv")
            if os.path.exists(count_filepath):
                try:
                    df_count = pd.read_csv(count_filepath)
                    if 'subdomain' in df_count.columns and 'query_count' in df_count.columns:
                        # subdomainをキーにしてカウントをマージ
                        df_count_subset = df_count[['subdomain', 'query_count']].rename(
                            columns={'query_count': 'count'}
                        )
                        df = pd.merge(df, df_count_subset, on='subdomain', how='left')
                except Exception as e:
                    print(f"Warning: Could not load count data: {e}")
            
            return df
        except Exception as e:
            print(f"Error loading {filepath}: {e}")
            return None
    
    elif time_mode == 'weekly':
        # 週単位データ: 指定日を含む週（月曜〜日曜）のデータを集約
        return load_weekly_data(date_str, where)
    
    return None

def load_weekly_data(date_str, where=0):
    """指定日を含む週のデータを集約して返す"""
    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return None
    
    # 週の月曜日を取得
    monday = target_date - timedelta(days=target_date.weekday())
    
    # 月曜から日曜までのデータを集約
    all_data = []
    for i in range(7):
        day = monday + timedelta(days=i)
        day_str = day.strftime("%Y-%m-%d")
        df = load_magnitude_data(day_str, where)
        if df is not None and not df.empty:
            all_data.append(df)
    
    if not all_data:
        return None
    
    # 全データを結合
    combined = pd.concat(all_data, ignore_index=True)
    
    # サブドメインごとに集計（平均値を使用）
    if 'subdomain' not in combined.columns:
        return None
    
    agg_cols = {'magnitude': 'mean'}
    if 'count' in combined.columns:
        agg_cols['count'] = 'sum'
    
    result = combined.groupby('subdomain').agg(agg_cols).reset_index()
    
    return result

@app.route('/')
def index():
    """メインダッシュボード"""
    dates = get_available_dates()
    latest_date = dates[0] if dates else None
    
    # DAYS_TO_SHOW が None なら全件、数値なら制限
    display_dates = dates if DAYS_TO_SHOW is None else dates[:DAYS_TO_SHOW]
    
    return render_template('index.html', 
                         latest_date=latest_date,
                         available_dates=display_dates)

@app.route('/api/magnitude/<date_str>')
def api_magnitude(date_str):
    """指定日のMagnitudeデータをJSONで返す"""
    try:
        where = request.args.get('where', 0, type=int)
        top_n = request.args.get('top', type=int)  # デフォルトなし = 全件
        time_mode = request.args.get('time_mode', 'daily', type=str)
        
        df = load_magnitude_data_with_mode(date_str, where, time_mode)
        if df is None:
            return jsonify({'error': 'Data not found'}), 404
        
        # 前日データの取得と差分計算
        dates = get_available_dates()
        prev_date = None
        try:
            idx = dates.index(date_str)
            if idx + 1 < len(dates):
                prev_date = dates[idx + 1]
        except ValueError:
            pass
            
        # 順位計算 (Magnitudeの降順)
        df['rank'] = df['magnitude'].rank(ascending=False, method='min')
        if 'count' in df.columns:
            df['count_rank'] = df['count'].rank(ascending=False, method='min')

        if prev_date:
            df_prev = load_magnitude_data(prev_date, where)
            if df_prev is not None:
                # 型変換を行ってマージの安定性を高める
                df['subdomain'] = df['subdomain'].astype(str)
                df['magnitude'] = pd.to_numeric(df['magnitude'], errors='coerce')
                if 'count' in df.columns:
                    df['count'] = pd.to_numeric(df['count'], errors='coerce')
                
                # 前日の順位も計算
                df_prev['prev_rank'] = df_prev['magnitude'].rank(ascending=False, method='min')
                
                cols_to_use = ['subdomain', 'magnitude', 'prev_rank']
                rename_map = {'magnitude': 'prev_magnitude'}
                
                if 'count' in df_prev.columns:
                    df_prev['prev_count_rank'] = df_prev['count'].rank(ascending=False, method='min')
                    cols_to_use.extend(['count', 'prev_count_rank'])
                    rename_map['count'] = 'prev_count'

                df_prev_subset = df_prev[cols_to_use].rename(columns=rename_map)
                df_prev_subset['subdomain'] = df_prev_subset['subdomain'].astype(str)
                df_prev_subset['prev_magnitude'] = pd.to_numeric(df_prev_subset['prev_magnitude'], errors='coerce')
                if 'prev_count' in df_prev_subset.columns:
                    df_prev_subset['prev_count'] = pd.to_numeric(df_prev_subset['prev_count'], errors='coerce')
                
                df = pd.merge(df, df_prev_subset, on='subdomain', how='left')
                df['diff'] = df['magnitude'] - df['prev_magnitude']
                # 順位変動: 前日順位 - 当日順位 (プラスなら順位上昇)
                df['rank_diff'] = df['prev_rank'] - df['rank']
                
                if 'count' in df.columns and 'prev_count' in df.columns:
                    df['count_diff'] = df['count'] - df['prev_count']
                    df['count_rank_diff'] = df['prev_count_rank'] - df['count_rank']
            else:
                df['diff'] = None
                df['rank_diff'] = None
                if 'count' in df.columns:
                    df['count_diff'] = None
                    df['count_rank_diff'] = None
        else:
            df['diff'] = None
            df['rank_diff'] = None
            if 'count' in df.columns:
                df['count_diff'] = None
                df['count_rank_diff'] = None

        # Top N を取得（指定がある場合のみ制限）
        if top_n is not None:
            top_data = df.nlargest(top_n, 'magnitude')
        else:
            # 全ドメインを magnitude の降順で取得
            top_data = df.sort_values('magnitude', ascending=False)
        
        # NaN (float) を None に変換してJSON化可能にする
        # DataFrame全体をobject型に変換してから置換することで、float列にNoneが入ることを許容する
        top_data = top_data.astype(object).where(pd.notnull(top_data), None)
        domains_list = top_data.to_dict('records')
        
        result = {
            'date': date_str,
            'where': where,
            'total_domains': len(df),
            'domains': domains_list
        }
        
        return jsonify(result)
    except Exception as e:
        app.logger.error(f"Error in api_magnitude: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/trend/<subdomain>')
def api_trend(subdomain):
    """特定サブドメインのトレンドデータ"""
    where = request.args.get('where', 0, type=int)
    days = request.args.get('days', 30, type=int)
    
    dates = get_available_dates()[:days]
    trend_data = []
    
    for date_str in reversed(dates):
        df = load_magnitude_data(date_str, where)
        if df is not None:
            row = df[df['subdomain'] == subdomain]
            if not row.empty:
                # 曜日を追加
                dt = datetime.strptime(date_str, "%Y-%m-%d")
                weekdays = ["月", "火", "水", "木", "金", "土", "日"]
                weekday = weekdays[dt.weekday()]
                date_display = f"{date_str} ({weekday})"

                val = float(row['magnitude'].iloc[0])
                cnt = float(row['count'].iloc[0]) if 'count' in row.columns else None

                trend_data.append({
                    'date': date_display,
                    'magnitude': val,
                    'count': cnt
                })
    
    return jsonify({
        'subdomain': subdomain,
        'data': trend_data
    })

@app.route('/compare')
def compare():
    """権威 vs リゾルバ比較ページ"""
    dates = get_available_dates()
    display_dates = dates if DAYS_TO_SHOW is None else dates[:DAYS_TO_SHOW]
    return render_template('compare.html', dates=display_dates)

@app.route('/trend')
def trend_page():
    """トレンド表示ページ"""
    dates = get_available_dates()
    latest_date = dates[0] if dates else datetime.now().strftime("%Y-%m-%d")
    return render_template('trend.html', latest_date=latest_date)

@app.route('/api/trend_combined')
def api_trend_combined():
    """特定サブドメインの権威・リゾルバ両方のトレンドデータ"""
    subdomain = request.args.get('subdomain')
    start_date_str = request.args.get('start')
    end_date_str = request.args.get('end')
    time_mode = request.args.get('time_mode', 'daily', type=str)
    
    if not subdomain:
        return jsonify({'error': 'Subdomain is required'}), 400
        
    # 日付範囲の生成
    try:
        start_dt = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date_str, "%Y-%m-%d")
    except (ValueError, TypeError):
        # デフォルト: 過去30日
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=30)
    
    # 日付リスト作成 (start -> end)
    date_list = []
    curr = start_dt
    while curr <= end_dt:
        date_list.append(curr.strftime("%Y-%m-%d"))
        curr += timedelta(days=1)
        
    trend_data = []
    
    for d in date_list:
        # 権威 (0)
        df_auth = load_magnitude_data_with_mode(d, 0, time_mode)
        auth_mag = None
        auth_cnt = None
        if df_auth is not None:
            row = df_auth[df_auth['subdomain'] == subdomain]
            if not row.empty:
                auth_mag = float(row['magnitude'].iloc[0])
                if 'count' in row.columns:
                    auth_cnt = float(row['count'].iloc[0])
                
        # リゾルバ (1)
        df_res = load_magnitude_data_with_mode(d, 1, time_mode)
        res_mag = None
        res_cnt = None
        if df_res is not None:
            row = df_res[df_res['subdomain'] == subdomain]
            if not row.empty:
                res_mag = float(row['magnitude'].iloc[0])
                if 'count' in row.columns:
                    res_cnt = float(row['count'].iloc[0])
        
        # どちらかがあればデータとして追加
        if auth_mag is not None or res_mag is not None:
            # 曜日を追加
            dt = datetime.strptime(d, "%Y-%m-%d")
            weekdays = ["月", "火", "水", "木", "金", "土", "日"]
            weekday = weekdays[dt.weekday()]
            date_display = f"{d} ({weekday})"

            trend_data.append({
                'date': date_display,
                'auth_mag': auth_mag,
                'res_mag': res_mag,
                'auth_cnt': auth_cnt,
                'res_cnt': res_cnt
            })
            
    return jsonify({
        'subdomain': subdomain,
        'time_mode': time_mode,
        'data': trend_data
    })

if __name__ == '__main__':
    # 開発用サーバー起動
    app.run(host='0.0.0.0', port=5000, debug=True)