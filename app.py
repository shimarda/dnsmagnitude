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
DAYS_TO_SHOW = None  # None = 全件表示、数値を設定すれば件数制限

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
        
        df = load_magnitude_data(date_str, where)
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

        if prev_date:
            df_prev = load_magnitude_data(prev_date, where)
            if df_prev is not None:
                # 型変換を行ってマージの安定性を高める
                df['subdomain'] = df['subdomain'].astype(str)
                df['magnitude'] = pd.to_numeric(df['magnitude'], errors='coerce')
                
                # 前日の順位も計算
                df_prev['prev_rank'] = df_prev['magnitude'].rank(ascending=False, method='min')

                df_prev_subset = df_prev[['subdomain', 'magnitude', 'prev_rank']].rename(columns={'magnitude': 'prev_magnitude'})
                df_prev_subset['subdomain'] = df_prev_subset['subdomain'].astype(str)
                df_prev_subset['prev_magnitude'] = pd.to_numeric(df_prev_subset['prev_magnitude'], errors='coerce')
                
                df = pd.merge(df, df_prev_subset, on='subdomain', how='left')
                df['diff'] = df['magnitude'] - df['prev_magnitude']
                # 順位変動: 前日順位 - 当日順位 (プラスなら順位上昇)
                df['rank_diff'] = df['prev_rank'] - df['rank']
            else:
                df['diff'] = None
                df['rank_diff'] = None
        else:
            df['diff'] = None
            df['rank_diff'] = None

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

                trend_data.append({
                    'date': date_display,
                    'magnitude': float(row['magnitude'].iloc[0])
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
        df_auth = load_magnitude_data(d, 0)
        auth_mag = None
        if df_auth is not None:
            row = df_auth[df_auth['subdomain'] == subdomain]
            if not row.empty:
                auth_mag = float(row['magnitude'].iloc[0])
                
        # リゾルバ (1)
        df_res = load_magnitude_data(d, 1)
        res_mag = None
        if df_res is not None:
            row = df_res[df_res['subdomain'] == subdomain]
            if not row.empty:
                res_mag = float(row['magnitude'].iloc[0])
        
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
                'res_mag': res_mag
            })
            
    return jsonify({
        'subdomain': subdomain,
        'data': trend_data
    })

if __name__ == '__main__':
    # 開発用サーバー起動
    app.run(host='0.0.0.0', port=5000, debug=True)