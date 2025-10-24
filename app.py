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
OUTPUT_DIR = "/home/shimada/analysis/output"
DAYS_TO_SHOW = 30

def get_available_dates():
    """利用可能な日付一覧を取得"""
    files = glob.glob(os.path.join(OUTPUT_DIR, "0-*.csv"))
    dates = []
    for f in files:
        basename = os.path.basename(f)
        # 0-YYYY-MM-DD.csv から日付抽出
        try:
            date_str = basename.split('-', 1)[1].replace('.csv', '')
            dates.append(date_str)
        except:
            continue
    return sorted(dates, reverse=True)

def load_magnitude_data(date_str, where=0):
    """指定日のMagnitudeデータを読み込み"""
    filepath = os.path.join(OUTPUT_DIR, f"{where}-{date_str}.csv")
    if not os.path.exists(filepath):
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
    
    return render_template('index.html', 
                         latest_date=latest_date,
                         available_dates=dates[:DAYS_TO_SHOW])

@app.route('/api/magnitude/<date_str>')
def api_magnitude(date_str):
    """指定日のMagnitudeデータをJSONで返す"""
    where = request.args.get('where', 0, type=int)
    top_n = request.args.get('top', type=int)  # デフォルトなし = 全件
    
    df = load_magnitude_data(date_str, where)
    if df is None:
        return jsonify({'error': 'Data not found'}), 404
    
    # Top N を取得（指定がある場合のみ制限）
    if top_n is not None:
        top_data = df.nlargest(top_n, 'magnitude')
    else:
        # 全ドメインを magnitude の降順で取得
        top_data = df.sort_values('magnitude', ascending=False)
    
    result = {
        'date': date_str,
        'where': where,
        'total_domains': len(df),
        'domains': top_data.to_dict('records')
    }
    
    return jsonify(result)

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
                trend_data.append({
                    'date': date_str,
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
    return render_template('compare.html', dates=dates[:DAYS_TO_SHOW])

if __name__ == '__main__':
    # 開発用サーバー起動
    app.run(host='0.0.0.0', port=5000, debug=True)