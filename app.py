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
    display_dates = dates if DAYS_TO_SHOW is None else dates[:DAYS_TO_SHOW]
    return render_template('compare.html', dates=display_dates)

if __name__ == '__main__':
    # 開発用サーバー起動
    app.run(host='0.0.0.0', port=5000, debug=True)