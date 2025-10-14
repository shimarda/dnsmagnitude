# ネットワーク分類DNS Magnitude分析ツール

## 概要

学内・学外ネットワーク分類を行い、クエリ・レスポンス別にDNS Magnitudeを計算する新機能です。

## 機能

### 1. ネットワーク分類
- **学内者用**: 133.51.112.0/20
- **学外者用**: 133.51.192.0/21  
- **その他**: 上記以外のIPアドレス

### 2. クエリ・レスポンス分離
- **クエリ分析**: `dns.flags.response = 0` のデータを分析
- **レスポンス分析**: `dns.flags.response = 1` かつ `dns.flags.rcode = 0` のデータを分析

### 3. DNS Magnitude計算
- ネットワークタイプ別にDNS Magnitudeを計算
- ドメインの人気度を測定

## 使用方法

### ネットワーク分析の実行

```bash
# クエリ分析
python3 network_analysis.py 2025 04 01 query

# レスポンス分析  
python3 network_analysis.py 2025 04 01 response

# カスタム入力ディレクトリ指定
python3 network_analysis.py 2025 04 01 query /path/to/csv/files/
```

### 結果の表示

```bash
# 基本表示（上位10件）
python3 view_network_results.py 2025-04-01 query

# 上位20件表示
python3 view_network_results.py 2025-04-01 query 20

# レスポンス分析結果表示
python3 view_network_results.py 2025-04-01 response
```

## 出力ファイル

分析結果は `output/network_analysis/` ディレクトリに保存されます。

### ファイル命名規則
```
magnitude-{network_type}-{analysis_type}-{date}.csv
```

例:
- `magnitude-internal-query-2025-04-01.csv`
- `magnitude-external-response-2025-04-01.csv`
- `magnitude-other-query-2025-04-01.csv`

### CSVフォーマット
```csv
date,network_type,analysis_type,subdomain,magnitude
2025-04-01,internal,query,example.tsukuba.ac.jp,8.123456
```

## 新規追加関数（func.py）

### ネットワーク分類関数
- `classify_ip_address(ip_str)`: IPアドレスを学内・学外で分類
- `load_query_response_csv(file_path)`: クエリ・レスポンス用CSV読み込み
- `filter_query_response_data(df, analysis_type)`: クエリ・レスポンスでフィルタリング

### 分析処理関数
- `classify_by_network_and_calculate_magnitude()`: ネットワーク分類とマグニチュード計算
- `write_network_magnitude_csv()`: 結果をCSV出力
- `process_network_analysis_files()`: メイン分析処理

## 実行例

```bash
# 2025年4月1日のクエリデータを分析
python3 network_analysis.py 2025 04 01 query

# 出力例:
# === ネットワーク分類DNS Magnitude分析 ===
# 対象日付: 2025-04-01
# 分析タイプ: query
# 入力ディレクトリ: /mnt/qnap2/shimada/resolver-q-r/
# 
# 処理対象ファイル数: 24
# 
# === 処理中: 2025-04-01 (query) ===
# 読み込み中: 2025-04-01-00.csv
# 読み込み中: 2025-04-01-01.csv
# ...
# 結合後データ数: 123456件
# クエリデータ: 98765件
# internalネットワーク: 45678件
# externalネットワーク: 32109件
# otherネットワーク: 20978件
# 
# === 分析完了 ===
```

## 制限事項

- 既存のfunc.py関数は変更されていません
- 新機能として独立して動作します
- tsharkで抽出されたCSVファイル形式に依存します

## 技術詳細

### IPアドレス分類ロジック
```python
# 学内者用ネットワーク: 133.51.112.0/20 (133.51.112.0 - 133.51.127.255)
# 学外者用ネットワーク: 133.51.192.0/21 (133.51.192.0 - 133.51.199.255)
```

### Magnitude計算式
```
magnitude = 10 * log(対象ドメインのIP数) / log(総IP数)
```

各ネットワークタイプ別に総IP数を計算し、ドメインごとのMagnitudeを算出します。

### ドメイン集計ルール
**重要**: サブドメインは最上位レベルで統合されます。

例:
- `aa.bb.cc.dd.tsukuba.ac.jp` → `dd` として集計
- `aa.bb.ss.dd.tsukuba.ac.jp` → `dd` として集計
- `tmao-sv01.cc.tsukuba.ac.jp` → `cc` として集計
- `corn181.cc.tsukuba.ac.jp` → `cc` として集計
- `www.tsukuba.ac.jp` → `www` として集計

これにより、同一サービス（最上位サブドメイン）への異なる経路のトラフィックが統合されて測定されます。
