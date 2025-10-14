# マグニチュード統計分析ツール

## 概要

ネットワーク分類別・分析タイプ別のDNS Magnitudeデータの統計分析（平均、分散、標準偏差など）を行うツールです。

## 機能

### 統計指標
各ドメインについて以下の統計を計算します：
- **count**: データ数（日数）
- **mean**: 平均マグニチュード
- **std**: 標準偏差
- **var**: 分散
- **min**: 最小値
- **max**: 最大値
- **median**: 中央値
- **q25**: 第1四分位数
- **q75**: 第3四分位数

### 分析対象
- **ネットワークタイプ**: internal, external, other
- **分析タイプ**: query, response
- **期間**: 指定した開始日から終了日まで

## 使用方法

### 1. 統計分析の実行

```bash
# 2日間の統計分析
python3 magnitude_statistics.py 2025-04-01 2025-04-02

# 1週間の統計分析
python3 magnitude_statistics.py 2025-04-01 2025-04-07

# 1ヶ月の統計分析
python3 magnitude_statistics.py 2025-04-01 2025-04-30

# カスタム入力ディレクトリ指定
python3 magnitude_statistics.py 2025-04-01 2025-04-07 /path/to/magnitude/files/
```

### 2. 統計結果の表示

```bash
# 基本表示（上位10件）
python3 view_magnitude_statistics.py 2025-04-01 2025-04-02

# 上位20件表示
python3 view_magnitude_statistics.py 2025-04-01 2025-04-07 20
```

## 出力ファイル

統計結果は `output/magnitude_statistics/` ディレクトリに保存されます。

### ファイル命名規則
```
magnitude-stats-{network_type}-{analysis_type}-{start_date}-to-{end_date}.csv
```

例:
- `magnitude-stats-internal-response-2025-04-01-to-2025-04-07.csv`
- `magnitude-stats-external-query-2025-04-01-to-2025-04-30.csv`

### CSVフォーマット
```csv
network_type,analysis_type,subdomain,count,mean,std,var,min,max,median,q25,q75,start_date,end_date
external,response,cc,7,8.945123,0.125431,0.015733,8.756234,9.123456,8.967891,8.834567,9.045678,2025-04-01,2025-04-07
```

## 実行例

### 統計分析実行
```bash
python3 magnitude_statistics.py 2025-04-01 2025-04-02

# 出力例:
# === マグニチュード統計分析: 2025-04-01 から 2025-04-02 ===
# 
# --- external-response 統計分析中 ---
# 対象データ数: 150件
# 対象日数: 2日
# 対象ドメイン数: 95ドメイン
# 統計結果を保存: output/magnitude_statistics/magnitude-stats-external-response-2025-04-01-to-2025-04-02.csv
# 上位5ドメインの統計:
#   1. cc             : 平均= 9.008, 分散= 0.000, 件数=2
#   2. sec            : 平均= 6.900, 分散= 0.142, 件数=2
#   3. www            : 平均= 6.573, 分散= 0.150, 件数=2
```

### 統計結果表示
```bash
python3 view_magnitude_statistics.py 2025-04-01 2025-04-02

# 出力例:
# === マグニチュード統計結果サマリー: 2025-04-01 から 2025-04-02 ===
# 
# --- 学外レスポンス ---
# ドメイン数: 95
# 総データ数: 150
# 平均マグニチュード（全ドメイン平均）: 1.736045
# 標準偏差（全ドメイン平均）: 0.334078
# 
# 上位 10 ドメイン:
# 順位   ドメイン                 平均       分散       標準偏差     件数    
# ----------------------------------------------------------------------
# 1    cc                   9.0081   0.0002   0.0156   2     
# 2    sec                  6.8995   0.1418   0.3766   2     
```

## 分析の活用方法

### 1. 期間変動の把握
- 日次・週次・月次での変動パターンの把握
- 分散値による安定性の評価

### 2. ネットワーク比較
- 学内 vs 学外のアクセスパターン比較
- クエリ vs レスポンスの特性比較

### 3. ドメイン人気度トレンド
- 継続的に高いマグニチュードを持つ安定ドメイン
- 変動が大きい不安定ドメインの特定

### 4. 異常検知
- 平均値から大きく外れる異常値の検出
- 急激な変動を示すドメインの監視

## 技術詳細

### 統計計算
```python
# 各ドメインの期間内データから計算
stats = {
    'mean': magnitudes.mean(),
    'std': magnitudes.std(),
    'var': magnitudes.var(),
    'min': magnitudes.min(),
    'max': magnitudes.max(),
    'median': magnitudes.median()
}
```

### データ前処理
- magnitude-*.csvファイルからの自動読み込み
- 指定期間でのフィルタリング
- ネットワークタイプ・分析タイプ別の分離処理

## 依存関係

- 事前に `network_analysis.py` でマグニチュードデータを生成済みであること
- `output/network_analysis/` に magnitude-*.csv ファイルが存在すること
