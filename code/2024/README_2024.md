# DNSログ解析ツール（2024年版リファクタリング）

2024年版のDNSログ解析ツールをリファクタリングし、コードの管理しやすさを向上させました。

## 主な変更点

### 1. 出力先の統一
- すべての出力は `/home/shimada/code/refactored/output-2024/` 配下に保存されます
- サブディレクトリ別に結果が整理されます：
  - `count/`: ドメインカウント結果（IP数ベース）
  - `dns_mag/`: DNSマグニチュード結果（対数ベース）
  - `ave-distr/`: 平均値・分散結果
  - `graphs/`: グラフ出力
  - `logs/`: ログファイル

### 2. 共通処理の関数化
- すべての共通処理を `func2024.py` に集約
- 各スクリプトは `func2024.py` の関数を呼び出すだけのシンプルな構造
- エラーハンドリングと logging の統一

### 3. コードの機能別分離
- `count_2024.py`: ドメインカウント処理（IP数ベース）
- `count-ip_2024.py`: 送信先IPアドレス数カウント
- `tshark-args_2024.py`: DNSマグニチュード計算（対数ベース）
- `magnitude-average-variance_2024.py`: 平均値・分散計算
- `freq_table_2024.py`: 度数分布表作成
- `correlation_2024.py`: 相関分析
- `graph-bar_2024.py`: バーグラフ作成

## 使用方法

### 環境設定
出力ディレクトリは自動的に作成されます。

### 基本的な使用例

#### 1. ドメインカウント分析（IP数ベース）
```bash
python3 count_2024.py -y 2024 -m 04 -d 01
```

#### 2. 送信先IPアドレス数カウント
```bash
python3 count-ip_2024.py -y 2024 -m 04 -d 01
```

#### 3. DNSマグニチュード計算（対数ベース）
```bash
python3 tshark-args_2024.py -y 2024 -m 04 -d 01 -w 0
```

#### 4. 平均値・分散計算
```bash
python3 magnitude-average-variance_2024.py -y 2024 -m 04 -d 01 -w 0
```

#### 5. 度数分布表作成
```bash
python3 freq_table_2024.py /path/to/average_file.csv --threshold 5
```

#### 6. 相関分析
```bash
python3 correlation_2024.py average_file.csv distribution_file.csv --output-file correlation.png
```

#### 7. バーグラフ作成
```bash
python3 graph-bar_2024.py -y 2024 -m 04 -d 01 --data-dir /path/to/data/
```

### パラメータ説明
- `-y`: 年
- `-m`: 月 
- `-d`: 日
- `-w`: データソース（0=権威DNS, 1=リゾルバー）
- `--threshold`: 閾値（度数分布表用）
- `--output-file`: 出力ファイル名
- `--data-dir`: データディレクトリ

## func2024.py の主要な関数

### ファイル操作
- `file_lst()`: パターンに一致するファイルリスト取得
- `file_time()`, `file_time_dict()`: ファイル名から時間情報抽出
- `find_output_files()`, `find_output_files_by_pattern()`: 出力ファイルの検索

### データ処理
- `extract_subdomain()`: サブドメイン抽出
- `open_reader()`, `open_reader_safe()`: 安全なファイル読み込み
- `count_dst_ip()`: 送信先IPアドレス数カウント

### マグニチュード計算
- `calculate_magnitude_2024()`: 2024年版マグニチュード計算（IP数ベース）
- `process_magnitude_hourly_data_2024()`: 時間別データ処理

### 出力処理
- `write_magnitude_csv_2024()`: マグニチュード結果出力（IP数ベース）
- `write_tshark_args_csv()`: tshark-args用出力（対数ベース）
- `write_average_distribution_csv_2024()`: 平均値・分散出力

### 統計分析
- `calculate_average_distribution_2024()`: 平均値・分散計算
- `make_frequency_table()`: 度数分布表作成

### グラフ用処理
- `load_graph_data()`: グラフ用データ読み込み
- `create_color_map()`: カラーマップ作成

### ユーティリティ
- `setup_logging()`: ログ設定
- `ensure_output_dir()`: 出力ディレクトリ作成
- `sort_lst()`: リストソート

## 2024年版の特徴

### マグニチュード計算の違い
- **count_2024.py**: 送信先IP数をそのまま使用（IP数ベース）
- **tshark-args_2024.py**: 対数変換を適用（マグニチュードベース）
  - `magnitude = 10 * log(src_addr_count) / log(A_tot)`

### ファイル形式の違い
- 出力ファイル名: `YYYY-MM-DD.csv` (2024) vs `count-{where}-YYYY-MM-DD.csv` (2025)
- カラム名: `dnsmagnitude` (2024) vs `count` (2025)

## ログ機能

すべてのスクリプトで統一されたログ機能を使用：
- 実行時ログ: `output-2024/logs/dns_analysis_2024.log`

## 出力ファイル構造

```
/home/shimada/code/refactored/output-2024/
├── count/
│   └── YYYY-MM-DD.csv (IP数ベース)
├── dns_mag/
│   └── {where}-YYYY-MM-DD.csv (対数ベース)
├── ave-distr/
│   ├── average-{where}-YYYY-MM-DD.csv
│   └── distribution-{where}-YYYY-MM-DD.csv
├── graphs/
│   └── *.png
└── logs/
    └── dns_analysis_2024.log
```

## 2025年版との違い

### 機能の違い
- **2024年版**: IP数ベースの分析、グラフ作成機能が充実
- **2025年版**: qtype分析、より詳細なエラーハンドリング

### コード構造の違い
- **2024年版**: シンプルな構造、グラフィカル分析重視
- **2025年版**: より高度なDNS分析、エラー処理強化

## 旧バージョンとの互換性

- コマンドライン引数は基本的に同じ
- 出力形式も同じ（出力先のみ変更）
- 計算ロジックは元のまま保持

## 注意事項

- 入力ファイルのパスは従来通り（`/mnt/qnap2/shimada/`）
- 出力先のみ変更（`/home/shimada/code/refactored/output-2024/`）
- すべての共通処理は `func2024.py` 内で管理
- 2024年版特有のマグニチュード計算方式を維持
