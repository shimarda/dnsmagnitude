# DNSログ解析ツール（リファクタリング版）

2025年版のDNSログ解析ツールをリファクタリングし、コードの管理しやすさを向上させました。

## 主な変更点

### 1. 出力先の統一
- すべての出力は `/home/shimada/code/refactored/output/` 配下に保存されます
- サブディレクトリ別に結果が整理されます：
  - `count/`: ドメインカウント結果
  - `qtype/`: qtype分析結果  
  - `magnitude/`: DNSマグニチュード結果
  - `ave-distr/`: 平均値・分散結果
  - `logs/`: ログファイル

### 2. 共通処理の関数化
- すべての共通処理を `func.py` に集約
- 各スクリプトは `func.py` の関数を呼び出すだけのシンプルな構造
- エラーハンドリングと logging の統一

### 3. コードの機能別分離
- `count.py`: ドメインカウント処理
- `qtype_ratio.py`: qtype比率分析（日別）
- `type_analysis.py`: qtype全体分析
- `type_per_domain.py`: ドメイン別qtype平均計算
- `count-ave-distr.py`: 平均値・分散計算
- `new-tshark-mag.py`: DNSマグニチュード計算

## 使用方法

### 環境設定
出力ディレクトリは自動的に作成されます。

### 基本的な使用例

#### 1. ドメインカウント分析 OK
```bash
python count.py -y 2025 -m 04 -d 01 -w 0
```

#### 2. qtype比率分析（日別）OK
```bash
python qtype_ratio.py -y 2025 -m 04 -d 01 -w 0
```

#### 3. qtype全体分析（単一パターン）OK
```bash
python type_analysis.py -y 2025 -m 04 -d 15 -w 0
```

#### 4. qtype全体分析（日付範囲）OK
```bash
python type_analysis.py --mode range --start-date 2025-04-01 --end-date 2025-04-30 -w 0
```

#### 5. ドメイン別qtype平均計算（日付範囲）
```bash
python type_per_domain.py --mode range --start-date 2025-04-01 --end-date 2025-04-30 -w 0
```

#### 6. ドメイン別qtype平均計算（パターン）
```bash
python type_per_domain.py --mode pattern -y 2025 -m 04 -d '*' -w 0
```

#### 7. 平均値・分散計算
```bash
python count-ave-distr.py -y 2025 -m 04 -d 01 -w 0
```

#### 8. DNSマグニチュード計算 OK
```bash
python new-tshark-mag.py -y 2025 -m 04 -d 01 -w 0
```

### パラメータ説明
- `-y`: 年
- `-m`: 月 
- `-d`: 日
- `-w`: データソース（0=権威DNS, 1=リゾルバー）
- `--start-date`: 開始日（YYYY-MM-DD形式）
- `--end-date`: 終了日（YYYY-MM-DD形式）
- `--mode`: 処理モード（range=日付範囲, pattern=パターンマッチング）

## func.py の主要な関数

### ファイル操作
- `file_lst()`: パターンに一致するファイルリスト取得
- `file_time()`, `file_time_dict()`: ファイル名から時間情報抽出
- `find_output_files()`: 出力ファイルの検索

### データ処理
- `extract_subdomain()`: サブドメイン抽出
- `open_reader_safe()`: 安全なファイル読み込み
- `detect_problematic_rows()`: 問題行の検出

### カウント処理
- `count_query()`, `safe_count_query()`: クエリカウント

### 出力処理
- `write_csv()`, `safe_write_csv()`: CSV書き込み
- `write_qtype_csv()`: qtype分析結果出力
- `write_magnitude_csv()`: マグニチュード結果出力

### qtype分析
- `qtype_ratio()`: 日別qtype比率分析
- `qtype_ratio_total()`: 全期間qtype比率分析
- `process_qtype_hourly_data()`: 時間別qtype処理

### 統計計算
- `calculate_average_distribution()`: 平均値・分散計算
- `calculate_qtype_average_ratios()`: qtype平均比率計算
- `calculate_magnitude()`: マグニチュード計算

### ユーティリティ
- `setup_logging()`: ログ設定
- `ensure_output_dir()`: 出力ディレクトリ作成

## ログ機能

すべてのスクリプトで統一されたログ機能を使用：
- 実行時ログ: `output/logs/[script_name].log`
- 問題行ログ: `output/logs/problematic_rows_*.csv`
- エラーログ: `output/logs/error_log.txt`

## 出力ファイル構造

```
/home/shimada/code/refactored/output/
├── count/
│   └── count-{where}-{year}-{month}-{day}.csv
├── qtype/
│   ├── qtype-{where}-{date}.csv
│   ├── qtype-total-{where}-{period}.csv
│   └── qtype-average-{where}-{period}.csv
├── magnitude/
│   └── magnitude-{where}-{year}-{month}-{day}.csv
├── ave-distr/
│   ├── average-{where}-{year}-{month}-{day}.csv
│   └── distribution-{where}-{year}-{month}-{day}.csv
└── logs/
    ├── dns_analysis.log
    ├── problematic_rows_*.csv
    └── error_log.txt
```

## 旧バージョンとの互換性

- コマンドライン引数は基本的に同じ
- 出力形式も同じ（出力先のみ変更）
- 新機能（日付範囲指定など）を追加

## 注意事項

- 入力ファイルのパスは従来通り（`/mnt/qnap2/shimada/`）
- 出力先のみ変更（`/home/shimada/code/refactored/output/`）
- すべての共通処理は `func.py` 内で管理
- エラーハンドリングを強化
