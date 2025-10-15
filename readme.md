# dnsmagnitude/src 概要と使い方

このファイルは `analysis/dnsmagnitude/src` 配下の各スクリプト/モジュールの機能説明と簡単な使い方をまとめたものです。

---

## 目次

- シェルスクリプト（tshark系）
  - `tshark-resovler-query-and-respons.sh`
  - `tshark-resolver-v2.sh`
  - `tshark-auth.sh`
  - `tshark-resolver.sh`
  - `tshark-resovler-query-and-respons.sh` (2025 配下)
  - `tshark-resolver.sh` (2025 配下)
- Python スクリプト（分析・可視化）
  - `view_network_results.py`
  - `make_boxplots.py`
  - `visual.py`
  - `network_analysis.py`
  - `count.py`
  - `magnitude-ave-distr.py`
  - `plot_stability.py` (※簡易参照)
  - `qtype_ratio.py` (※簡易参照)
  - `count.py` (上記)
  - `new-tshark-mag.py` (※簡易参照)
- 補助モジュール
  - `func.py`（主に `analysis/dnsmagnitude/src/func.py` と `src/2025/func.py` が存在）

---

## シェルスクリプト (tshark 関連)

目的: dnscapで保存した圧縮ダンプファイルを指定時間範囲で抽出し、tshark を用いて必要なフィールドを CSV へ変換する。主に tsukuba.ac.jp ドメインに関するフィルタを含む。

共通の使い方:
- 引数: 開始 JST 時刻 と 終了 JST 時刻 (形式: YYYYMMDDHHMM)
- 処理: 指定範囲の dump-*.gz をコピー、解凍、tshark によりフィルタをかけて CSV 出力
- 出力: スクリプト内の `dst_dir` 配下に `YYYY-MM-DD-HH.csv` 形式で出力

個別ファイル:

- `tshark-resovler-query-and-respons.sh`
  - tsukuba.ac.jp ドメインかつ非権威回答 (dns.flags.authoritative == 0) を対象
  - 出力フィールド: frame.time, ip.src, ip.dst, dns.qry.name, dns.qry.type, dns.flags.response, dns.flags.rcode, vlan.id
  - 出力先: `/mnt/qnap2/shimada/resolver-q-r/`

- `tshark-resolver-v2.sh`
  - リゾルバ向け。ストリーミング最適化のために一度全行を抽出し、その後 grep で `.tsukuba.ac.jp` にマッチする行のみを残す処理を行う
  - 出力先: `/mnt/qnap2/shimada/resolver/`
  - 実行中は一時ファイル (`*.tmp.csv`) を生成し、5 ファイルごとに sleep/sync を行う

- `tshark-auth.sh`
  - 権威サーバ向け。dns.flags.authoritative == 1 を対象
  - 出力先: `/mnt/qnap2/shimada/input/`

- `tshark-resolver.sh` (古い版)
  - resolver-v2 の前バージョンに相当（基本フィルタは同様）

- `2025` ディレクトリ内の tshark スクリプト
  - 年別の同等スクリプトが存在する。基本動作は上記と同等で、パスや細かいフィールドに差分がある場合あり。

注意点:
- 実行には `tshark` と `gzip`、および `/mnt/qnap2` の読み取り権限が必要。
- 出力 CSV のフィールド名や順序はスクリプトによって異なるため、後続の Python スクリプトが期待するカラムを必ず確認すること。

---

## Python スクリプト（分析・可視化）

全般: Python スクリプトは Pandas を用いて日次 CSV を読み込み、集計・統計・可視化を行います。多くのスクリプトは `func.py` のユーティリティ関数を利用します。

- `func.py` (主要補助モジュール)
  - 出力ディレクトリ管理: `ensure_output_dir(subdir)`
  - CSV 安全読み込み: `safe_read_csv`, `load_csv_with_error_handling`, `open_reader_safe` など
  - サブドメイン抽出: `extract_subdomain(domain_name)` — `.tsukuba.ac.jp` を基準に最上位サブドメインを抽出する。
  - DNS Magnitude 計算: `calculate_dns_magnitude(df, date_str)` — IP のユニーク数を用いて 10*log(|IpSet|)/log(A_tot) で算出
  - ネットワーク分類: `classify_ip_address(ip_str)` — 学内/学外/その他 に分類
  - クエリ/レスポンスのフィルタ: `filter_query_response_data(df, analysis_type)`
  - ネットワーク別マグニチュード: `classify_by_network_and_calculate_magnitude(df)` と `write_network_magnitude_csv`
  - qtype 集約などのユーティリティ関数も含む

- `count.py`
  - 旧来の集計スクリプト（`func.py` の関数を利用）
  - 入力: ローカルの `/mnt/qnap3/shimada-dnsmagnitude/logs/YYYY/MM/DD/` 配下の CSV
  - 処理: 全ファイルを結合、qtype 比率の計算、DNS Magnitude の計算と CSV 出力
  - 実行例: `python3 count.py 2025 04 01`

- `visual.py`
  - 日次 CSV（count と magnitude）から月次統計を作り、散布図、箱ひげ図、ヒートマップを出力
  - 主要関数:
    - `read_daily_counts_for_range(count_dir, where, start_date, end_date)`
    - `read_daily_magnitude_for_range(mag_dir, where, start_date, end_date)`
    - `summarize_monthly_counts`, `summarize_monthly_magnitude`
    - 可視化: `save_corr_and_scatter`, `save_boxplot_mag`, `save_heatmap_mag`
  - 実行例:
    python3 visual.py --count-dir /path/to/count_dir --mag-dir /path/to/mag_dir --start-date 2025-04-01 --end-date 2025-04-30 --out-dir ./figures

- `magnitude-ave-distr.py`
  - 指定期間またはパターンで複数の日の Magnitude を読み、サブドメインごとの平均・分散・標準偏差などを計算して出力
  - 強力なオプションとして日付範囲モードとパターンモードを提供
  - 実行例 (範囲): `python3 magnitude-ave-distr.py --mode range --start-date 2025-04-01 --end-date 2025-04-30 -w 1`

- `run_analysis_v2.py` (2025 ディレクトリ)
  - 新フォーマット (query/response 列を持つCSV) に対応した互換ラッパ
  - 分析タイプ: `count`, `qtype`, `qtype_total`
  - 実行例: `python3 run_analysis_v2.py count 2025 04 01 1 query`

- `view_network_results.py`
  - `output/network_analysis/` にある `magnitude-<network>-<analysis>-YYYY-MM-DD.csv` を読み、ネットワーク（internal/external/other）ごとの上位ドメインや統計を表示するツール
  - 実行例: `python3 view_network_results.py 2025-04-01 query 20`

- `make_boxplots.py`
  - 指定した月のドメイン毎の月平均・標準偏差の箱ひげ図を生成する
  - 入力: `--base-dir` に日次 CSV が格納されているディレクトリ（例: `/home/shimada/analysis/output`）
  - 出力: `month_boxplot_mean.png`, `month_boxplot_std.png`

- `query-count.py` (2025 配下)
  - 指定時間範囲内でサブドメインごとのクエリ数を集計し、パーセンテージ出力するツール
  - 実行例: `python3 query-count.py -y 2025 -m 04 -d 01 -w 1 --start-hour 0 --end-hour 23`

- `dnsmagnitude-time.py` (2025 配下)
  - 指定時間範囲のデータから DNS Magnitude を計測するスクリプト（`open_reader` + `extract_subdomain` を含む）

---

## 補助スクリプト / その他

- `plot_stability.py`, `qtype_ratio.py`, `new-tshark-mag.py` などは同ディレクトリに存在します（詳細はファイルヘッダを参照してください）。

---

## 実行上の注意

- 各スクリプトは期待される CSV カラム（例: `dns.qry.name`, `dnsmagnitude`, `ip`, `qname` 等）に依存しています。tshark の抽出結果と Python スクリプトで期待されるカラム名が一致していることを確認してください。
- 多くのスクリプトが `/mnt/qnap2` や `/mnt/qnap3` といったネットワークマウントを参照します。これらのパスが環境で利用可能か、読み取り権限があるか確認してください。
- 大量データを扱うため、メモリやディスク容量に注意してください。`tshark-resolver-v2.sh` のように一時ファイルを作るスクリプトでは一時ディスクの容量も意識してください。

---

## 追加でやると良いこと（提案）

- `func.py` にドキュメントストリングを追加し、関数の入出力を明確化する。
- スクリプト間でカラム名の期待値を定義する小さな互換レイヤ（例: `schema.py`）を作成し、tshark出力と分析コードを結合検証する。
- unit テスト（軽量）を `tests/` に追加し、データフォーマット変更時の回帰を検出しやすくする。

---

作成: 自動生成 by 作業スクリプト
