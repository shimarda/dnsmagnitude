# 新CSVフォーマット対応: DNS分析ツール

## 概要
新しいCSVカラム構成に対応したDNS分析ツールです。
```
frame.time,ip.src,ip.dst,dns.qry.name,dns.qry.type,dns.flags.response,dns.flags.rcode,vlan.id
```

## 📊 **パケット処理ルール**

### 🔍 **自動フィルタリング**
- **問い合わせパケット** (`dns.flags.response=0`): **すべて処理**
- **応答パケット** (`dns.flags.response=1`): **rcode=0のもののみ処理**

### 🎯 **分析対象**
| パケットタイプ | 条件 | 目的 |
|---------------|------|------|
| query | `dns.flags.response=0` | 全ての問い合わせを分析 |
| response | `dns.flags.response=1` AND `dns.flags.rcode=0` | 成功応答のみ分析 |

## 🛠️ **利用可能なツール**

### 1. **高機能統合ツール** (推奨)
```bash
# 基本使用方法
python3 /home/shimada/code/refactored/resolver_analysis_v2.py 2025 04 01 query count

# 分析タイプ
python3 resolver_analysis_v2.py 2025 04 01 query count      # カウント分析
python3 resolver_analysis_v2.py 2025 04 01 response magnitude # マグニチュード分析
python3 resolver_analysis_v2.py 2025 04 01 query qtype       # Qtype分析
```

### 2. **従来スクリプト互換ツール**
```bash
# 2025フォルダの既存スクリプトを新フォーマットで実行
python3 /home/shimada/code/2025/run_analysis_v2.py count 2025 04 01 1 query
python3 /home/shimada/code/2025/run_analysis_v2.py qtype 2025 04 01 1 response
```

### 3. **ネットワーク分類分析** (最新機能)
```bash
# 学内外分類 + マグニチュード分析
python3 /home/shimada/code/refactored/network_analysis.py 2025 04 01 query
python3 /home/shimada/code/refactored/network_analysis.py 2025 04 01 response
```

## 📈 **分析結果の比較**

### **問い合わせ vs 応答の比較例**
```bash
# 同日の問い合わせと応答を比較
python3 resolver_analysis_v2.py 2025 04 01 query count
python3 resolver_analysis_v2.py 2025 04 01 response count

# Qtypeの違いを分析
python3 resolver_analysis_v2.py 2025 04 01 query qtype
python3 resolver_analysis_v2.py 2025 04 01 response qtype
```

## 📁 **出力ファイル構成**

### **新フォーマット対応出力**
```
/home/shimada/analysis/output-2025/
├── count_v2/
│   ├── count-query-2025-04-01.csv      # 問い合わせカウント
│   └── count-response-2025-04-01.csv   # 応答カウント（rcode=0のみ）
├── magnitude_v2/
│   ├── magnitude-query-2025-04-01.csv  # 問い合わせマグニチュード
│   └── magnitude-response-2025-04-01.csv # 応答マグニチュード
├── qtype_v2/
│   ├── qtype-query-2025-04-01.csv      # 問い合わせQtype分析
│   └── qtype-response-2025-04-01.csv   # 応答Qtype分析
└── network_analysis/                    # ネットワーク分類結果
    ├── network-query-2025-04-01.csv
    └── network-response-2025-04-01.csv
```

### **CSVフォーマット例**

#### **カウント分析**
```csv
date,packet_type,domain,count
2025-04-01,query,example,1234
2025-04-01,response,example,1100
```

#### **マグニチュード分析**
```csv
date,packet_type,network_type,domain,magnitude
2025-04-01,query,internal,example,1000
2025-04-01,response,external,example,800
```

#### **Qtype分析**
```csv
date,packet_type,qtype,count,ratio
2025-04-01,query,1,50000,0.850000
2025-04-01,response,1,45000,0.900000
```

## 🚀 **実行例**

### **日次分析**
```bash
#!/bin/bash
# 1日分の完全分析

DATE="2025-04-01"
YEAR="2025"
MONTH="04"
DAY="01"

echo "=== $DATE の完全DNS分析 ==="

# 問い合わせパケット分析
echo "📤 問い合わせパケット分析"
python3 /home/shimada/code/refactored/resolver_analysis_v2.py $YEAR $MONTH $DAY query count
python3 /home/shimada/code/refactored/resolver_analysis_v2.py $YEAR $MONTH $DAY query qtype
python3 /home/shimada/code/refactored/network_analysis.py $YEAR $MONTH $DAY query

# 応答パケット分析
echo "📥 応答パケット分析（rcode=0のみ）"
python3 /home/shimada/code/refactored/resolver_analysis_v2.py $YEAR $MONTH $DAY response count
python3 /home/shimada/code/refactored/resolver_analysis_v2.py $YEAR $MONTH $DAY response qtype
python3 /home/shimada/code/refactored/network_analysis.py $YEAR $MONTH $DAY response

echo "分析完了: /home/shimada/analysis/output-2025/"
```

### **期間分析**
```bash
# 1週間の期間分析
for day in {01..07}; do
    python3 resolver_analysis_v2.py 2025 04 $day query count
    python3 resolver_analysis_v2.py 2025 04 $day response count
done
```

## 🔧 **技術仕様**

### **パケットフィルタリング詳細**
1. **CSV読み込み時**:
   - 全カラムを文字列として読み込み
   - NaN値は適切にデフォルト値で補完

2. **フィルタリング処理**:
   ```python
   # 問い合わせ
   df[df['dns.flags.response'] == '0']
   
   # 応答（成功のみ）
   response_df = df[df['dns.flags.response'] == '1']
   result_df = response_df[response_df['dns.flags.rcode'] == '0']
   ```

3. **エラー処理**:
   - 不正なパケットは自動除外
   - 処理ログで件数を確認可能

### **パフォーマンス最適化**
- **メモリ効率**: 大容量CSVファイルに対応
- **処理速度**: パンダスベクトル化処理
- **エラー耐性**: 不正データの自動スキップ

## 📊 **分析結果の活用**

### **比較分析**
```bash
# 問い合わせと応答の件数比較
python3 -c "
import pandas as pd
query_df = pd.read_csv('/home/shimada/analysis/output-2025/count_v2/count-query-2025-04-01.csv')
response_df = pd.read_csv('/home/shimada/analysis/output-2025/count_v2/count-response-2025-04-01.csv')

print('問い合わせ総数:', query_df['count'].sum())
print('応答総数:', response_df['count'].sum())
print('応答率:', response_df['count'].sum() / query_df['count'].sum())
"
```

### **統計情報生成**
```bash
# マグニチュード統計の生成
python3 /home/shimada/code/refactored/magnitude_statistics.py 2025-04-01 2025-04-07
```

## 🔍 **トラブルシューティング**

### **よくある問題**

1. **ファイルが見つからない**
   ```
   解決: パスを確認 /mnt/qnap2/shimada/resolver-q-r/
   ```

2. **パケット件数が0**
   ```
   解決: パケットタイプとフィルタ条件を確認
   ```

3. **メモリ不足**
   ```
   解決: 処理期間を短縮するか、サーバースペックを向上
   ```

### **ログ確認**
```bash
# 処理ログの確認
tail -f /home/shimada/analysis/output-2025/logs/dns_analysis.log
```

## 📚 **関連ドキュメント**

- [ネットワーク分類分析](README_NETWORK_ANALYSIS.md)
- [マグニチュード統計分析](README_MAGNITUDE_STATISTICS.md)
- [高速パイプライン処理](README_pipeline.md)
