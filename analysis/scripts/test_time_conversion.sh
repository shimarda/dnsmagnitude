#!/bin/bash
# 時刻変換テストスクリプト

echo "=== JST → UTC 変換テスト ==="
echo ""

# テストケース1: 2025-04-01 00:00 JST → 2025-03-31 15:00 UTC
test_jst="202504010000"
echo "入力 (JST): $test_jst"
expected_utc="202503311500"
start_utc=$(TZ=UTC date -d "TZ=\"Asia/Tokyo\" $(echo $test_jst | sed -r 's/([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})/\1-\2-\3 \4:\5/')" +"%Y%m%d%H%M")
echo "出力 (UTC): $start_utc"
echo "期待値:     $expected_utc"
if [[ "$start_utc" == "$expected_utc" ]]; then
    echo "✓ テスト1 成功"
else
    echo "✗ テスト1 失敗"
fi
echo ""

# テストケース2: 2025-04-01 23:59 JST → 2025-04-01 14:59 UTC
test_jst="202504012359"
echo "入力 (JST): $test_jst"
expected_utc="202504011459"
end_utc=$(TZ=UTC date -d "TZ=\"Asia/Tokyo\" $(echo $test_jst | sed -r 's/([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})/\1-\2-\3 \4:\5/')" +"%Y%m%d%H%M")
echo "出力 (UTC): $end_utc"
echo "期待値:     $expected_utc"
if [[ "$end_utc" == "$expected_utc" ]]; then
    echo "✓ テスト2 成功"
else
    echo "✗ テスト2 失敗"
fi
echo ""

# テストケース3: UTC → JST 変換（ダンプファイル名から）
echo "=== UTC → JST 変換テスト ==="
echo ""

test_utc_time="202504011400"
echo "入力 (UTC): $test_utc_time"
expected_jst="202504012300"
jst_time=$(TZ=Asia/Tokyo date --date "${test_utc_time:0:4}-${test_utc_time:4:2}-${test_utc_time:6:2} ${test_utc_time:8:2}:${test_utc_time:10:2}:00 UTC" '+%Y%m%d%H%M')
echo "出力 (JST): $jst_time"
echo "期待値:     $expected_jst"
if [[ "$jst_time" == "$expected_jst" ]]; then
    echo "✓ テスト3 成功"
else
    echo "✗ テスト3 失敗"
fi
echo ""

echo "=== 実際の処理フロー例 ==="
echo ""
echo "1. ダンプファイル: dump-202504011400.gz (UTC 14:00)"
echo "2. JSTに変換: 2025-04-01 23:00 JST"
echo "3. 出力ファイル: 2025-04-01-23.csv"
echo ""
echo "4. 監視スクリプトが JST 2025-04-01 の処理を開始"
echo "5. tshark抽出範囲(JST): 202504010000 - 202504012359"
echo "6. UTC変換後の範囲: 202503311500 - 202504011459"
echo "7. 該当ダンプファイル: dump-202503311500.gz ~ dump-202504011459.gz"
echo ""
