#!/bin/bash
# 監視機能のテストスクリプト

echo "=== DNS Magnitude 監視システムテスト ==="
echo ""

# 1. ディレクトリ確認
echo "【1. 監視ディレクトリの確認】"
WATCH_DIR="/mnt/qnap2/dnscap/dnscap"
if [ -d "$WATCH_DIR" ]; then
    echo "✓ ディレクトリ存在: $WATCH_DIR"
    echo "ファイル数: $(ls -1 $WATCH_DIR/dump-*.gz 2>/dev/null | wc -l)"
else
    echo "✗ ディレクトリが見つかりません: $WATCH_DIR"
fi

# 2. JST 23時相当のファイル確認（UTC 14時）
echo ""
echo "【2. JST 23時相当のファイル（UTC 14時）】"
ls -lh "$WATCH_DIR"/dump-*1400.gz 2>/dev/null | tail -5
FILE_COUNT=$(ls -1 "$WATCH_DIR"/dump-*1400.gz 2>/dev/null | wc -l)
echo "該当ファイル数: $FILE_COUNT"

# 3. 最新のJST 23時ファイル
echo ""
echo "【3. 最新のJST 23時相当ファイル】"
LATEST=$(ls -1t "$WATCH_DIR"/dump-*1400.gz 2>/dev/null | head -1)
if [ -n "$LATEST" ]; then
    echo "ファイル名: $(basename $LATEST)"
    
    # ファイル名から日時を抽出
    FILENAME=$(basename $LATEST)
    if [[ $FILENAME =~ dump-([0-9]{8})1400\.gz ]]; then
        UTC_DATE="${BASH_REMATCH[1]}"
        YEAR="${UTC_DATE:0:4}"
        MONTH="${UTC_DATE:4:2}"
        DAY="${UTC_DATE:6:2}"
        
        echo "UTC日時: $YEAR-$MONTH-$DAY 14:00"
        
        # JST変換
        JST_DATE=$(date -d "$YEAR-$MONTH-$DAY 14:00 UTC +9 hours" "+%Y-%m-%d %H:%M" 2>/dev/null)
        echo "JST日時: $JST_DATE"
    fi
else
    echo "該当ファイルなし"
fi

# 4. 既存の抽出済みCSV確認
echo ""
echo "【4. 既存の抽出済みCSV】"
echo "権威サーバー:"
ls -lht /mnt/qnap2/shimada/input/*.csv 2>/dev/null | head -3
echo ""
echo "リゾルバ:"
ls -lht /mnt/qnap2/shimada/resolver/*.csv 2>/dev/null | head -3

# 5. 出力ディレクトリ確認
echo ""
echo "【5. Magnitude出力ディレクトリ】"
ls -lht /home/shimada/analysis/output/*.csv 2>/dev/null | head -5

echo ""
echo "=== テスト完了 ==="