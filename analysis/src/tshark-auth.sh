#!/bin/bash

# JSTの時間範囲を指定
start_jst=$1 # 開始時間(YYYYMMDDHHMM)
end_jst=$2   # 終了時間(YYYYMMDDHHMM)

# メモリ使用量の閾値設定
MEMORY_THRESHOLD_MB=10000  # 10GB以上の空きメモリが必要
CHUNK_DURATION=300         # チャンク分割時間（秒）: 5分単位
USE_CHUNKING_THRESHOLD_MB=500  # このサイズ以上でチャンク分割

# JSTからUTCに変換（JST = UTC + 9時間なので、UTC = JST - 9時間）
start_utc=$(TZ=UTC date -d "TZ=\"Asia/Tokyo\" $(echo $start_jst | sed -r 's/([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})/\1-\2-\3 \4:\5/')" +"%Y%m%d%H%M")
end_utc=$(TZ=UTC date -d "TZ=\"Asia/Tokyo\" $(echo $end_jst | sed -r 's/([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})/\1-\2-\3 \4:\5/')" +"%Y%m%d%H%M")

# 元の圧縮ファイルのパスを取得
original_files=$(ls -1 /mnt/qnap2/dnscap/dnscap/dump-*.gz | sort)

function convert_time_to_jst() {
    # 引数からファイル名を取得
    file_name="$1"
    # ファイル名から時刻部分を取得
    time_part=$(echo "$file_name" | grep -oP '\d{12}')
    # UTCからJSTに変換
    jst_time=$(TZ=Asia/Tokyo date --date "${time_part:0:4}-${time_part:4:2}-${time_part:6:2} ${time_part:8:2}:${time_part:10:2}:00 UTC" '+%Y%m%d%H%M')
    echo $jst_time
}

function check_memory() {
    free -m | awk '/^Mem:/ {print $7}'
}

function wait_for_memory() {
    while true; do
        available=$(check_memory)
        if [ $available -gt $MEMORY_THRESHOLD_MB ]; then
            break
        fi
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] メモリ不足 (空き: ${available}MB)。30秒待機..."
        sleep 30
        sync
        echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true
    done
}

function get_file_size_mb() {
    local file="$1"
    stat -c%s "$file" | awk '{print int($1/1024/1024)}'
}

function process_with_chunking() {
    local input_file="$1"
    local output_file="$2"
    local temp_dir="${output_file%.csv}_chunks"
    
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] チャンク分割処理開始: $input_file"
    echo "空きメモリ: $(check_memory)MB"
    
    mkdir -p "$temp_dir"
    
    # editcapで時間単位に分割
    echo "ファイルを${CHUNK_DURATION}秒単位に分割中..."
    editcap -i $CHUNK_DURATION "$input_file" "${temp_dir}/chunk_" 2>&1
    
    if [ $? -ne 0 ]; then
        echo "エラー: ファイル分割に失敗しました"
        rm -rf "$temp_dir"
        return 1
    fi
    
    # 最初のチャンクでヘッダー付きで処理
    local first_chunk=true
    local chunk_count=0
    
    for chunk in "${temp_dir}"/chunk_*; do
        if [ ! -f "$chunk" ]; then
            continue
        fi
        
        ((chunk_count++))
        echo "  チャンク $chunk_count 処理中... ($(basename $chunk))"
        
        wait_for_memory
        
        if [ "$first_chunk" = true ]; then
            # 最初のチャンクのみヘッダー付き
            tshark \
                -r "$chunk" \
                -Y "(ip.src == 130.158.68.20 or ip.src == 130.158.68.21 or ip.src == 130.158.71.54) and (dns.flags.authoritative == 1) and (dns.flags.rcode == 0)" \
                -T fields \
                -e frame.time -e ip.src -e ip.dst -e ipv6.dst -e dns.qry.name -e dns.qry.type \
                -E header=y -E separator=, -E quote=d \
                > "$output_file" 2>&1
            first_chunk=false
        else
            # 2番目以降はヘッダーなしで追記
            tshark \
                -r "$chunk" \
                -Y "(ip.src == 130.158.68.20 or ip.src == 130.158.68.21 or ip.src == 130.158.71.54) and (dns.flags.authoritative == 1) and (dns.flags.rcode == 0)" \
                -T fields \
                -e frame.time -e ip.src -e ip.dst -e ipv6.dst -e dns.qry.name -e dns.qry.type \
                -E header=n -E separator=, -E quote=d \
                >> "$output_file" 2>&1
        fi
        
        # チャンク処理後すぐに削除
        rm -f "$chunk"
        
        # 小まめにメモリ解放
        if (( chunk_count % 3 == 0 )); then
            sync
            echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true
        fi
    done
    
    rm -rf "$temp_dir"
    
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] チャンク処理完了 (チャンク数: $chunk_count)"
    return 0
}

file_count=0

# 各ファイルに対して処理を実行
for file in $original_files
do
    # ファイル名からUTC時刻部分を抽出
    file_time=$(basename "$file" | grep -oP '\d{12}')

    # ファイルの時刻が指定範囲内か確認
    if [[ "$file_time" -ge "$start_utc" && "$file_time" -le "$end_utc" ]]; then
        wait_for_memory
        
        # コピー先のディレクトリ
        dst_dir="/mnt/qnap2/shimada/input/"
        copied_file="$dst_dir$(basename "$file")"

        # ファイルをコピー
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ファイルコピー: $(basename "$file")"
        cp "$file" "$dst_dir"

        # 解凍するためのファイルパス（解凍後のファイル名）
        input_file="${copied_file%.gz}"

        # 解凍
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] 解凍中..."
        gzip -d -k "$copied_file"
        
        file_size_mb=$(get_file_size_mb "$input_file")
        echo "解凍後のファイルサイズ: ${file_size_mb}MB"

        # 元のファイル名からUTC時刻部分を抽出し、JSTに変換
        jst_timestamp=$(convert_time_to_jst "$input_file")
        time_str="${jst_timestamp:0:4}-${jst_timestamp:4:2}-${jst_timestamp:6:2}-${jst_timestamp:8:2}"

        # 出力ファイル名を生成
        output_file="${dst_dir}${time_str}.csv"

        # ---- timeコマンドでtsharkの実行時間を計測 ----
        echo "----- $input_file の処理開始 -----"
        
        # ファイルサイズに応じて処理方法を選択
        if [ $file_size_mb -gt $USE_CHUNKING_THRESHOLD_MB ]; then
            echo "大容量ファイル(${file_size_mb}MB)検出。チャンク分割処理を実行します。"
            process_with_chunking "$input_file" "$output_file"
        else
            echo "通常サイズファイル(${file_size_mb}MB)。標準処理を実行します。"
            /usr/bin/time -v tshark \
                -r "$input_file" \
                -Y "(ip.src == 130.158.68.20 or ip.src == 130.158.68.21 or ip.src == 130.158.71.54) and (dns.flags.authoritative == 1) and (dns.flags.rcode == 0)" \
                -T fields \
                -e frame.time -e ip.src -e ip.dst -e ipv6.dst -e dns.qry.name -e dns.qry.type \
                -E header=y -E separator=, -E quote=d \
                > "$output_file" 2>&1
        fi
        
        echo "----- $input_file の処理終了 -----"
        
        # 解凍ファイルを削除（不要な場合）
        rm -f "$input_file"
        rm -f "$copied_file"
        # 出力ファイルのパスを表示
        echo "$output_file"

        ((file_count++))
        
        # 定期的なメモリ解放
        if (( file_count % 2 == 0 )); then
            echo "[$(date '+%Y-%m-%d %H:%M:%S')] ${file_count}ファイル処理完了。メモリ解放中..."
            sync
            echo 3 > /proc/sys/vm/drop_caches 2>/dev/null || true
            sleep 5
            echo "現在の空きメモリ: $(check_memory)MB"
        fi
    fi
done

echo "[$(date '+%Y-%m-%d %H:%M:%S')] ===== 全処理完了 ====="
echo "処理ファイル数: $file_count"
echo "最終空きメモリ: $(check_memory)MB"