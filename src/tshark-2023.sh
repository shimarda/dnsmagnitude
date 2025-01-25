#!/bin/bash

# JSTの時間範囲を指定
start_jst=$1 # 開始時間
end_jst=$2   # 終了時間

# JSTからUTCに変換
start_utc=$(date -u -d "$(echo $start_jst | sed -r 's/([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})/\1-\2-\3 \4:\5/') JST" +"%Y%m%d%H%M")
end_utc=$(date -u -d "$(echo $end_jst | sed -r 's/([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})/\1-\2-\3 \4:\5/') JST" +"%Y%m%d%H%M")



# 元の圧縮ファイルのパスを取得
original_files=$(ls -1 /mnt/qnap2/dnscap/2023/dump-*.gz | sort)

function convert_time_to_jst() {
    # 引数からファイル名を取得
    file_name="$1"
 
    # ファイル名から時刻部分を取得
    time_part=$(echo "$file_name" | grep -oP '\d{12}')
   
    # UTCからJSTに変換
    jst_time=$(TZ=Asia/Tokyo date --date "${time_part:0:4}-${time_part:4:2}-${time_part:6:2} ${time_part:8:2}:${time_part:10:2}:00 UTC" '+%Y%m%d%H%M')
    echo $jst_time
}

# 各ファイルに対して処理を実行
for file in $original_files
do
    # ファイル名からUTC時刻部分を抽出
    file_time=$(basename "$file" | grep -oP '\d{12}')

    # ファイルの時刻が指定範囲内か確認
    if [[ "$file_time" -ge "$start_utc" && "$file_time" -le "$end_utc" ]]; then
        # コピー先のディレクトリ
        dst_dir="/mnt/qnap2/shimada/input/"
        copied_file="$dst_dir$(basename "$file")"
        
        # ファイルをコピー
        cp "$file" "$dst_dir"

        # 解凍するためのファイルパス（解凍後のファイル名）
        input_file="${copied_file%.gz}"

        # 解凍
        gzip -d -k "$copied_file"

        # 元のファイル名からUTC時刻部分を抽出し、JSTに変換
        jst_timestamp=$(convert_time_to_jst "$input_file")
        time="${jst_timestamp:0:4}-${jst_timestamp:4:2}-${jst_timestamp:6:2}-${jst_timestamp:8:2}"

        # 出力ファイル名を生成
        output_file="${dst_dir}${time}.csv"

        # tsharkコマンドを実行し、結果をCSV形式で出力
        tshark -r "$input_file" -Y "(ip.src == 130.158.68.20 or ip.src == 130.158.68.21 or ip.src == 130.158.71.54) and (dns.flags.authoritative == 1) and (dns.flags.rcode == 0)" -T fields -e frame.time -e ip.src -e ip.dst -e ipv6.dst -e dns.qry.name -e dns.qry.type -E header=y -E separator=, -E quote=d > "$output_file"

        # 解凍ファイルを削除（不要な場合）
        rm "$input_file"
        rm "$copied_file"

        # 出力ファイルのパスを表示
        echo "$output_file"
    fi
done
