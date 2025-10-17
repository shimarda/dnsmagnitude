#!/bin/bash

# 元の圧縮ファイルのパスを取得
original_files=$(ls -1 /mnt/qnap2/dnscap/2023/dump-202311010*.gz | sort)

convert_time_to_jst() {
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
    # コピー先のディレクトリ
    # リゾルバ側
    #dst_dir="/mnt/qnap2/shimada/hakobe-pcap/"
    # 権威側
    dst_dir="/mnt/qnap2/shimada/input/"
    # コピーした圧縮ファイルのパス
    copied_file="$dst_dir$(basename "$file")"
    
    # ファイルをコピー
    cp "$file" "$dst_dir"

    # 解凍するためのファイルパス（解凍後のファイル名）
    input_file="${copied_file%.gz}"

    # 解凍
    gzip -d -k "$copied_file"

    # 元のファイル名からUTC時刻部分を抽出し、JSTに変換
    utc_timestamp=$(basename "$input_file" | sed 's/dump-//')
    jst_timestamp=$(convert_time_to_jst $utc_timestamp)
    time="${jst_timestamp:0:4}-${jst_timestamp:4:2}-${jst_timestamp:6:2}-${jst_timestamp:8:2}"

    # 出力ファイル名を生成
    output_file="${dst_dir}${time}.csv"

    # tsharkコマンドを実行し、結果をCSV形式で出力
    # リゾルバ側
    #tshark -r "$input_file" -Y "(ip.src == 130.158.68.25 or ip.src == 130.158.68.26) and (dns.qry.name matches tsukuba.ac.jp) and (dns.flags.response == 1)  and (dns.flags.rcode == 0)" -T fields -e frame.time -e ip.src -e ip.dst -e dns.qry.name -e dns.qry.type -E header=y -E separator=, -E quote=d > "$output_file"
    # 権威サーバの応答
   tshark -r "$input_file" -Y "(ip.src == 130.158.68.20 or ip.src == 130.158.68.21 or ip.src == 130.158.71.54) and (dns.flags.response == 1) and (dns.flags.authoritative == 1) and (dns.flags.rcode == 0)" -T fields -e frame.time -e ip.src -e ip.dst -e dns.qry.name -e dns.qry.type -E header=y -E separator=, -E quote=d > "$output_file"

    # 解凍ファイルを削除（不要な場合）
    rm "$input_file"

    # 出力ファイルのパスを表示
    echo "$output_file"
done