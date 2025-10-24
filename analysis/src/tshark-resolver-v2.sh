#!/bin/bash

start_jst=$1
end_jst=$2

start_utc=$(date -u -d "$(echo $start_jst | sed -r 's/([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})/\1-\2-\3 \4:\5/') JST" +"%Y%m%d%H%M")
end_utc=$(date -u -d "$(echo $end_jst | sed -r 's/([0-9]{4})([0-9]{2})([0-9]{2})([0-9]{2})([0-9]{2})/\1-\2-\3 \4:\5/') JST" +"%Y%m%d%H%M")

original_files=$(ls -1 /mnt/qnap2/dnscap/dnscap/dump-*.gz | sort)

function convert_time_to_jst() {
    file_name="$1"
    time_part=$(echo "$file_name" | grep -oP '\d{12}')
    jst_time=$(TZ=Asia/Tokyo date --date "${time_part:0:4}-${time_part:4:2}-${time_part:6:2} ${time_part:8:2}:${time_part:10:2}:00 UTC" '+%Y%m%d%H%M')
    echo $jst_time
}

file_count=0

for file in $original_files
do
    file_time=$(basename "$file" | grep -oP '\d{12}')

    if [[ "$file_time" -ge "$start_utc" && "$file_time" -le "$end_utc" ]]; then
        dst_dir="/mnt/qnap2/shimada/resolver/"
        copied_file="$dst_dir$(basename "$file")"

        cp "$file" "$dst_dir"
        input_file="${copied_file%.gz}"
        gzip -d -k "$copied_file"

        jst_timestamp=$(convert_time_to_jst "$input_file")
        time_str="${jst_timestamp:0:4}-${jst_timestamp:4:2}-${jst_timestamp:6:2}-${jst_timestamp:8:2}"
        output_file="${dst_dir}${time_str}.csv"
        temp_file="${dst_dir}${time_str}.tmp.csv"

        echo "----- $input_file の処理開始 -----"
        
        # tsharkから正規表現フィルタを除去（ストリーミング処理を最適化）
        /usr/bin/time -v tshark \
            -r "$input_file" \
            -Y '(ip.src == 130.158.68.25 or ip.src == 130.158.68.26) and (dns.qry.name matches "\.tsukuba\.ac\.jp$") and (dns.flags.response == 1) and (dns.flags.authoritative == 0) and (dns.flags.rcode == 0)' \
            -T fields \
            -e frame.time -e ip.src -e ip.dst -e ipv6.dst -e dns.qry.name -e dns.qry.type \
            -E header=y -E separator=, -E quote=d \
            > "$temp_file"
        
        # grep側で正確にtsukuba.ac.jpで終わる行のみを抽出
        # ヘッダー行を保持し、dns.qry.nameフィールド(5番目)が.tsukuba.ac.jpで終わる行を抽出
        (head -n 1 "$temp_file" && tail -n +2 "$temp_file" | grep -E ',"[^"]*\.tsukuba\.ac\.jp",') > "$output_file"
        
        echo "----- $input_file の処理終了 -----"

        # 一時ファイルと元ファイルを削除
        rm -f "$temp_file"
        rm -f "$input_file"
        rm -f "$copied_file"

        echo "$output_file"
        
        ((file_count++))
        
        # 定期的なメモリ解放
        if (( file_count % 5 == 0 )); then
            echo "===== ${file_count}ファイル処理完了。メモリ解放待機中 ====="
            sleep 3
            sync
        fi
    fi
done

echo "===== 全処理完了 ====="