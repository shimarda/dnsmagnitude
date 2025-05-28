#!/bin/bash

# ── 1. 定数／関数定義 ───────────────────────────
WATCH_DIR="/mnt/qnap2/dnscap/dnscap"
DST_DIR="/mnt/qnap2/shimada/input"
LOG_FILE="/home/shimada/analysis/processed.log"

# JSTタイム変換
convert_time_to_jst() {
  local fname="$1"
  local p=$(echo "$fname" | grep -oP '\d{12}')
  TZ=Asia/Tokyo date --date "${p:0:4}-${p:4:2}-${p:6:2} ${p:8:2}:${p:10:2}:00 UTC" '+%Y%m%d%H%M'
}

# .gz -> CSV 出力ロジック
process_one() {
  local fullgz="$1"
  local base=$(basename "$fullgz")

  echo "--- process_one: $base ---"
  cp "$fullgz" "$DST_DIR/" || { echo "copy failed"; return; }
  local plain="${DST_DIR}/${base%.gz}"
  gzip -d -k "$DST_DIR/$base"

  local jst=$(convert_time_to_jst "$plain")
  local timestr="${jst:0:4}-${jst:4:2}-${jst:6:2}-${jst:8:2}"
  local csv="${DST_DIR}/${timestr}.csv"

  /usr/bin/time -v tshark \
    -r "$plain" \
    -Y "(ip.src == 130.158.68.20 or ip.src == 130.158.68.21 or ip.src == 130.158.71.54) and (dns.flags.authoritative==1) and (dns.flags.rcode==0)" \
    -T fields \
    -e frame.time -e ip.src -e ip.dst -e ipv6.dst -e dns.qry.name -e dns.qry.type \
    -E header=y -E separator=, -E quote=d \
    > "$csv"

  echo "-> 出力完了: $csv"
  rm -f "$plain" "$DST_DIR/$base"
}

# ── 2. 初期化 ────────────────────────────────
mkdir -p "$DST_DIR"
touch "$LOG_FILE"   # 書き込み可能な場所を指定してください

# ── 3. 起動時一括処理 ─────────────────────────
for gz in "$WATCH_DIR"/dump-*.gz; do
  [[ -f "$gz" ]] || continue
  base=$(basename "$gz")
  if ! grep -qxF "$base" "$LOG_FILE"; then
    process_one "$gz"
    echo "$base" >> "$LOG_FILE"
  fi
done

# ── 4. inotifywait 監視 ───────────────────────
inotifywait -m -e create --format "%f" "$WATCH_DIR" | while read fname; do
  if [[ "$fname" =~ \.gz$ ]] && ! grep -qxF "$fname" "$LOG_FILE"; then
    process_one "$WATCH_DIR/$fname"
    echo "$fname" >> "$LOG_FILE"
  fi
done
