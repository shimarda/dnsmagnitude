#!/bin/bash
# monitor_dumps_uv.sh - JST 23時対応版（権威+リゾルバ両方対応）

set -e

# ========== 設定 ==========
WATCH_DIR="/mnt/qnap2/dnscap/dnscap"
PROJECT_DIR="/home/shimada/dns-dashboard/analysis"
SCRIPT_DIR="/home/shimada/dns-dashboard/analysis/src"
OUTPUT_DIR="/home/shimada/analysis/output"
LOG_DIR="${PROJECT_DIR}/logs"
LOCK_FILE="/tmp/dns_processing.lock"

# tsharkスクリプトの場所
TSHARK_SCRIPT_DIR="${SCRIPT_DIR}"

# uv環境のPython
PYTHON_BIN="${PROJECT_DIR}/.venv/bin/python3"

# 処理済みファイルログ
PROCESSED_LOG="${LOG_DIR}/processed_files.log"
LOG_FILE="${LOG_DIR}/monitor_$(date +%Y%m%d).log"

# ========== 関数定義 ==========

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

check_uv_env() {
    if [[ ! -f "$PYTHON_BIN" ]]; then
        log "ERROR: uv仮想環境が見つかりません: $PYTHON_BIN"
        exit 1
    fi
    log "✓ uv環境確認: $PYTHON_BIN"
}

extract_datetime_from_filename() {
    local filename="$1"
    if [[ $filename =~ dump-([0-9]{12})\.gz ]]; then
        local timestamp="${BASH_REMATCH[1]}"
        local year="${timestamp:0:4}"
        local month="${timestamp:4:2}"
        local day="${timestamp:6:2}"
        local hour="${timestamp:8:2}"
        local minute="${timestamp:10:2}"
        
        echo "${year}-${month}-${day} ${hour}:${minute}"
        return 0
    fi
    return 1
}

is_jst_23_oclock_file() {
    local filename="$1"
    if [[ $filename =~ dump-[0-9]{8}1400\.gz ]]; then
        return 0
    fi
    return 1
}

is_processed() {
    local filepath="$1"
    if [[ -f "$PROCESSED_LOG" ]] && grep -q "^${filepath}$" "$PROCESSED_LOG"; then
        return 0
    fi
    return 1
}

mark_as_processed() {
    local filepath="$1"
    echo "$filepath" >> "$PROCESSED_LOG"
}

wait_for_file_stable() {
    local filepath="$1"
    local prev_size=0
    local curr_size
    local stable_count=0
    
    log "ファイル安定化待機: $(basename $filepath)"
    
    for i in {1..180}; do
        if [[ ! -f "$filepath" ]]; then
            log "ERROR: ファイルが存在しません: $filepath"
            return 1
        fi
        
        curr_size=$(stat -c%s "$filepath" 2>/dev/null || stat -f%z "$filepath" 2>/dev/null)
        
        if [[ "$curr_size" -eq "$prev_size" ]]; then
            ((stable_count++))
            if [[ $stable_count -ge 3 ]]; then
                log "✓ ファイル安定化完了: $(basename $filepath) (${curr_size} bytes)"
                return 0
            fi
        else
            stable_count=0
        fi
        
        prev_size=$curr_size
        sleep 10
    done
    
    log "WARNING: ファイル安定化タイムアウト"
    return 1
}

process_dump_file() {
    local dump_file="$1"
    local filename=$(basename "$dump_file")
    
    log "=========================================="
    log "処理開始: $filename"
    log "=========================================="
    
    if ! wait_for_file_stable "$dump_file"; then
        log "ERROR: ファイル安定化失敗。処理をスキップ"
        return 1
    fi
    
    local utc_datetime=$(extract_datetime_from_filename "$filename")
    if [[ -z "$utc_datetime" ]]; then
        log "ERROR: ファイル名から日時を抽出できません: $filename"
        return 1
    fi
    
    log "ファイル日時(UTC): $utc_datetime"
    
    local jst_datetime=$(date -d "$utc_datetime UTC +9 hours" "+%Y-%m-%d %H:%M" 2>/dev/null)
    
    if [[ -z "$jst_datetime" ]]; then
        log "ERROR: 日時変換に失敗しました"
        return 1
    fi
    
    local jst_date=$(echo "$jst_datetime" | cut -d' ' -f1)
    local year=$(echo "$jst_date" | cut -d'-' -f1)
    local month=$(echo "$jst_date" | cut -d'-' -f2)
    local day=$(echo "$jst_date" | cut -d'-' -f3)
    
    log "処理対象日(JST): $jst_date"
    log "ファイル時刻(JST): $jst_datetime"
    
    if [[ -f "$LOCK_FILE" ]]; then
        log "WARNING: 別の処理が実行中です。スキップ"
        return 1
    fi
    
    touch "$LOCK_FILE"
    trap "rm -f $LOCK_FILE; log 'ERROR: 処理中断'" EXIT
    
    # Step 1: tshark抽出（権威サーバーとリゾルバの両方）
    log "Step 1: tshark抽出確認"
    
    local start_jst="${year}${month}${day}0000"
    local end_jst="${year}${month}${day}2359"
    
    log "tshark抽出範囲(JST): ${start_jst} - ${end_jst}"
    
    # 1-1. 権威サーバー用tshark抽出
    log "Step 1-1: 権威サーバー用tshark抽出"
    
    if [[ -f "${TSHARK_SCRIPT_DIR}/tshark-auth.sh" ]]; then
        cd "${TSHARK_SCRIPT_DIR}"
        
        local auth_sample_file="/mnt/qnap2/shimada/input/${year}-${month}-${day}-00.csv"
        if [[ -f "$auth_sample_file" ]]; then
            log "✓ 権威サーバーCSVファイルは既に存在します。tshark抽出をスキップ"
        else
            log "権威サーバー用tshark抽出を実行中..."
            if bash tshark-auth.sh "$start_jst" "$end_jst" >> "$LOG_FILE" 2>&1; then
                log "✓ 権威サーバー用tshark抽出完了"
            else
                log "WARNING: 権威サーバー用tshark抽出でエラー（処理続行）"
            fi
        fi
    else
        log "WARNING: tshark-auth.sh が見つかりません: ${TSHARK_SCRIPT_DIR}/tshark-auth.sh"
    fi
    
    # 1-2. リゾルバ用tshark抽出
    log "Step 1-2: リゾルバ用tshark抽出"
    
    if [[ -f "${TSHARK_SCRIPT_DIR}/tshark-resolver-v2.sh" ]]; then
        cd "${TSHARK_SCRIPT_DIR}"
        
        local resolver_sample_file="/mnt/qnap2/shimada/resolver/${year}-${month}-${day}-00.csv"
        if [[ -f "$resolver_sample_file" ]]; then
            log "✓ リゾルバCSVファイルは既に存在します。tshark抽出をスキップ"
        else
            log "リゾルバ用tshark抽出を実行中..."
            if bash tshark-resolver-v2.sh "$start_jst" "$end_jst" >> "$LOG_FILE" 2>&1; then
                log "✓ リゾルバ用tshark抽出完了"
            else
                log "WARNING: リゾルバ用tshark抽出でエラー（処理続行）"
            fi
        fi
    else
        log "WARNING: tshark-resolver-v2.sh が見つかりません: ${TSHARK_SCRIPT_DIR}/tshark-resolver-v2.sh"
    fi
    
    # Step 2: DNS Magnitude計算（権威サーバー）
    log "Step 2: DNS Magnitude計算（権威サーバー）"
    
    cd "$PROJECT_DIR"
    
    local auth_result="${OUTPUT_DIR}/0-${jst_date}.csv"
    
    if [[ -f "$auth_result" ]]; then
        log "既存ファイルをバックアップ: $auth_result"
        cp "$auth_result" "${auth_result}.backup_$(date +%Y%m%d_%H%M%S)"
    fi
    
    if [[ -f "${SCRIPT_DIR}/new-tshark-mag.py" ]]; then
        if "$PYTHON_BIN" "${SCRIPT_DIR}/new-tshark-mag.py" \
            -y "$year" -m "$month" -d "$day" -w 0 \
            -o "${LOG_DIR}/error_auth_${year}${month}${day}.txt" >> "$LOG_FILE" 2>&1; then
            log "✓ 権威サーバーMagnitude計算完了"
        else
            log "WARNING: 権威サーバーMagnitude計算でエラー"
        fi
    else
        log "ERROR: new-tshark-mag.py が見つかりません: ${SCRIPT_DIR}/new-tshark-mag.py"
    fi
    
    # Step 3: DNS Magnitude計算（リゾルバ）
    log "Step 3: DNS Magnitude計算（リゾルバ）"
    
    local resolver_result="${OUTPUT_DIR}/1-${jst_date}.csv"
    
    if [[ -f "$resolver_result" ]]; then
        log "既存ファイルをバックアップ: $resolver_result"
        cp "$resolver_result" "${resolver_result}.backup_$(date +%Y%m%d_%H%M%S)"
    fi
    
    if [[ -f "${SCRIPT_DIR}/new-tshark-mag.py" ]]; then
        if "$PYTHON_BIN" "${SCRIPT_DIR}/new-tshark-mag.py" \
            -y "$year" -m "$month" -d "$day" -w 1 \
            -o "${LOG_DIR}/error_resolver_${year}${month}${day}.txt" >> "$LOG_FILE" 2>&1; then
            log "✓ リゾルバMagnitude計算完了"
        else
            log "WARNING: リゾルバMagnitude計算でエラー"
        fi
    else
        log "ERROR: new-tshark-mag.py が見つかりません: ${SCRIPT_DIR}/new-tshark-mag.py"
    fi
    
    # Step 4: 結果確認
    if [[ -f "$auth_result" ]]; then
        local lines=$(wc -l < "$auth_result")
        log "✓ 権威サーバー結果: $lines 行"
    else
        log "WARNING: 権威サーバー結果ファイルが生成されませんでした"
    fi
    
    if [[ -f "$resolver_result" ]]; then
        local lines=$(wc -l < "$resolver_result")
        log "✓ リゾルバ結果: $lines 行"
    else
        log "WARNING: リゾルバ結果ファイルが生成されませんでした"
    fi
    
    # Step 5: サマリー生成（オプション）
    if [[ -f "${SCRIPT_DIR}/generate_summary.py" ]]; then
        log "Step 5: 統計サマリー生成"
        "$PYTHON_BIN" "${SCRIPT_DIR}/generate_summary.py" "$jst_date" >> "$LOG_FILE" 2>&1 || true
    fi
    
    mark_as_processed "$dump_file"
    
    rm -f "$LOCK_FILE"
    trap - EXIT
    
    log "=========================================="
    log "処理完了: $filename → JST $jst_date"
    log "=========================================="
    
    return 0
}

# ========== メイン処理 ==========

mkdir -p "$LOG_DIR"
mkdir -p "$OUTPUT_DIR"
touch "$PROCESSED_LOG"

check_uv_env

log "============================================"
log "パケットダンプ監視開始"
log "監視ディレクトリ: $WATCH_DIR"
log "監視パターン: dump-*1400.gz (JST 23時相当)"
log "スクリプトディレクトリ: $TSHARK_SCRIPT_DIR"
log "Python: $PYTHON_BIN"
log "============================================"

log "既存ファイルチェック中..."
for file in "$WATCH_DIR"/dump-*1400.gz; do
    if [[ -f "$file" ]] && ! is_processed "$file"; then
        log "未処理ファイル検出: $(basename "$file")"
        process_dump_file "$file"
    fi
done

log "inotifywaitによるリアルタイム監視を開始..."

inotifywait -m -e close_write,moved_to --format '%w%f' "$WATCH_DIR" 2>/dev/null | while read filepath; do
    filename=$(basename "$filepath")
    
    if is_jst_23_oclock_file "$filename"; then
        log "新規ファイル検出(JST 23時相当): $filename"
        
        if is_processed "$filepath"; then
            log "既に処理済み: $filename"
            continue
        fi
        
        process_dump_file "$filepath"
    fi
done