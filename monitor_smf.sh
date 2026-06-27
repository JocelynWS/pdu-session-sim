#!/usr/bin/env bash
set -euo pipefail

DURATION_SECONDS="${1:-900}"
INTERVAL_SECONDS="${2:-1}"
OUT_DIR="${3:-monitor_reports}"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
CSV_FILE="${OUT_DIR}/smf_resource_${TIMESTAMP}.csv"
SUMMARY_FILE="${OUT_DIR}/smf_resource_${TIMESTAMP}_summary.txt"

mkdir -p "$OUT_DIR"

find_smf_pid() {
	pgrep -f "go run cmd/smf/main.go" | head -n 1 ||
		pgrep -f "cmd/smf/main.go" | head -n 1 ||
		pgrep -x "smf" | head -n 1 ||
		pgrep -f "/smf($| )" | head -n 1
}

PID="$(find_smf_pid || true)"
if [[ -z "${PID}" ]]; then
	echo "ERROR: Không tìm thấy tiến trình SMF đang chạy."
	echo "Hãy chạy SMF trước, ví dụ: LOG_LEVEL=error go run cmd/smf/main.go"
	exit 1
fi

if ! kill -0 "$PID" 2>/dev/null; then
	echo "ERROR: PID $PID không còn tồn tại."
	exit 1
fi

SMF_CMD="$(ps -p "$PID" -o args=)"
echo "Monitoring SMF PID: $PID"
echo "Command: $SMF_CMD"
echo "Duration: ${DURATION_SECONDS}s, interval: ${INTERVAL_SECONDS}s"
echo "CSV: $CSV_FILE"
echo "Summary: $SUMMARY_FILE"
echo

echo "timestamp,pid,cpu_percent,mem_percent,rss_mb,vsz_mb" > "$CSV_FILE"

samples=0
cpu_sum="0"
mem_sum="0"
rss_sum="0"
vsz_sum="0"
cpu_peak="0"
mem_peak="0"
rss_peak="0"
vsz_peak="0"

end_time=$(( $(date +%s) + DURATION_SECONDS ))

while [[ "$(date +%s)" -lt "$end_time" ]]; do
	if ! kill -0 "$PID" 2>/dev/null; then
		echo "WARN: SMF PID $PID đã dừng trước khi monitor kết thúc."
		break
	fi

	read -r cpu mem rss_kb vsz_kb < <(ps -p "$PID" -o %cpu=,%mem=,rss=,vsz=)
	now="$(date '+%Y-%m-%d %H:%M:%S')"
	rss_mb="$(awk -v kb="$rss_kb" 'BEGIN { printf "%.2f", kb / 1024 }')"
	vsz_mb="$(awk -v kb="$vsz_kb" 'BEGIN { printf "%.2f", kb / 1024 }')"

	echo "$now,$PID,$cpu,$mem,$rss_mb,$vsz_mb" >> "$CSV_FILE"

	cpu_sum="$(awk -v a="$cpu_sum" -v b="$cpu" 'BEGIN { printf "%.4f", a + b }')"
	mem_sum="$(awk -v a="$mem_sum" -v b="$mem" 'BEGIN { printf "%.4f", a + b }')"
	rss_sum="$(awk -v a="$rss_sum" -v b="$rss_mb" 'BEGIN { printf "%.4f", a + b }')"
	vsz_sum="$(awk -v a="$vsz_sum" -v b="$vsz_mb" 'BEGIN { printf "%.4f", a + b }')"

	cpu_peak="$(awk -v a="$cpu_peak" -v b="$cpu" 'BEGIN { print (b > a ? b : a) }')"
	mem_peak="$(awk -v a="$mem_peak" -v b="$mem" 'BEGIN { print (b > a ? b : a) }')"
	rss_peak="$(awk -v a="$rss_peak" -v b="$rss_mb" 'BEGIN { print (b > a ? b : a) }')"
	vsz_peak="$(awk -v a="$vsz_peak" -v b="$vsz_mb" 'BEGIN { print (b > a ? b : a) }')"

	samples=$((samples + 1))
	printf "\rSamples: %d | CPU avg/peak: " "$samples"
	awk -v sum="$cpu_sum" -v n="$samples" -v peak="$cpu_peak" 'BEGIN { printf "%.2f%% / %.2f%%", sum / n, peak }'
	printf " | RSS avg/peak: "
	awk -v sum="$rss_sum" -v n="$samples" -v peak="$rss_peak" 'BEGIN { printf "%.2f MB / %.2f MB", sum / n, peak }'

	sleep "$INTERVAL_SECONDS"
done

echo

if [[ "$samples" -eq 0 ]]; then
	echo "ERROR: Không thu được sample nào."
	exit 1
fi

cpu_avg="$(awk -v sum="$cpu_sum" -v n="$samples" 'BEGIN { printf "%.2f", sum / n }')"
mem_avg="$(awk -v sum="$mem_sum" -v n="$samples" 'BEGIN { printf "%.2f", sum / n }')"
rss_avg="$(awk -v sum="$rss_sum" -v n="$samples" 'BEGIN { printf "%.2f", sum / n }')"
vsz_avg="$(awk -v sum="$vsz_sum" -v n="$samples" 'BEGIN { printf "%.2f", sum / n }')"

{
	echo "SMF Resource Usage Report"
	echo "Generated at: $(date '+%Y-%m-%d %H:%M:%S')"
	echo "PID: $PID"
	echo "Command: $SMF_CMD"
	echo "Samples: $samples"
	echo "Interval seconds: $INTERVAL_SECONDS"
	echo
	echo "CPU Average (%): $cpu_avg"
	echo "CPU Peak (%): $cpu_peak"
	echo "Memory Average (%): $mem_avg"
	echo "Memory Peak (%): $mem_peak"
	echo "RSS Average (MB): $rss_avg"
	echo "RSS Peak (MB): $rss_peak"
	echo "VSZ Average (MB): $vsz_avg"
	echo "VSZ Peak (MB): $vsz_peak"
	echo
	echo "CSV detail: $CSV_FILE"
} | tee "$SUMMARY_FILE"

