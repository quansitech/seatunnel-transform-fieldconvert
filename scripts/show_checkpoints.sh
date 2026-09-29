#!/bin/bash
# show_checkpoints.sh —— 格式化查看 SeaTunnel(Zeta) 某个任务的 checkpoint 状态
#
# 用法: ./show_checkpoints.sh <jobId> [选项]
#   -n, --limit N    显示最近 N 条历史（默认 20，0=全部）
#   -s, --status S   只看某状态: COMPLETED / FAILED / IN_PROGRESS / CANCELED
#   -h, --help       帮助
#
# 环境变量:
#   ST_BASE  v2 REST 地址（默认 http://localhost:8080）
#   AUTH     basic auth 'user:pass'（见 seatunnel.yaml 的 basic-auth-username/password）
#
# 详细说明见同目录 README.md

ST_BASE="${ST_BASE:-http://localhost:8080}"
AUTH="${AUTH:-}"

AUTH_OPT=()
[ -n "$AUTH" ] && AUTH_OPT=(-u "$AUTH")

usage() {
  printf '%s\n' \
    "用法: $0 <jobId> [选项]" \
    "  -n, --limit N    显示最近 N 条历史（默认 20，0=全部）" \
    "  -s, --status S   只看某状态: COMPLETED / FAILED / IN_PROGRESS / CANCELED" \
    "  -h, --help       帮助" \
    "" \
    "环境变量: ST_BASE（默认 $ST_BASE）、AUTH（user:pass，需自行设置）"
}

JOBID=""
LIMIT=20
STATUS=""

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--help) usage; exit 0;;
    -n|--limit)
      [ $# -ge 2 ] || { echo "缺少 --limit 的值" >&2; exit 1; }
      LIMIT="$2"; shift 2;;
    -s|--status)
      [ $# -ge 2 ] || { echo "缺少 --status 的值" >&2; exit 1; }
      STATUS="$2"; shift 2;;
    -*) echo "未知参数: $1" >&2; usage; exit 1;;
    *) JOBID="$1"; shift;;
  esac
done

if ! [[ "$JOBID" =~ ^[0-9]+$ ]]; then
  echo "请提供任务 ID（纯数字）" >&2
  usage
  exit 1
fi
if ! [[ "$LIMIT" =~ ^[0-9]+$ ]]; then
  echo "--limit 需要是数字（0 表示全部）" >&2
  exit 1
fi

TMP="/tmp/st-checkpoints-${JOBID}.json"
code=$(curl -s "${AUTH_OPT[@]}" -o "$TMP" -w '%{http_code}' "$ST_BASE/jobs/checkpoints/$JOBID" 2>/dev/null)
if [ "$code" != "200" ]; then
  echo "请求 $ST_BASE/jobs/checkpoints/$JOBID 失败: HTTP $code" >&2
  case "$code" in
    401) echo "  提示: 认证失败，检查 AUTH（见 seatunnel.yaml 的 basic-auth-username/password）" >&2;;
    404) echo "  提示: 路径不存在，确认 ST_BASE 指向 v2 REST（不是 v1 的 5801 端口）" >&2;;
    000) echo "  提示: 连接失败，确认 ST_BASE 的地址和端口" >&2;;
  esac
  exit 1
fi

python3 - "$TMP" "$LIMIT" "$STATUS" <<'PY'
import json, sys
from datetime import datetime

path, limit_s, status_filter = sys.argv[1], sys.argv[2], sys.argv[3]
limit = int(limit_s)
status_filter = status_filter.strip().upper()

def ts(ms):
    if not ms:
        return "-"
    try:
        return datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return str(ms)

def dur(ms):
    if ms is None:
        return "-"
    if ms < 1000:
        return f"{ms}ms"
    if ms < 120000:
        return f"{ms / 1000:.1f}s"
    return f"{ms / 60000:.1f}min"

def width(s):
    # 中日韩全角字符按 2 列宽计算，保证中英文混排表格对齐
    import unicodedata
    return sum(2 if unicodedata.east_asian_width(c) in "WF" else 1 for c in str(s))

def pad(s, n, left=False):
    s = str(s)
    sp = " " * max(0, n - width(s))
    return s + sp if left else sp + s

data = json.load(open(path))
if not isinstance(data, dict) or not data.get("pipelines"):
    print(f"任务 {data.get('jobId', path) if isinstance(data, dict) else path} 没有 checkpoint 信息"
          "（任务可能已结束、已过期，或 jobId 不存在）")
    sys.exit(0)

print(f"任务 {data.get('jobId')}  概览更新时间: {ts(data.get('updatedAt'))}")
print()

for p in data["pipelines"]:
    c = p.get("counts") or {}
    print(f"Pipeline {p.get('pipelineId')}")
    print(f"  计数: 触发 {c.get('triggered', '-')} | 完成 {c.get('completed', '-')}"
          f" | 失败 {c.get('failed', '-')} | 进行中 {c.get('inProgress', '-')}"
          f" | 恢复 {c.get('restored', '-')}")

    lc = p.get("latestCompleted") or {}
    if lc.get("checkpointId") is not None:
        print(f"  最近完成: #{lc.get('checkpointId')}  {ts(lc.get('triggerTimestamp'))}"
              f"  耗时 {dur(lc.get('durationMillis'))}  state {lc.get('stateSize', '-')}")
    lf = p.get("latestFailed") or {}
    if lf.get("checkpointId") is not None:
        print(f"  最近失败: #{lf.get('checkpointId')}  {ts(lf.get('triggerTimestamp'))}"
              f"  原因: {lf.get('failureReason') or '-'}")
    ls = p.get("latestSavepoint") or {}
    if ls.get("checkpointId") is not None:
        print(f"  最近保存: #{ls.get('checkpointId')}  {ts(ls.get('triggerTimestamp'))}"
              f"  状态 {ls.get('status', '-')}")
    for ip in (p.get("inProgress") or []):
        print(f"  进行中: #{ip.get('checkpointId')}"
              f"  已确认 {ip.get('acknowledged')}/{ip.get('total')} subtasks")

    hist = [h.get("checkpoint") or {} for h in (p.get("history") or [])]
    if status_filter:
        hist = [h for h in hist if (h.get("status") or "").upper() == status_filter]
    total_avail = len(hist)
    shown = hist if limit == 0 else hist[:limit]

    print()
    tip = f"，筛选 {status_filter}" if status_filter else ""
    if limit == 0:
        print(f"  历史 checkpoint（全部 {len(shown)} 条{tip}）:")
    else:
        print(f"  历史 checkpoint（最近 {len(shown)} 条 / 共 {total_avail} 条{tip}）:")
    print("  " + pad("#ID", 8) + "  " + pad("状态", 11, True) + " "
          + pad("触发时间", 19, True) + " " + pad("耗时", 8) + " " + pad("state", 8)
          + "  " + "失败原因")
    for h in shown:
        print("  " + pad(h.get("checkpointId"), 8) + "  "
              + pad(str(h.get("status") or "-"), 11, True) + " "
              + pad(ts(h.get("triggerTimestamp")), 19, True) + " "
              + pad(dur(h.get("durationMillis")), 8) + " " + pad(h.get("stateSize", "-"), 8)
              + "  " + (h.get("failureReason") or ""))

    durs = [h.get("durationMillis") for h in shown if h.get("durationMillis") is not None]
    trigs = sorted(h.get("triggerTimestamp") for h in shown if h.get("triggerTimestamp"))
    if durs:
        ds = sorted(durs)
        p95 = ds[min(len(ds) - 1, int(len(ds) * 0.95))]
        print(f"\n  统计: 耗时 min {dur(min(durs))} / avg {dur(sum(durs) // len(durs))}"
              f" / max {dur(max(durs))} / p95 {dur(p95)}")
    if len(trigs) >= 2:
        gaps = [trigs[i + 1] - trigs[i] for i in range(len(trigs) - 1)]
        print(f"        触发间隔 avg {dur(sum(gaps) // len(gaps))} / max {dur(max(gaps))}")
    failed = [h for h in shown if (h.get("status") or "").upper() == "FAILED"]
    if failed:
        print(f"  检出 FAILED {len(failed)} 条")
    print()
PY
