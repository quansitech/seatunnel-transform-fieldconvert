#!/bin/bash
# show_jobs.sh —— 查看 SeaTunnel(Zeta) v2 REST 上的任务状态与每表读写积压
#
# 用法: ./show_jobs.sh [all|running|failed|finished|canceled|savepoint|unknowable|<jobId>]
#   不带参数 = failed
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
    "用法: $0 [类型]" \
    "  all          所有任务（运行中 + 已结束全部状态）" \
    "  running      运行中的任务" \
    "  failed       失败的任务（默认）" \
    "  finished     已成功完成的任务" \
    "  canceled     已取消的任务" \
    "  savepoint    以 savepoint 方式停止的任务" \
    "  unknowable   状态未知的任务" \
    "  <jobId>      指定单个任务详情" \
    "" \
    "环境变量: ST_BASE（默认 $ST_BASE）、AUTH（user:pass，需自行设置）"
}

case "${1:-failed}" in
  -h|--help|help) usage; exit 0;;
esac

state="${1:-failed}"
state_lc="${state,,}"

files=()
fetch() {   # $1=API 路径  $2=输出文件
  local code
  code=$(curl -s "${AUTH_OPT[@]}" -o "$2" -w '%{http_code}' "$ST_BASE$1" 2>/dev/null)
  if [ "$code" = "200" ]; then
    files+=("$2")
    return
  fi
  echo "请求 $ST_BASE$1 失败: HTTP $code" >&2
  case "$code" in
    401) echo "  提示: 认证失败，检查 AUTH（见 seatunnel.yaml 的 basic-auth-username/password）" >&2;;
    404) echo "  提示: 路径不存在，确认 ST_BASE 指向 v2 REST（容器端口 8080，不是 v1 的 5801）" >&2;;
    000) echo "  提示: 连接失败，确认 ST_BASE 的地址和端口" >&2;;
  esac
}

case "$state_lc" in
  all)
    fetch /finished-jobs /tmp/st-jobs-1.json
    fetch /running-jobs  /tmp/st-jobs-2.json
    ;;
  running)                  fetch /running-jobs                /tmp/st-jobs-1.json;;
  failed)                   fetch /finished-jobs/FAILED        /tmp/st-jobs-1.json;;
  finished)                 fetch /finished-jobs/FINISHED      /tmp/st-jobs-1.json;;
  canceled|cancelled)       fetch /finished-jobs/CANCELED      /tmp/st-jobs-1.json;;
  savepoint|savepoint_done) fetch /finished-jobs/SAVEPOINT_DONE /tmp/st-jobs-1.json;;
  unknowable)               fetch /finished-jobs/UNKNOWABLE    /tmp/st-jobs-1.json;;
  [0-9]*)                   fetch "/job-info/$state"           /tmp/st-jobs-1.json;;
  *) usage; exit 1;;
esac

[ ${#files[@]} -eq 0 ] && exit 1

python3 - "${files[@]}" <<'PY'
import json, sys

def norm(obj):
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        if isinstance(obj.get("data"), list):   # 分页响应 {"data":[...],"total":N}
            return obj["data"]
        if "jobId" in obj:                      # 单任务详情 /job-info/<id>
            return [obj]
    return []

jobs = []
for path in sys.argv[1:]:
    try:
        with open(path) as f:
            jobs += norm(json.load(f))
    except Exception as e:
        print(f"解析 {path} 失败: {e}", file=sys.stderr)
        try:
            raw = open(path, encoding="utf-8", errors="replace").read(200)
        except Exception:
            raw = ""
        if raw.strip():
            print(f"  响应内容: {' '.join(raw.split())}", file=sys.stderr)
        else:
            print("  响应为空，确认 ST_BASE 指向 SeaTunnel v2 REST 服务", file=sys.stderr)

if not jobs:
    print("没有匹配的任务")
    sys.exit(0)

for j in jobs:
    m = j.get("metrics") or {}
    err = (j.get("errorMsg") or "").strip().splitlines()
    status = j.get("jobStatus") or "RUNNING"
    finish = j.get("finishTime") or ("(运行中)" if status == "RUNNING" else "-")
    print(f"[{status}] {j.get('jobId')}  {j.get('jobName') or '-'}")
    print(f"  时间: {j.get('createTime') or '-'} -> {finish}")
    if err and err[0] != "-":
        print(f"  错误: {err[0]}")
    print(f"  读取: {m.get('SourceReceivedCount','-')} 行"
          f" / 已提交: {m.get('SinkCommittedCount','-')} 行"
          f" / 队列积压: {m.get('IntermediateQueueSize','-')}")

    src = {k.split('.', 1)[1]: int(v)
           for k, v in (m.get('TableSourceReceivedCount') or {}).items()}
    wr = {k.split('.', 1)[1]: int(v)
          for k, v in (m.get('TableSinkWriteCount') or {}).items()}
    rows = sorted(((t, src.get(t, 0), wr.get(t, 0)) for t in set(src) | set(wr)),
                  key=lambda r: (-(r[1] - r[2]), r[0]))
    if not rows:
        print("  （无表级指标数据）")
    else:
        print(f"  {'表':<38}{'源读取':>10}{'已写出':>10}{'差值':>10}")
        for t, a, b in rows:
            print(f"  {t:<38}{a:>10}{b:>10}{a-b:>10}")
        if all(r[1] == r[2] for r in rows):
            print("  （各表读写一致，无积压）")
    print()
PY
