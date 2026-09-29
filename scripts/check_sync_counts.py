#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
check_sync_counts.py —— MySQL 与 ClickHouse 同步行数核对 + 钉钉告警

数据全部取自 SeaTunnel 同步配置（MySQL-CDC source + Clickhouse sink）：
  - 待检查的表      -> source 的 table-names
  - MySQL 账密/地址 -> source 的 username/password/base-url（或 url）
  - ClickHouse 账密 -> sink 的 username/password/host（或 url）

核对逻辑：
  MySQL:       SELECT COUNT(*) FROM `db`.`table`
  ClickHouse:  SELECT count()  FROM `db`.`table` FINAL   （ReplacingMergeTree 去重）
  |MySQL - ClickHouse| > 阈值（默认 10）或任一查询失败 -> 聚合所有异常表发送钉钉

脚本级配置（配置路径、阈值、钉钉 webhook/keyword）在下面「配置区」里改；运行：
  python3 scripts/check_sync_counts.py
  python3 scripts/check_sync_counts.py --list-tables   # 只解析配置、列出待检查的表，不连库
  python3 scripts/check_sync_counts.py --dry-run       # 只打印钉钉告警内容，不发钉钉

退出码：
  0  全部正常
  1  存在超阈值差异或查询失败
  2  配置/运行环境错误（如一个可检查的配置都没有）

依赖：python3 + （pymysql 或本机 mysql 客户端）；ClickHouse 走 HTTP 接口，无需客户端。
"""

from __future__ import annotations

import argparse
import base64
import glob
import json
import os
import re
import shutil
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

# ============================================================
# 配置区（按需修改）
# ============================================================

# 同步配置文件或目录，可写多个
CONF_PATHS = ["/mnt/docker-script/seatunnel/config"]

# 差异阈值：|MySQL - ClickHouse| > THRESHOLD 时告警
THRESHOLD = 10

# 查询超时（秒）
QUERY_TIMEOUT = 30

# MySQL / ClickHouse 的地址、库名、账号密码都从同步配置读取，脚本里不用配

# 钉钉机器人（keyword 安全模式）：webhook 留空则只打印告警内容、不发送
DINGTALK_WEBHOOK = "https://oapi.dingtalk.com/robot/send"
DINGTALK_KEYWORD = ""  # 机器人安全设置里的自定义关键词，会写入消息内容

# ============================================================


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


# ============================================================
# 一、HOCON 配置解析（仅覆盖本项目配置用到的最小子集）
# ============================================================

_STR_RE = r'"(?:\\.|[^"\\])*"'


def strip_comments(text: str) -> str:
    """去掉 HOCON 注释（# 和 //），不碰字符串内部（密码里的 # 要保留）。"""
    out: List[str] = []
    i, n = 0, len(text)
    quote: Optional[str] = None  # None / '"' / '"""'
    while i < n:
        ch = text[i]
        if quote == '"""':
            if text.startswith('"""', i):
                quote = None
                out.append('"""')
                i += 3
            else:
                out.append(ch)
                i += 1
        elif quote == '"':
            out.append(ch)
            if ch == "\\" and i + 1 < n:
                out.append(text[i + 1])
                i += 2
            else:
                if ch == '"':
                    quote = None
                i += 1
        else:
            if text.startswith('"""', i):
                quote = '"""'
                out.append('"""')
                i += 3
            elif ch == '"':
                quote = '"'
                out.append(ch)
                i += 1
            elif ch == "#":
                while i < n and text[i] != "\n":
                    i += 1
            elif ch == "/" and i + 1 < n and text[i + 1] == "/":
                while i < n and text[i] != "\n":
                    i += 1
            else:
                out.append(ch)
                i += 1
    return "".join(out)


def _scan_block(text: str, open_idx: int, open_ch: str, close_ch: str) -> Optional[str]:
    """从 open_idx 处的 open_ch 开始，扫描出配对的块内容（字符串内的括号不算）。"""
    depth = 0
    quote: Optional[str] = None
    i, n = open_idx, len(text)
    while i < n:
        ch = text[i]
        if quote == '"""':
            if text.startswith('"""', i):
                quote = None
                i += 3
            else:
                i += 1
        elif quote == '"':
            if ch == "\\":
                i += 2
            else:
                if ch == '"':
                    quote = None
                i += 1
        else:
            if text.startswith('"""', i):
                quote = '"""'
                i += 3
            elif ch == '"':
                quote = '"'
                i += 1
            elif ch == open_ch:
                depth += 1
                i += 1
            elif ch == close_ch:
                depth -= 1
                if depth == 0:
                    return text[open_idx + 1 : i]
                i += 1
            else:
                i += 1
    return None


def find_block(text: str, name: str, start: int = 0) -> Optional[Tuple[str, int]]:
    """找到 `name { ... }`，返回 (块内容, 结束位置)。支持带引号的插件名写法。"""
    pat = re.compile(r'(?<![A-Za-z0-9_.\-])["\']?' + re.escape(name) + r'["\']?\s*\{')
    m = pat.search(text, start)
    if not m:
        return None
    open_idx = text.index("{", m.start())
    inner = _scan_block(text, open_idx, "{", "}")
    if inner is None:
        return None
    return inner, open_idx + len(inner) + 2


def iter_blocks(text: str, name: str) -> Iterable[str]:
    pos = 0
    while True:
        r = find_block(text, name, pos)
        if r is None:
            return
        inner, end = r
        yield inner
        pos = end


def _unquote(value: str) -> str:
    if len(value) >= 2 and value[0] == '"' and value[-1] == '"':
        return re.sub(r"\\(.)", r"\1", value[1:-1])
    return value


def get_scalar(block: str, key: str) -> Optional[str]:
    pat = re.compile(
        r"(?<![A-Za-z0-9_.\-])" + re.escape(key) + r"\s*=\s*(" + _STR_RE + r"|[^\s,#}\]]+)"
    )
    m = pat.search(block)
    return _unquote(m.group(1)) if m else None


def get_array(block: str, key: str) -> List[str]:
    m = re.search(r"(?<![A-Za-z0-9_.\-])" + re.escape(key) + r"\s*=\s*\[", block)
    if not m:
        return []
    open_idx = block.index("[", m.start())
    inner = _scan_block(block, open_idx, "[", "]")
    if inner is None:
        return []
    return [_unquote(x) for x in re.findall(_STR_RE, inner)]


# ============================================================
# 二、同步配置 -> 检查任务
# ============================================================


@dataclass
class TableRef:
    db: str
    name: str

    @property
    def full(self) -> str:
        return f"{self.db}.{self.name}"


@dataclass
class Task:
    file: str
    mysql: dict
    clickhouse: dict
    tables: List[TableRef]


def read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return f.read()


def parse_mysql_url(url: str) -> dict:
    m = re.match(r"jdbc:mysql://([^/:?#]+)(?::(\d+))?(?:/([^?#]*))?", url or "")
    if not m:
        return {}
    return {
        "host": m.group(1),
        "port": int(m.group(2) or 3306),
        "database": (m.group(3) or "").strip("/"),
    }


def parse_mysql_block(block: str) -> dict:
    url = get_scalar(block, "base-url") or get_scalar(block, "url") or ""
    info = parse_mysql_url(url)
    db = info.get("database") or ""
    if not db:
        names = get_array(block, "database-names")
        db = names[0] if names else ""
    return {
        "host": info.get("host") or get_scalar(block, "host") or "localhost",
        "port": int(info.get("port") or get_scalar(block, "port") or 3306),
        "user": get_scalar(block, "username") or get_scalar(block, "user") or "root",
        "password": get_scalar(block, "password") or "",
        "database": db,
    }


def parse_clickhouse_block(block: str) -> dict:
    url = (get_scalar(block, "url") or "").strip()
    host_raw = (get_scalar(block, "host") or "").strip()
    scheme, host, port = "http", "", 8123

    def _apply_url(u: str) -> None:
        nonlocal scheme, host, port
        if u.startswith("jdbc:clickhouse://"):
            m = re.match(r"jdbc:clickhouse://([^/:?#]+)(?::(\d+))?", u)
            if m:
                host = m.group(1)
                port = int(m.group(2) or 8123)
            return
        if "://" not in u:
            u = "http://" + u
        p = urllib.parse.urlparse(u)
        scheme = p.scheme or scheme
        if p.hostname:
            host = p.hostname
        if p.port:
            port = p.port

    if url:
        _apply_url(url)
    elif host_raw:
        _apply_url(host_raw)

    return {
        "scheme": scheme,
        "host": host or "localhost",
        "port": int(port),
        "user": get_scalar(block, "username") or get_scalar(block, "user") or "default",
        "password": get_scalar(block, "password") or "",
        "database": get_scalar(block, "database") or "",
    }


def normalize_tables(names: Sequence[str], default_db: str) -> Tuple[List[TableRef], List[str]]:
    tables: List[TableRef] = []
    skipped: List[str] = []
    seen = set()
    for raw in names:
        name = (raw or "").strip()
        if not name:
            continue
        if "*" in name:
            skipped.append(f"{name}（通配符暂不支持，MySQL-CDC 的 table-names 一般应为真实表名）")
            continue
        if "." in name:
            db, _, tbl = name.rpartition(".")
        else:
            db, tbl = default_db, name
        if not db or not tbl:
            skipped.append(f"{name}（无法确定所属库，请在 table-names 里写 db.table）")
            continue
        key = f"{db}.{tbl}"
        if key in seen:
            skipped.append(f"{key}（配置内重复）")
            continue
        seen.add(key)
        tables.append(TableRef(db=db, name=tbl))
    return tables, skipped


def parse_conf_file(path: str) -> Tuple[List[Task], List[str]]:
    """解析一个同步配置，返回 (任务列表, 警告列表)。非 MySQL->ClickHouse 配置返回空。"""
    warns: List[str] = []
    fname = os.path.basename(path)
    try:
        text = strip_comments(read_text(path))
    except OSError as e:
        return [], [f"{fname}: 读取失败 {e}"]

    src_blocks: List[str] = []
    for s in iter_blocks(text, "source"):
        src_blocks.extend(iter_blocks(s, "MySQL-CDC"))
    if not src_blocks:
        src_blocks = list(iter_blocks(text, "MySQL-CDC"))
    if not src_blocks:
        return [], []

    ch_blocks: List[str] = []
    for s in iter_blocks(text, "sink"):
        ch_blocks.extend(iter_blocks(s, "Clickhouse"))
    if not ch_blocks:
        ch_blocks = list(iter_blocks(text, "Clickhouse"))
    if not ch_blocks:
        return [], [f"{fname}: 没有 Clickhouse sink，跳过"]

    ch_creds = [parse_clickhouse_block(b) for b in ch_blocks]
    tasks: List[Task] = []
    for sb in src_blocks:
        my = parse_mysql_block(sb)
        tables, skipped = normalize_tables(get_array(sb, "table-names"), my["database"])
        for s in skipped:
            warns.append(f"{fname}: 跳过 {s}")
        if not tables:
            continue
        ch = ch_creds[0]
        if len(ch_creds) > 1:
            dbs = {t.db for t in tables}
            match = [c for c in ch_creds if c.get("database") in dbs]
            ch = match[0] if match else ch_creds[0]
            warns.append(
                f"{fname}: 检测到 {len(ch_creds)} 个 Clickhouse sink，"
                f"这些表使用 database={ch.get('database')!r} 的 sink"
            )
        tasks.append(Task(file=fname, mysql=my, clickhouse=ch, tables=tables))
    return tasks, warns


def discover_conf_files() -> List[str]:
    paths: List[str] = []
    for p in CONF_PATHS:
        if os.path.isdir(p):
            for pat in ("*.conf", "*.config"):
                paths.extend(sorted(glob.glob(os.path.join(p, pat))))
        elif os.path.isfile(p):
            paths.append(p)
        else:
            eprint(f"[warn] 配置路径不存在: {p}")
    out: List[str] = []
    seen = set()
    for p in paths:
        rp = os.path.realpath(p)
        if rp not in seen and os.path.isfile(rp):
            seen.add(rp)
            out.append(p)
    return out


# ============================================================
# 三、连接层：MySQL（pymysql / mysql CLI）、ClickHouse HTTP
# ============================================================


class SyncCheckError(Exception):
    pass


_PYMYSQL = None
_PYMYSQL_LOADED = False


def load_pymysql():
    global _PYMYSQL, _PYMYSQL_LOADED
    if not _PYMYSQL_LOADED:
        _PYMYSQL_LOADED = True
        try:
            import pymysql  # type: ignore

            _PYMYSQL = pymysql
        except ImportError:
            _PYMYSQL = None
    return _PYMYSQL


class MySQLRunner:
    """执行 SQL 并返回无表头的 TSV 文本。"""

    def __init__(self, kind, host, port, user, password, timeout):
        self.kind = kind
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.timeout = timeout
        self.desc = f"{'pymysql' if kind == 'pymysql' else 'mysql-cli'} {host}:{port}"
        self._conn = None

    def close(self):
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def query(self, sql: str) -> str:
        if self.kind == "pymysql":
            return self._query_pymysql(sql)
        return self._query_cli(sql)

    def _query_pymysql(self, sql: str) -> str:
        pymysql = load_pymysql()
        if self._conn is None:
            self._conn = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                connect_timeout=10,
                read_timeout=self.timeout,
                charset="utf8mb4",
                autocommit=True,
            )
        with self._conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        return "\n".join("\t".join("" if v is None else str(v) for v in row) for row in rows)

    def _query_cli(self, sql: str) -> str:
        env = dict(os.environ)
        env["MYSQL_PWD"] = self.password
        cmd = [
            shutil.which("mysql"),
            "--protocol=TCP",
            "-h", self.host,
            "-P", str(self.port),
            "-u", self.user,
            "--batch", "--raw", "--skip-column-names",
            "--connect-timeout=10",
            "-e", sql,
        ]
        p = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=self.timeout + 15)
        if p.returncode != 0:
            raise SyncCheckError((p.stderr or p.stdout).strip()[:500])
        return p.stdout


def connect_mysql(cred: dict) -> MySQLRunner:
    host, port = cred["host"], int(cred["port"])
    user, pwd = cred["user"], cred["password"]

    if load_pymysql() is not None:
        runner = MySQLRunner("pymysql", host, port, user, pwd, QUERY_TIMEOUT)
    elif shutil.which("mysql"):
        runner = MySQLRunner("cli", host, port, user, pwd, QUERY_TIMEOUT)
    else:
        raise SyncCheckError("缺少 MySQL 客户端：pip install pymysql，或安装 mysql-client")
    try:
        runner.query("SELECT 1")
        return runner
    except Exception as e:
        runner.close()
        raise SyncCheckError(f"MySQL 连接失败（{host}:{port}）：{e}")


class ClickHouseHTTP:
    def __init__(self, base_url: str, user: str, password: str, timeout: int):
        self.base_url = base_url.rstrip("/")
        self.user = user
        self.password = password
        self.timeout = timeout

    def query(self, sql: str) -> str:
        url = self.base_url + "/?default_format=TabSeparated"
        req = urllib.request.Request(url, data=sql.encode("utf-8"), method="POST")
        token = base64.b64encode(f"{self.user}:{self.password}".encode("utf-8")).decode("ascii")
        req.add_header("Authorization", "Basic " + token)
        req.add_header("Content-Type", "text/plain; charset=utf-8")
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return resp.read().decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "replace")
            raise SyncCheckError(f"HTTP {e.code}: {' '.join(body.split())[:400]}")
        except Exception as e:
            raise SyncCheckError(str(e))


def connect_clickhouse(cred: dict) -> Tuple[ClickHouseHTTP, str]:
    base = f"{cred['scheme']}://{cred['host']}:{cred['port']}"
    client = ClickHouseHTTP(base, cred["user"], cred["password"], QUERY_TIMEOUT)
    try:
        client.query("SELECT 1")
        return client, base
    except Exception as e:
        raise SyncCheckError(f"ClickHouse 连接失败（{base}）：{e}")


# ============================================================
# 四、行数查询
# ============================================================


def qid(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def mysql_union_sql(tables: Sequence[TableRef]) -> str:
    parts = [
        f"SELECT {i} AS t, COUNT(*) AS c FROM {qid(t.db)}.{qid(t.name)}"
        for i, t in enumerate(tables)
    ]
    return " UNION ALL ".join(parts)


def ch_union_sql(tables: Sequence[TableRef]) -> str:
    parts = [
        f"SELECT {i} AS t, count() AS c FROM {qid(t.db)}.{qid(t.name)} FINAL"
        for i, t in enumerate(tables)
    ]
    return " UNION ALL ".join(parts)


def parse_alias_rows(text: str) -> Dict[int, int]:
    out: Dict[int, int] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        fields = line.split("\t")
        if len(fields) < 2:
            continue
        try:
            out[int(fields[0].strip())] = int(fields[1].strip())
        except ValueError:
            continue
    return out


def _parse_single_count(text: str) -> int:
    for line in text.splitlines():
        line = line.strip()
        if line:
            return int(line.split("\t")[0].split()[0])
    raise SyncCheckError("查询结果为空")


def fetch_mysql_counts(runner: Optional[MySQLRunner], tables: Sequence[TableRef], conn_err: str = ""):
    counts: Dict[int, int] = {}
    errors: Dict[int, str] = {}
    if runner is None:
        return counts, {i: conn_err for i in range(len(tables))}
    try:
        rows = parse_alias_rows(runner.query(mysql_union_sql(tables)))
        if len(rows) == len(tables):
            return rows, errors
        errors = {i: "批量查询未返回该表结果" for i in range(len(tables)) if i not in rows}
        return rows, errors
    except Exception:
        pass
    for i, t in enumerate(tables):
        try:
            counts[i] = _parse_single_count(
                runner.query(f"SELECT COUNT(*) FROM {qid(t.db)}.{qid(t.name)}")
            )
        except Exception as e:
            errors[i] = str(e)
    return counts, errors


def fetch_ch_counts(client: Optional[ClickHouseHTTP], tables: Sequence[TableRef], conn_err: str = ""):
    counts: Dict[int, int] = {}
    errors: Dict[int, str] = {}
    if client is None:
        return counts, {i: conn_err for i in range(len(tables))}
    try:
        rows = parse_alias_rows(client.query(ch_union_sql(tables)))
        if len(rows) == len(tables):
            return rows, errors
        errors = {i: "批量查询未返回该表结果" for i in range(len(tables)) if i not in rows}
        return rows, errors
    except Exception:
        pass
    for i, t in enumerate(tables):
        try:
            counts[i] = _parse_single_count(
                client.query(f"SELECT count() FROM {qid(t.db)}.{qid(t.name)} FINAL")
            )
        except Exception as e:
            errors[i] = str(e)
    return counts, errors


# ============================================================
# 五、结果与告警
# ============================================================


@dataclass
class CompareRow:
    task_file: str
    table: str
    mysql: Optional[int] = None
    clickhouse: Optional[int] = None
    errors: List[str] = field(default_factory=list)

    @property
    def diff(self) -> Optional[int]:
        if self.mysql is None or self.clickhouse is None:
            return None
        return self.mysql - self.clickhouse

    @property
    def is_error(self) -> bool:
        return bool(self.errors)

    def is_anomaly(self, threshold: int) -> bool:
        if self.is_error:
            return True
        return abs(self.diff) > threshold


def print_report(rows: List[CompareRow], threshold: int, only_diff: bool) -> None:
    last_file = None
    widths = max([len(r.table) for r in rows] + [len("表名")])
    for r in rows:
        if r.task_file != last_file:
            last_file = r.task_file
            print(f"\n== {last_file} ==")
            print(f"{'表名'.ljust(max(2, widths - 2))}  {'MySQL':>12}  {'ClickHouse':>12}  {'差异':>10}  状态")
        if only_diff and not r.is_anomaly(threshold):
            continue
        my = f"{r.mysql:,}" if r.mysql is not None else "-"
        ch = f"{r.clickhouse:,}" if r.clickhouse is not None else "-"
        diff = f"{r.diff:+,}" if r.diff is not None else "-"
        if r.is_error:
            status = "ERR"
        elif r.is_anomaly(threshold):
            status = "DIFF"
        else:
            status = "OK"
        print(f"{r.table:<{widths}}  {my:>12}  {ch:>12}  {diff:>10}  {status}")
        for err in r.errors:
            print(f"{'':<{widths}}  └─ {err}")


def build_alert(
    rows: List[CompareRow], threshold: int, keyword: str, started_at: datetime
) -> Tuple[str, List[str], List[str]]:
    diff_rows = [r for r in rows if not r.is_error and abs(r.diff) > threshold]
    err_rows = [r for r in rows if r.is_error]
    title = "数据同步数量告警"
    head = [
        f"## {title}",
        "",
    ]
    if keyword:
        # 钉钉机器人「自定义关键词」安全设置要求消息内容包含该关键词
        head.append(f"> 关键词：{keyword}")
    head.extend([
        f"> 时间：{started_at:%Y-%m-%d %H:%M:%S}",
        f"> 阈值：{threshold} 行 | 检查 {len(rows)} 张表 | "
        f"行数差异 {len(diff_rows)} 张 | 查询失败 {len(err_rows)} 张",
    ])
    body: List[str] = []
    if diff_rows:
        body.append("")
        body.append("**行数差异（MySQL - ClickHouse）**")
        for r in sorted(diff_rows, key=lambda x: -abs(x.diff or 0)):
            body.append(
                f"- `{r.table}`：MySQL {r.mysql:,} / ClickHouse {r.clickhouse:,}，"
                f"差异 **{r.diff:+,}**"
            )
    if err_rows:
        body.append("")
        body.append("**查询失败**")
        groups: Dict[str, List[str]] = {}
        for r in err_rows:
            msg = "；".join(" ".join(e.split()).replace("`", "'") for e in r.errors)
            groups.setdefault(msg, []).append(r.table)
        for msg, tbls in groups.items():
            if len(tbls) <= 4:
                names = "、".join(f"`{t}`" for t in tbls)
                body.append(f"- {names}：{msg}")
            else:
                sample = "、".join(f"`{t}`" for t in tbls[:3])
                body.append(f"- {sample} 等 {len(tbls)} 张表：{msg}")
    return title, head, body


def chunk_lines(lines: List[str], limit: int = 12000) -> List[List[str]]:
    chunks: List[List[str]] = []
    cur: List[str] = []
    size = 0
    for line in lines:
        b = len(line.encode("utf-8")) + 1
        if cur and size + b > limit:
            chunks.append(cur)
            cur, size = [], 0
        if b > limit:
            line = line[: limit // 4]
            b = len(line.encode("utf-8")) + 1
        cur.append(line)
        size += b
    if cur:
        chunks.append(cur)
    return chunks


def send_dingtalk(webhook: str, title: str, head: List[str], body: List[str], timeout: int) -> bool:
    chunks = chunk_lines(body)
    ok = True
    for idx, chunk in enumerate(chunks, 1):
        msg_title = title if len(chunks) == 1 else f"{title}（{idx}/{len(chunks)}）"
        text = "\n".join(head + chunk)
        payload = {"msgtype": "markdown", "markdown": {"title": msg_title, "text": text}}
        req = urllib.request.Request(
            webhook,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            method="POST",
        )
        req.add_header("Content-Type", "application/json; charset=utf-8")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8", "replace")
        except Exception as e:
            eprint(f"[error] 钉钉发送失败: {e}")
            ok = False
            continue
        try:
            data = json.loads(raw)
            if data.get("errcode") != 0:
                eprint(f"[error] 钉钉返回错误: {raw}")
                ok = False
        except ValueError:
            eprint(f"[warn] 钉钉返回非 JSON 内容: {raw[:200]}")
    return ok


# ============================================================
# 六、主流程
# ============================================================


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="比对 MySQL 与 ClickHouse 行数，超阈值发钉钉告警（表、地址、账密取自 SeaTunnel 同步配置）"
    )
    p.add_argument("--list-tables", action="store_true", help="只解析同步配置、列出待检查的表，不连库")
    p.add_argument("--dry-run", action="store_true", help="只打印钉钉告警内容，不发送")
    return p.parse_args(argv)


def print_tasks(tasks: List[Task]) -> None:
    for t in tasks:
        my = t.mysql
        ch = t.clickhouse
        print(f"\n== {t.file} ==")
        print(f"  MySQL:      {my['user']}@{my['host']}:{my['port']}")
        print(f"  ClickHouse: {ch['user']}@{ch['scheme']}://{ch['host']}:{ch['port']}"
              + (f"/{ch['database']}" if ch.get("database") else ""))
        print(f"  表（{len(t.tables)}）:")
        for tb in t.tables:
            print(f"    {tb.full}")


def main(argv: Optional[Sequence[str]] = None) -> int:
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
        except Exception:
            pass

    opts = parse_args(argv)
    started_at = datetime.now()
    threshold = abs(THRESHOLD)

    files = discover_conf_files()
    if not files:
        eprint(f"[error] 没找到同步配置文件，请检查脚本配置区的 CONF_PATHS: {CONF_PATHS}")
        return 2

    tasks: List[Task] = []
    for f in files:
        t, warns = parse_conf_file(f)
        for w in warns:
            eprint(f"[warn] {w}")
        tasks.extend(t)

    # 去重完全相同的 (MySQL, ClickHouse, 表)
    dedup: List[Task] = []
    seen_keys = set()
    for t in tasks:
        key = (t.mysql["host"], t.mysql["port"], t.mysql["user"],
               t.clickhouse["host"], t.clickhouse["port"], t.clickhouse["database"])
        fresh = []
        for tb in t.tables:
            k = key + (tb.full,)
            if k in seen_keys:
                eprint(f"[warn] {t.file}: {tb.full} 在其它配置中已检查过，跳过重复项")
                continue
            seen_keys.add(k)
            fresh.append(tb)
        if fresh:
            dedup.append(Task(file=t.file, mysql=t.mysql, clickhouse=t.clickhouse, tables=fresh))
    tasks = dedup

    if not tasks:
        eprint("[error] 配置里没有可检查的表（需要 MySQL-CDC 的 table-names 且目标为 Clickhouse）")
        return 2

    if opts.list_tables:
        print_tasks(tasks)
        total = sum(len(t.tables) for t in tasks)
        print(f"\n共 {len(tasks)} 个配置、{total} 张表")
        return 0

    eprint(f"[info] 配置: {len(files)} 个文件，{len(tasks)} 个检查任务，"
           f"共 {sum(len(t.tables) for t in tasks)} 张表，阈值 {threshold}")

    mysql_cache: Dict[tuple, object] = {}
    ch_cache: Dict[tuple, object] = {}

    def get_mysql(cred: dict):
        key = (cred["host"], cred["port"], cred["user"], cred["password"])
        if key not in mysql_cache:
            try:
                r = connect_mysql(cred)
                mysql_cache[key] = r
                eprint(f"[info] MySQL 已连接: {r.desc}")
            except SyncCheckError as e:
                mysql_cache[key] = e
        v = mysql_cache[key]
        if isinstance(v, SyncCheckError):
            raise v
        return v

    def get_clickhouse(cred: dict):
        key = (cred["scheme"], cred["host"], cred["port"], cred["user"], cred["password"])
        if key not in ch_cache:
            try:
                c, url = connect_clickhouse(cred)
                ch_cache[key] = c
                eprint(f"[info] ClickHouse 已连接: {url}")
            except SyncCheckError as e:
                ch_cache[key] = e
        v = ch_cache[key]
        if isinstance(v, SyncCheckError):
            raise v
        return v

    rows: List[CompareRow] = []
    for task in tasks:
        try:
            runner = get_mysql(task.mysql)
            mysql_err = ""
        except SyncCheckError as e:
            runner, mysql_err = None, str(e)
        try:
            client = get_clickhouse(task.clickhouse)
            ch_err = ""
        except SyncCheckError as e:
            client, ch_err = None, str(e)

        my_counts, my_errors = fetch_mysql_counts(runner, task.tables, mysql_err)
        ch_counts, ch_errors = fetch_ch_counts(client, task.tables, ch_err)
        for i, tb in enumerate(task.tables):
            errs = []
            if i in my_errors:
                errs.append(f"MySQL: {my_errors[i]}")
            if i in ch_errors:
                errs.append(f"ClickHouse: {ch_errors[i]}")
            rows.append(CompareRow(
                task_file=task.file,
                table=tb.full,
                mysql=my_counts.get(i),
                clickhouse=ch_counts.get(i),
                errors=errs,
            ))

    # 关闭长连接
    for v in mysql_cache.values():
        if isinstance(v, MySQLRunner):
            v.close()

    print_report(rows, threshold, only_diff=False)

    diff_rows = [r for r in rows if not r.is_error and abs(r.diff) > threshold]
    err_rows = [r for r in rows if r.is_error]
    print(f"\n共 {len(rows)} 张表：差异 {len(diff_rows)}，失败 {len(err_rows)}，"
          f"正常 {len(rows) - len(diff_rows) - len(err_rows)}（阈值 {threshold}）")

    if not diff_rows and not err_rows:
        print("[OK] 行数核对通过")
        return 0

    keyword = DINGTALK_KEYWORD.strip()
    title, head, body = build_alert(rows, threshold, keyword, started_at)
    print("\n" + "=" * 72)
    print("\n".join(head + body))
    print("=" * 72)

    if opts.dry_run:
        eprint("[info] --dry-run：告警内容已打印，未发送钉钉")
    elif not DINGTALK_WEBHOOK.strip():
        eprint("[warn] 未配置 DINGTALK_WEBHOOK，未发送钉钉（仅本地打印）")
    else:
        if send_dingtalk(DINGTALK_WEBHOOK.strip(), title, head, body, timeout=10):
            eprint("[info] 钉钉告警已发送")
        else:
            eprint("[error] 钉钉告警发送失败")

    return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        eprint("\n[info] 已中断")
        sys.exit(130)
    except BrokenPipeError:
        # 管道下游（如 head）提前退出时静默结束，避免打印 traceback
        try:
            os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        except Exception:
            pass
        sys.exit(0)
