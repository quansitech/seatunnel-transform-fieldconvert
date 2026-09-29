# scripts

SeaTunnel（Zeta）运维辅助脚本。

## show_jobs.sh

通过 SeaTunnel **v2 REST API** 查看任务列表和运行情况：状态、错误信息、读写行数，
以及每张表的「源读取 / 已写出 / 差值」（差值 = 积压），一眼定位哪个表拖后腿。

### 依赖

| 依赖 | 说明 |
|------|------|
| bash | 4.0+（脚本用到 `${var,,}` 小写转换） |
| curl | 发 REST 请求 |
| python3 | 解析 JSON、格式化输出 |
| SeaTunnel | 已开启 v2 REST 服务（容器内默认 8080，带 Basic Auth） |

### 安装

```bash
chmod +x show_jobs.sh
```

> 原名叫 `show_fail.sh` 也能用，功能完全相同（默认参数就是 `failed`）。

### 用法

```bash
./show_jobs.sh [类型]
```

不带参数时默认 `failed`。

| 类型 | 含义 | 对应 API |
|------|------|----------|
| `all` | 所有任务（运行中 + 已结束全部状态） | `/finished-jobs` + `/running-jobs` |
| `running` | 运行中的任务 | `/running-jobs` |
| `failed` | 失败的任务（**默认**） | `/finished-jobs/FAILED` |
| `finished` | 已成功完成的任务 | `/finished-jobs/FINISHED` |
| `canceled` | 已取消的任务 | `/finished-jobs/CANCELED` |
| `savepoint` | 以 savepoint 方式停止的任务 | `/finished-jobs/SAVEPOINT_DONE` |
| `unknowable` | 状态未知的任务 | `/finished-jobs/UNKNOWABLE` |
| `<jobId>` | 指定单个任务详情 | `/job-info/<jobId>` |

其他参数/状态名也可以直接传（会原样拼到 `/finished-jobs/<STATE>`），状态枚举见
`org.apache.seatunnel.engine.server.master.JobHistoryService.JobState`。

### 配置

地址和账号通过环境变量传入，不改脚本即可切换环境：

```bash
# 内置默认值：
#   ST_BASE = http://localhost:8080
# AUTH 没有内置默认值，需要自己传入（见集群 seatunnel.yaml 的 basic-auth-username/password）：
#   AUTH    = <user>:<password>

# 示例：指向另一套环境
ST_BASE='http://<master-host>:8084' AUTH='<user>:<pass>' ./show_jobs.sh running
```

- `ST_BASE` 不要带结尾斜杠。
- 集群没开 Basic Auth 时，`AUTH=''` 即可不发送认证头。

### 输出示例

```
[FAILED] 1156250172460630017  mysql-to-ch-marathon-2
  时间: 2026-09-26 15:25:23 -> 2026-09-26 16:02:53
  错误: org.apache.seatunnel.engine.server.checkpoint.CheckpointException: Checkpoint expired before completing. Please increase checkpoint timeout in the seatunnel.yaml or jobConfig env.
  读取: 1980123 行 / 已提交: 1617717 行 / 队列积压: 14346
  表                                            源读取       已写出        差值
  donation.qs_jd_log                        163649    155453      8196
  donation.qs_jd_info                       119368    113218      6150
  donation.qs_donatebook_private_detail     715946    715946         0
  donation.qs_donation_h5                     1152      1152         0
```

字段说明：

| 字段 | 含义 |
|------|------|
| `读取` | 数据源已读取总行数（`SourceReceivedCount`） |
| `已提交` | Sink 已提交总行数（`SinkCommittedCount`） |
| `队列积压` | 中间队列积压行数（`IntermediateQueueSize`），持续 > 0 说明 Sink 写不过来 |
| `源读取 / 已写出 / 差值` | 每张表的读取行数、写出行数、差值；差值非 0 = 该表有积压 |

行为说明：

- 表格按「差值 = 源读取 - 已写出」**从大到小排序（差值相同按表名排序），完整展示所有表**
  （不折叠、不截断），差值非 0 的排在最上面，一眼看到积压最大的表。
- 所有表读写一致时，末尾提示「各表读写一致，无积压」；没有表级指标时提示
  「无表级指标数据」。
- 运行中的任务没有 `finishTime`，显示为 `(运行中)`。
- `all` 会把已结束和运行中的任务合并输出。

### 常见问题

| 现象 | 原因 / 处理 |
|------|-------------|
| `401 Unauthorized` | `AUTH` 不对；对照 `seatunnel.yaml` 的 `basic-auth-username/password` |
| `Empty reply from server` / 连不上 | `ST_BASE` 端口不对。v2 REST 在容器内 `8080`（宿主机映射可能是 8084/8080）；`5801` 是老的 v1 Hazelcast 端口，路径不同 |
| `没有匹配的任务` | 该状态确实没有任务；已结束任务只保留 **24 小时**（`history-job-expire-minutes: 1440`），超时会自动清理 |
| 有任务但指标为空 | 任务刚提交还没有 metrics，或历史指标已被清理 |
| `./show_jobs.sh: line N: warning: here-document ... (wanted 'PY')` | 文件被编辑器/复制粘贴改出了行尾空格。执行 `sed -i 's/[[:space:]]*$//' show_jobs.sh` 修复 |

## show_checkpoints.sh

格式化查看某个任务的 checkpoint 概览：触发/完成/失败计数、最近完成与失败、历史明细、
耗时与触发间隔统计。专治 `/jobs/checkpoints/<jobId>` 返回的一坨原始 JSON。

### 用法

```bash
./show_checkpoints.sh <jobId> [-n 条数] [-s 状态]
```

| 选项 | 说明 |
|------|------|
| `-n, --limit N` | 显示最近 N 条历史（默认 20，`0` 表示全部） |
| `-s, --status S` | 只看某状态：`COMPLETED` / `FAILED` / `IN_PROGRESS` / `CANCELED` |

### 输出示例

```
任务 1156556246841556993  概览更新时间: 2026-09-28 14:36:18

Pipeline 1
  计数: 触发 1124 | 完成 1123 | 失败 1 | 进行中 0 | 恢复 1
  最近完成: #1124  2026-09-28 14:36:18  耗时 56ms  state 3
  最近失败: #21  2026-09-27 20:02:51  原因: Pipeline turn to end state.

  历史 checkpoint（最近 5 条 / 共 9 条）:
       #ID  状态        触发时间                耗时    state  失败原因
      1124  COMPLETED   2026-09-28 14:36:18     56ms       3
      1123  COMPLETED   2026-09-28 14:35:18     46ms       3
      ...

  统计: 耗时 min 15ms / avg 37ms / max 56ms / p95 56ms
        触发间隔 avg 60.0s / max 60.0s
```

字段说明：

| 字段 | 含义 |
|------|------|
| `触发 / 完成 / 失败 / 进行中 / 恢复` | 该 pipeline 的 checkpoint 计数（`restored` = 任务从 checkpoint 恢复过几次） |
| `state` | checkpoint 状态统计值，稳定即可。**注意**：2.3.13 引擎里这个字段实际统计的是各 subtask 上报的非空 state 条目个数（源码 `PendingCheckpoint` 的 `.map(s -> s.length).count()` 写法所致），不是字节数；正常的情况下它是个小且稳定的数字 |
| `耗时` | 从触发到完成的时间；持续接近 `checkpoint.timeout` 就要警惕 |
| `触发间隔` | 相邻 checkpoint 的时间差，应接近 `env.checkpoint.interval` 配置 |

常见现象：

- **`latestFailed` 是 `Pipeline turn to end state.`**：任务停止/重启时，正在进行的 checkpoint
  会被标记失败，属正常现象，不代表数据问题；之后 `restored +1` 就是从最后一个成功
  checkpoint 恢复。
- **历史里看不到老的 FAILED**：概览只保留最近一段历史；查更早的用
  `/jobs/checkpoints/history/<jobId>?limit=&status=`。
- 查不到信息：任务不是流式任务、已结束，或数据已过期。

## check_sync_counts.py

比对 MySQL 与 ClickHouse 的行数，发现差异（默认阈值 10 行）或查询失败时，把异常表聚合成
一条钉钉 markdown 消息发送。

- **待检查的表从同步配置读取**：解析 `MySQL-CDC` 的 `table-names`，不需要另维护表清单。
- **两个库的账密也从同步配置读取**：MySQL 取 source 的 `base-url/username/password`，
  ClickHouse 取 sink 的 `host/username/password`。
- **ClickHouse 计数带 `FINAL`**：`SELECT count() FROM db.table FINAL`，对
  ReplacingMergeTree 的同主键重复数据去重后再和 MySQL 比。
- **批量 + 逐表兜底**：先用一条 `UNION ALL` 查完一个配置里的所有表；失败时自动退化成
  逐表查询，能精确报告是哪张表有问题（表不存在、权限不对等）。
- **只在异常时打扰**：没有差异也没有查询失败就什么都不发，退出码 0，适合挂 cron。

### 配置

脚本顶部「配置区」只需配这些：

```python
CONF_PATHS = ["/mnt/docker-script/seatunnel/config"]  # 同步配置文件或目录，可写多个
THRESHOLD = 10                                        # 差异阈值：|MySQL - ClickHouse| > 10 告警
QUERY_TIMEOUT = 30                                    # 查询超时（秒）
DINGTALK_WEBHOOK = ""                                 # 钉钉机器人 webhook，留空 = 只打印不发
DINGTALK_KEYWORD = ""                                 # 机器人安全设置的「自定义关键词」，会写入消息
```

> MySQL / ClickHouse 的地址、库名、账号密码都直接取自同步配置（`mysql:3306`、
> `clickhouse:8123`），脚本里不另配；因此脚本需要运行在能解析这些服务名的环境里
> （如和数据库同一个 docker 网络）。

### 命令行参数

| 参数 | 说明 |
|------|------|
| `--list-tables` | 只解析同步配置、列出待检查的表，不连库 |
| `--dry-run` | 只打印钉钉告警内容，不发送 |

其它（配置路径、阈值、钉钉）都读脚本「配置区」。

### 依赖

| 依赖 | 说明 |
|------|------|
| python3 | 3.8+ |
| MySQL 访问 | `pip install pymysql` 或安装本机 `mysql` 客户端（二选一） |
| ClickHouse | HTTP 接口（8123）可访问，无需客户端 |
| 钉钉 | 自定义机器人 webhook（不发告警可不配） |

### 运行

```bash
python3 scripts/check_sync_counts.py                     # 正常核对，异常发钉钉
python3 scripts/check_sync_counts.py --list-tables       # 只看解析出的表
python3 scripts/check_sync_counts.py --dry-run           # 只打印钉钉内容，不发送
```

退出码：`0` 正常；`1` 有差异或查询失败；`2` 配置/环境错误（如没找到可检查的配置）。

### 输出示例

```
== mysql_to_clickhouse_donation.conf ==
表名                                            MySQL    ClickHouse          差异  状态
donation.qs_area                               3,447         3,397         +50  DIFF
donation.qs_book_info                            27            52         -25  DIFF
donation.qs_center_role_user                     25            25          +0  OK
...

共 98 张表：差异 2，失败 0，正常 96（阈值 10）
```

钉钉消息内容：

```
## 数据同步数量告警

> 时间：2026-10-14 10:00:00
> 阈值：10 行 | 检查 98 张表 | 行数差异 2 张 | 查询失败 0 张

**行数差异（MySQL - ClickHouse）**
- `donation.qs_area`：MySQL 3,447 / ClickHouse 3,397，差异 **+50**
- `donation.qs_book_info`：MySQL 27 / ClickHouse 52，差异 **-25**
```

### 定时检查（crontab）

```cron
# 每 10 分钟核对一次，异常发钉钉（webhook 已写在脚本配置区）
*/10 * * * * cd /mnt/www/seatunnel-transform-fieldconvert && \
  python3 scripts/check_sync_counts.py >>/var/log/sync_count_check.log 2>&1
```

### 常见问题

| 现象 | 原因 / 处理 |
|------|-------------|
| `缺少 MySQL 客户端` | `pip install pymysql`，或安装 `mysql-client` |
| `ClickHouse 连接失败` | 脚本运行环境要能解析同步配置里的服务名（`clickhouse`），或把配置里的 `host` 改成实际地址 |
| 某张表报 `Table doesn't exist` | MySQL 或 ClickHouse 端缺表，说明同步还没建表或表被删了 |
| 差异长期固定不收敛 | 检查该表同步任务是否有失败 checkpoint（用 `show_jobs.sh` 看积压），或源表有物理删除未被 CDC 捕获 |

### 相关说明

- 前两个脚本都只用 GET 查询，不会修改集群状态；本脚本只执行 `SELECT COUNT(*)`，同样只读，
  可安全在生产环境执行。
- 任务配置里的 `checkpoint.timeout / checkpoint.interval`、写入压缩等调优项不在脚本
  范围内，见仓库根目录 `README.md` 的「REST API」章节。
- checkpoint 持久化、重启恢复、map-store 配置与踩坑记录见
  [`seatunnel/checkpoint_persistence/doc.md`](https://github.com/quansitech/coding-exp/blob/main/seatunnel/checkpoint_persistence/doc.md)。
- 任务重启的重复写入问题与全量重跑（RECREATE_SCHEMA）配置见
  [`seatunnel/job_restart_full_reload/doc.md`](https://github.com/quansitech/coding-exp/blob/main/seatunnel/job_restart_full_reload/doc.md)。
