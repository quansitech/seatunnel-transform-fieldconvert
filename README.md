# SeaTunnel Transform Plugin: FieldConvert

SeaTunnel 自定义 Transform 插件，支持在数据同步管道中对指定表的指定字段进行类型转换，适用于 MySQL CDC 等场景下源端与目标端字段类型不一致的情况。

## 功能特性

- **按表按字段灵活配置**：通过 `table_pattern` 匹配表，精确指定需要转换的字段
- **通配符支持**：`table_pattern` 支持 `*` 通配符，如 `donation.*` 匹配整个库
- **多规则叠加**：同一张表可命中多条规则，分别转换不同字段
- **零侵入**：不命中的字段和表原样透传，不影响其他数据

## 支持的转换方法

| method                         | 说明                                           | 额外参数                       |
| ------------------------------ | ---------------------------------------------- | ------------------------------ |
| `unix_timestamp_to_datetime` | 将 int/long/decimal 时间戳转换为 LocalDateTime | `timezone`（时区，默认 UTC） |
| `cast`                       | 数值类型转换（如 tinyint → int）              | `target_type`（目标类型）    |

### cast 支持的 target_type

`int`、`long`、`short`、`byte`、`double`、`float`、`string`、`boolean`

## 构建方法

### 前置条件

- JDK 11+
- Maven 3.6+

### 构建

```bash
cd seatunnel-transform-fieldconvert
mvn clean package -DskipTests
```

构建产物位于 `target/seatunnel-transform-fieldconvert-1.0.0.jar`。

## 部署到 SeaTunnel

将构建好的 jar 复制到 SeaTunnel 安装目录的 `lib/` 下：

```bash
cp target/seatunnel-transform-fieldconvert-1.0.0.jar $SEATUNNEL_HOME/lib/
```

重启 SeaTunnel 任务即可生效，插件通过 SPI 自动发现。

## 配置格式

```hocon
transform {
  FieldConvert {
    plugin_input = "<上游数据流名>"
    plugin_output = "<输出数据流名>"

    rules = [
      {
        table_pattern = "<表匹配模式>"
        field = "<字段名>"
        method = "<转换方法>"
        timezone = "<时区>"          # 仅 unix_timestamp_to_datetime 需要
        target_type = "<目标类型>"   # 仅 cast 需要
      }
    ]
  }
}
```

### table_pattern 匹配规则

| 格式               | 含义             | 示例                      |
| ------------------ | ---------------- | ------------------------- |
| `database.table` | 精确匹配         | `donation.qs_book_info` |
| `database.*`     | 匹配该库下所有表 | `donation.*`            |
| `*.*` 或省略     | 匹配所有表       | —                        |

## 配置示例

### 示例一：MySQL CDC 同步到 ClickHouse，转换时间戳和类型

场景：MySQL 中 `create_date` 是 int 类型的时间戳，需要转为 ClickHouse 的 DateTime（东八区）；`is_general` 是 tinyint(1)，需要转为 ClickHouse 的 Int8。

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 15000
}

source {
  MySQL-CDC {
    plugin_output = "mysql_donation"
    username = "seatunnel_cdc"
    password = "******"
    base-url = "jdbc:mysql://mysql:3306"
    server-id = 2
    table-names = ["donation.qs_book_info"]
  }
}

transform {
  FieldConvert {
    plugin_input = "mysql_donation"
    plugin_output = "transformed_donation"

    rules = [
      {
        table_pattern = "donation.*"
        field = "create_date"
        method = "unix_timestamp_to_datetime"
        timezone = "Asia/Shanghai"
      },
      {
        table_pattern = "donation.qs_book_info"
        field = "is_general"
        method = "cast"
        target_type = "int"
      }
    ]
  }
}

sink {
  Clickhouse {
    plugin_input = "transformed_donation"
    host = "clickhouse:8123"
    database = "donation"
    table = "${table_name}"
    username = "admin"
    password = "******"
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
  }
}
```

### 示例二：100 张表统一转换时间戳字段

所有表都有 `create_date` 字段需要转换，只需一条通配规则：

```hocon
transform {
  FieldConvert {
    plugin_input = "mysql_data"
    plugin_output = "transformed_data"

    rules = [
      {
        table_pattern = "donation.*"
        field = "create_date"
        method = "unix_timestamp_to_datetime"
        timezone = "Asia/Shanghai"
      }
    ]
  }
}
```

### 示例三：多库多字段混合转换

```hocon
transform {
  FieldConvert {
    plugin_input = "source_data"
    plugin_output = "transformed_data"

    rules = [
      # order 库所有表的时间戳转换
      {
        table_pattern = "order.*"
        field = "created_at"
        method = "unix_timestamp_to_datetime"
        timezone = "Asia/Shanghai"
      },
      # user 库的 status 字段类型转换
      {
        table_pattern = "user.user_info"
        field = "status"
        method = "cast"
        target_type = "int"
      }
    ]
  }
}
```

## 项目结构

```
seatunnel-transform-fieldconvert/
├── pom.xml
├── README.md
└── src/main/java/org/apache/seatunnel/transform/fieldconvert/
    ├── FieldConvertRule.java                # 单条规则模型
    ├── FieldConvertConfig.java              # 配置解析与规则匹配
    ├── FieldConvertTransform.java           # 核心转换逻辑
    ├── FieldConvertMultiCatalogTransform.java  # 多表路由
    └── FieldConvertTransformFactory.java       # SPI 插件注册入口
```

## 技术实现

- 继承 SeaTunnel 2.3.13 的 `MultipleFieldOutputTransform`，支持同名字段类型替换（不会产生字段重复）
- 通过 `AbstractMultiCatalogMapTransform` 实现多表自动路由，不包含目标字段的表自动透传
- SPI 通过 `@AutoService` 自动注册，jar 放入 `lib/` 即可被 SeaTunnel 发现

## SeaTunnel REST API 管理 Job

SeaTunnel 2.3.13 有两套 REST API：

| 版本                 | Base URL                                         | 认证                                                                                          | 说明                                              |
| -------------------- | ------------------------------------------------ | --------------------------------------------------------------------------------------------- | ------------------------------------------------- |
| **v2（推荐）** | `http://<master-host>:8084`（容器内 `8080`） | Basic Auth：`admin / <password>`（见 `seatunnel.yaml` 的 `basic-auth-username/password`） | Jetty 实现，支持提交/查询/停止任务，Web UI 同端口 |
| v1（已废弃）         | `http://<master-host>:5801`                    | 无                                                                                            | Hazelcast REST，官方默认关闭，仅作兼容            |

### 提交 Job：POST /submit-job（v2）

Query 参数：

| 参数                     | 必填 | 说明                                              |
| ------------------------ | ---- | ------------------------------------------------- |
| `jobName`              | 否   | 任务名，便于在任务列表里识别                      |
| `jobId`                | 否   | 自定义 jobId，不传自动生成                        |
| `isStartWithSavePoint` | 否   | 是否从 savepoint 恢复（需同时传`jobId`）        |
| `format`               | 否   | 请求体格式：`json`（默认）/ `hocon` / `sql` |

请求体直接放配置内容（HOCON/SQL 按纯文本读取，`Content-Type: text/plain` 即可）。

**方式一：直接提交 HOCON 配置内容（推荐，可直接复用现有 .conf）**

```bash
curl -s -u 'admin:<password>' -X POST \
  'http://localhost:8084/submit-job?jobName=mysql-to-ch-marathon-2&format=hocon' \
  -H 'Content-Type: text/plain; charset=UTF-8' \
  --data-binary @./config/mysql_to_clickhouse_marathon_2.conf

# 响应（jobId 自动生成）
{"jobId":"<自动生成>","jobName":"mysql-to-ch-marathon-2"}
```

**方式二：multipart 上传配置文件**（字段名固定为 `config_file`，支持 `.conf`/`.config`/`.json`/`.sql`）

```bash
curl -s -u 'admin:<password>' -X POST \
  'http://localhost:8084/submit-job/upload?jobName=mysql-to-ch-marathon-2' \
  -F 'config_file=@./config/mysql_to_clickhouse_marathon_2.conf'
```

**方式三：JSON 格式**（`format=json`，插件用 `plugin_name`，source/sink 为数组）

```bash
curl -s -u 'admin:<password>' -X POST \
  'http://localhost:8084/submit-job?jobName=fake-to-console&format=json' \
  -H 'Content-Type: application/json' \
  -d '{
    "env": { "job.mode": "BATCH" },
    "source": [
      { "plugin_name": "FakeSource", "plugin_output": "fake", "row.num": 10,
        "schema": { "fields": { "name": "string", "age": "int" } } }
    ],
    "transform": [],
    "sink": [ { "plugin_name": "Console", "plugin_input": ["fake"] } ]
  }'

# 响应（jobId 自动生成）
{"jobId":"<自动生成>","jobName":"fake-to-console"}
```

> **REST 提交 vs CLI 提交**：REST 提交的任务由集群托管，HTTP 请求结束/客户端断开**不会取消任务**；而 `seatunnel.sh` 默认 `-cj=true`，客户端退出会取消任务（需要 `--async` 或 `-cj false`）。脚本/CI 里提交常驻的 STREAMING 任务推荐用 REST。

### 查询 Job（v2）

```bash
# 集群概览（运行/完成/失败任务数）
curl -s -u 'admin:<password>' http://localhost:8080/overview

# 运行中的任务
curl -s -u 'admin:<password>' http://localhost:8080/running-jobs

# 已结束的任务
curl -s -u 'admin:<password>' 'http://localhost:8080/finished-jobs?page=1&rows=10'

# 任务详情（jobStatus / errorMsg / metrics）
curl -s -u 'admin:<password>' http://localhost:8080/job-info/<JOB_ID>

# checkpoint 概览 / 历史
curl -s -u 'admin:<password>' http://localhost:8080/jobs/checkpoints/<JOB_ID>
curl -s -u 'admin:<password>' 'http://localhost:8080/jobs/checkpoints/history/<JOB_ID>?limit=10&status=COMPLETED'
```

### 停止 Job（v2）

```bash
curl -s -u 'admin:<password>' -X POST http://localhost:8080/stop-job \
  -H 'Content-Type: application/json' \
  -d '{"jobId": "<JOB_ID>", "isStopWithSavePoint": false}'
```

### 旧版 v1 API（5801，兼容用）

```bash
# 查看运行中的任务
curl -s http://localhost:5801/hazelcast/rest/maps/running-jobs

# 任务详情
curl -s http://localhost:5801/hazelcast/rest/maps/job-info/<JOB_ID>

# 已结束的任务
curl -s http://localhost:5801/hazelcast/rest/maps/finished-jobs

# 停止任务
curl -s -X POST http://localhost:5801/hazelcast/rest/maps/stop-job \
  -H "Content-Type: application/json" \
  -d '{"jobId": "<JOB_ID>"}'
```

### 通过 CLI 提交 Job

```bash
# 前台提交并持续观察（客户端退出会取消任务）
docker exec seatunnel-client ./bin/seatunnel.sh \
  -c ./config/mysql_to_clickhouse_marathon_2.conf -m cluster

# 提交后客户端立即退出，任务留在集群
docker exec -T seatunnel-client ./bin/seatunnel.sh \
  -c ./config/mysql_to_clickhouse_marathon_2.conf -m cluster --async

# 或退出客户端但不取消任务
docker exec seatunnel-client ./bin/seatunnel.sh \
  -c ./config/mysql_to_clickhouse_marathon_2.conf -m cluster -cj false
```

## 更新插件后重新部署

```bash
cd /mnt/www/seatunnel-transform-fieldconvert
mvn clean package -DskipTests -q

JAR="seatunnel-transform-fieldconvert-2.3.13.jar"
for c in seatunnel_master seatunnel_worker_1 seatunnel_worker_2 seatunnel-client; do
  docker cp target/$JAR $c:/opt/seatunnel/lib/$JAR
done

docker restart seatunnel_master seatunnel_worker_1 seatunnel_worker_2
```

## 运行版本

- SeaTunnel: 2.3.13
- Java: 8
