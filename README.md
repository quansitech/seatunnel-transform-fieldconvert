# SeaTunnel Transform Plugin: FieldConvert

SeaTunnel 自定义 Transform 插件，支持在数据同步管道中对指定表的指定字段进行类型转换，适用于 MySQL CDC 等场景下源端与目标端字段类型不一致的情况。

## 功能特性

- **按表按字段灵活配置**：通过 `table_pattern` 匹配表，精确指定需要转换的字段
- **通配符支持**：`table_pattern` 支持 `*` 通配符，如 `donation.*` 匹配整个库
- **多规则叠加**：同一张表可命中多条规则，分别转换不同字段
- **零侵入**：不命中的字段和表原样透传，不影响其他数据

## 支持的转换方法

| method | 说明 | 额外参数 |
|--------|------|----------|
| `unix_timestamp_to_datetime` | 将 int/long/decimal 时间戳转换为 LocalDateTime | `timezone`（时区，默认 UTC） |
| `cast` | 数值类型转换（如 tinyint → int） | `target_type`（目标类型） |

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

| 格式 | 含义 | 示例 |
|------|------|------|
| `database.table` | 精确匹配 | `donation.qs_book_info` |
| `database.*` | 匹配该库下所有表 | `donation.*` |
| `*.*` 或省略 | 匹配所有表 | — |

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

Base URL: `http://<master-host>:5801`（集群模式）

### 查看运行中的 Job

```bash
curl -s http://localhost:5801/hazelcast/rest/maps/running-jobs
```

简洁输出：

```bash
curl -s http://localhost:5801/hazelcast/rest/maps/running-jobs \
  | python3 -c "import sys,json
for j in json.load(sys.stdin):
    print(j['jobId'], j['jobStatus'], j.get('jobName',''))"
```

### 查看指定 Job 详情

```bash
curl -s http://localhost:5801/hazelcast/rest/maps/running-job/<JOB_ID>
```

### 查看已完成的 Job

```bash
curl -s http://localhost:5801/hazelcast/rest/maps/finished-jobs
```

### 停止 Job

```bash
curl -s -X POST http://localhost:5801/hazelcast/rest/maps/stop-job \
  -H "Content-Type: application/json" \
  -d '{"jobId": "<JOB_ID>"}'
```

### 通过 REST API 提交 Job

```bash
curl -s -X POST http://localhost:5801/hazelcast/rest/maps/submit-job \
  -H "Content-Type: application/json" \
  -d '{
    "env": { "job.name": "my-job", "job.mode": "STREAMING" },
    "source": [{ ... }],
    "transform": { ... },
    "sink": [{ ... }]
  }'
```

### 通过 CLI 提交 Job（HOCON 配置文件，推荐）

```bash
docker exec seatunnel-client ./bin/seatunnel.sh \
  -c ./config/mysql_to_clickhouse_marathon_2.conf -m cluster
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
