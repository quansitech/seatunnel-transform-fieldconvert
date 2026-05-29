---
name: generating-seatunnel-mysql-to-clickhouse-config
description: Use when generating SeaTunnel MySQL CDC to ClickHouse sync config from database schema. Triggers include "generate sync config", "mysql to clickhouse", "CDC pipeline config", or any task requiring analysis of MySQL columns to produce a complete HOCON config with source table-names, FieldConvert transform rules, and ClickHouse sink.
---

# Generating SeaTunnel MySQL CDC to ClickHouse Config

## Overview

Query MySQL schema to auto-generate a complete SeaTunnel HOCON config file: source (table-names excluding views), FieldConvert transform rules (timestamp conversion + tinyint cast), and ClickHouse sink. Count-based verification ensures zero omissions.

## Prerequisites

- MySQL connection: host, port, database name, credentials
- ClickHouse connection: host, port, database name, credentials
- SeaTunnel server-id (unique per CDC instance)

## Exclude Framework Tables

**Mandatory**: These framework/system tables must NOT be included in sync config. They belong to the application framework (e.g. QSCMF) and have no business value in ClickHouse.

```
migrations, qs_access, qs_action, qs_action_node, qs_addons,
qs_config, qs_cross_api_register, qs_hooks, qs_menu, qs_node,
qs_queue, qs_schedule
```

All SQL queries below MUST add: `AND c.TABLE_NAME NOT IN ('migrations','qs_access','qs_action','qs_action_node','qs_addons','qs_config','qs_cross_api_register','qs_hooks','qs_menu','qs_node','qs_queue','qs_schedule')`

If working with a non-QSCMF project, ask the user which tables to exclude.

## Core Process

### Step 1: List Business Tables (for source table-names)

```sql
SELECT DISTINCT c.TABLE_NAME
FROM information_schema.COLUMNS c
LEFT JOIN information_schema.VIEWS v
  ON c.TABLE_SCHEMA = v.TABLE_SCHEMA AND c.TABLE_NAME = v.TABLE_NAME
WHERE c.TABLE_SCHEMA = '<database>'
  AND v.TABLE_NAME IS NULL
  AND c.TABLE_NAME NOT IN ('migrations','qs_access','qs_action','qs_action_node','qs_addons','qs_config','qs_cross_api_register','qs_hooks','qs_menu','qs_node','qs_queue','qs_schedule')
ORDER BY c.TABLE_NAME;
```

Generate `table-names = ["<database>.table1", "<database>.table2", ...]`.

**Mandatory**: Exclude views AND framework tables.

### Step 2: Count Tables with Timestamp Fields (verification target)

```sql
SELECT COUNT(DISTINCT c.TABLE_NAME) as total_tables
FROM information_schema.COLUMNS c
LEFT JOIN information_schema.VIEWS v
  ON c.TABLE_SCHEMA = v.TABLE_SCHEMA AND c.TABLE_NAME = v.TABLE_NAME
WHERE c.TABLE_SCHEMA = '<database>'
  AND v.TABLE_NAME IS NULL
  AND c.TABLE_NAME NOT IN ('migrations','qs_access','qs_action','qs_action_node','qs_addons','qs_config','qs_cross_api_register','qs_hooks','qs_menu','qs_node','qs_queue','qs_schedule')
  AND c.DATA_TYPE IN ('int', 'bigint', 'tinyint', 'smallint', 'mediumint', 'decimal')
  AND (c.COLUMN_NAME LIKE '%date%' OR c.COLUMN_NAME LIKE '%time%');
```

**Save this number.** It is the verification target for Step 7.

### Step 3: List Timestamp Fields Grouped by Table

```sql
SELECT c.TABLE_NAME,
       c.COLUMN_NAME,
       c.DATA_TYPE,
       c.COLUMN_COMMENT
FROM information_schema.COLUMNS c
LEFT JOIN information_schema.VIEWS v
  ON c.TABLE_SCHEMA = v.TABLE_SCHEMA AND c.TABLE_NAME = v.TABLE_NAME
WHERE c.TABLE_SCHEMA = '<database>'
  AND v.TABLE_NAME IS NULL
  AND c.TABLE_NAME NOT IN ('migrations','qs_access','qs_action','qs_action_node','qs_addons','qs_config','qs_cross_api_register','qs_hooks','qs_menu','qs_node','qs_queue','qs_schedule')
  AND c.DATA_TYPE IN ('int', 'bigint', 'tinyint', 'smallint', 'mediumint', 'decimal')
  AND (c.COLUMN_NAME LIKE '%date%' OR c.COLUMN_NAME LIKE '%time%')
ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION;
```

For each field, apply this decision:

```
Is DATA_TYPE numeric (int/decimal/etc)?
  YES → Does COLUMN_NAME contain "date" or "time"?
    YES → Is COLUMN_COMMENT clearly a count/number (e.g. "times", "count", "次数")?
      YES → SKIP (not a timestamp)
      NO  → INCLUDE → method = "unix_timestamp_to_datetime", timezone = "Asia/Shanghai"
    NO  → SKIP
  NO  → SKIP (already datetime/timestamp/varchar)
```

### Step 4: Detect Tinyint Fields Needing Cast

```sql
SELECT c.TABLE_NAME, c.COLUMN_NAME, c.COLUMN_COMMENT
FROM information_schema.COLUMNS c
LEFT JOIN information_schema.VIEWS v
  ON c.TABLE_SCHEMA = v.TABLE_SCHEMA AND c.TABLE_NAME = v.TABLE_NAME
WHERE c.TABLE_SCHEMA = '<database>'
  AND v.TABLE_NAME IS NULL
  AND c.TABLE_NAME NOT IN ('migrations','qs_access','qs_action','qs_action_node','qs_addons','qs_config','qs_cross_api_register','qs_hooks','qs_menu','qs_node','qs_queue','qs_schedule')
  AND c.DATA_TYPE = 'tinyint'
  AND c.COLUMN_NAME LIKE 'is_%'
ORDER BY c.TABLE_NAME, c.ORDINAL_POSITION;
```

Fields like `is_general`, `is_test`, `is_broken` → `method = "cast"`, `target_type = "int"`.

ClickHouse doesn't have unsigned tinyint(1) boolean. Casting to int ensures compatibility.

### Step 5: Detect Tables Without Primary Keys

```sql
SELECT t.TABLE_NAME
FROM information_schema.TABLES t
LEFT JOIN information_schema.VIEWS v
  ON t.TABLE_SCHEMA = v.TABLE_SCHEMA AND t.TABLE_NAME = v.TABLE_NAME
LEFT JOIN (
    SELECT DISTINCT TABLE_SCHEMA, TABLE_NAME
    FROM information_schema.COLUMNS
    WHERE COLUMN_KEY = 'PRI' AND TABLE_SCHEMA = '<database>'
) pk ON t.TABLE_SCHEMA = pk.TABLE_SCHEMA AND t.TABLE_NAME = pk.TABLE_NAME
WHERE t.TABLE_SCHEMA = '<database>'
  AND t.TABLE_TYPE = 'BASE TABLE'
  AND t.TABLE_NAME NOT IN ('migrations','qs_access','qs_action','qs_action_node','qs_addons','qs_config','qs_cross_api_register','qs_hooks','qs_menu','qs_node','qs_queue','qs_schedule')
  AND pk.TABLE_NAME IS NULL
ORDER BY t.TABLE_NAME;
```

**Warning**: Tables without PKs cannot use `ReplacingMergeTree` with `${rowtype_primary_key}`. Options:
1. Pre-create these tables in ClickHouse manually with a suitable ORDER BY
2. Use a separate sink with `MergeTree` engine and `ORDER BY tuple()`
3. Skip these tables from CDC sync

### Step 6: Generate Complete Config

Output a HOCON config with three sections:

**Source**: Use `table-names` (from Step 1), NOT `table-pattern`.
```hocon
source {
  MySQL-CDC {
    plugin_output = "mysql_<database>"
    username = "<mysql_user>"
    password = "<mysql_password>"
    base-url = "jdbc:mysql://<mysql_host>:<mysql_port>"
    server-id = <unique_id>
    table-names = [
      "<database>.table1",
      "<database>.table2"
    ]
  }
}
```

**Transform**: Combine rules from Steps 3 and 4.
```hocon
transform {
  FieldConvert {
    plugin_input = "mysql_<database>"
    plugin_output = "transformed_<database>"
    rules = [
      {
        table_pattern = "<database>.table1"
        field = "create_time"
        method = "unix_timestamp_to_datetime"
        timezone = "Asia/Shanghai"
      },
      {
        table_pattern = "<database>.table2"
        field = "is_active"
        method = "cast"
        target_type = "int"
      }
    ]
  }
}
```

**Sink**: ClickHouse with ReplacingMergeTree.
```hocon
sink {
  Clickhouse {
    plugin_input = "transformed_<database>"
    host = "<clickhouse_host>:8123"
    database = "<database>"
    table = "\${table_name}"
    username = "<ck_user>"
    password = "<ck_password>"
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"

    # Dynamic placeholders - auto-resolved per table from upstream schema
    # \${primary_key} = actual PK column(s) from MySQL, enables CDC UPDATE/DELETE
    primary_key = "\${primary_key}"

    save_mode_create_template = """
CREATE TABLE IF NOT EXISTS `\${database}`.`\${table}` (
\${rowtype_primary_key},
\${rowtype_fields}
) ENGINE = ReplacingMergeTree()
ORDER BY (\${rowtype_primary_key})
PRIMARY KEY (\${rowtype_primary_key})
SETTINGS index_granularity = 8192
"""
    allow_experimental_lightweight_delete = true
    clickhouse.config = {
      enable_http_compression = "0"
      compress = "0"
    }
  }
}
```

### Step 7: Verify Count

Count distinct `table_pattern` values in generated transform rules. Must equal Step 2 count.

If mismatch: check for missing tables, re-query, and fix.

## Mandatory Checks

| Check | Why |
|-------|-----|
| Exclude framework tables from all queries | No business value, wastes resources and may cause errors |
| Match both `%date%` AND `%time%` | `create_time` won't match `%date%` alone |
| Exclude views via `LEFT JOIN VIEWS ... WHERE v.TABLE_NAME IS NULL` | CDC doesn't capture views |
| Use `table-names` not `table-pattern` | `table-pattern` may include views |
| Verify transform table count vs Step 2 | Catches omissions before deployment |
| Check field comments for ambiguous names | `donated_times` is a count, not a timestamp |
| Detect tables without primary keys | ReplacingMergeTree template fails without PK |

## Common Mistakes

| Mistake | What Happens | Fix |
|---------|-------------|-----|
| Only matching `%date%` | Misses all `*_time` fields | Always use both patterns |
| Using `table-pattern = "db\\..*"` | Includes views, fails at sink | Use explicit `table-names` |
| Skipping count verification | Silent omissions found only at runtime | Count must match |
| Not checking field comments | `donated_times` (count) treated as timestamp | Review COLUMN_COMMENT |
| Ignoring tables without PK | ReplacingMergeTree CREATE fails | Pre-create or use different engine |
