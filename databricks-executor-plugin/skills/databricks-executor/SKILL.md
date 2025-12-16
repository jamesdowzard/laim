---
name: databricks-executor
description: Execute arbitrary Python, SQL, Scala, or R code on Databricks. For SQL queries, use serverless SQL Warehouses (fast, no cluster needed). For Python/Scala/R, use clusters. Triggers include "run on Databricks", "execute on cluster", "Spark query", "test this on Databricks", "databricks REPL", "run PySpark", "query Delta table", or any request to execute code on a Databricks environment.
---

# Databricks Executor

Execute code on Databricks clusters or SQL Warehouses using multiple backends:
- **SQL Statement API 2.0** - For SQL on serverless SQL Warehouses (fast, no cluster needed)
- **Command Execution API 2.0** - For Python/SQL/Scala/R on clusters

## Quick Start

### Prerequisites

User must have configured Databricks credentials. Check for configuration:

```bash
# Check for config file
cat ~/.databrickscfg 2>/dev/null || echo "No config file"

# Check for environment variables
echo "HOST: ${DATABRICKS_HOST:-not set}"
echo "WAREHOUSE: ${DATABRICKS_WAREHOUSE_ID:-not set}"
echo "CLUSTER: ${DATABRICKS_CLUSTER_ID:-not set}"
```

If not configured, see [references/configuration.md](references/configuration.md) for setup instructions.

### Execute SQL (Serverless - Recommended)

For SQL queries, use the SQL Statement API with a serverless SQL Warehouse:

```bash
# SQL on serverless warehouse (fast, no cluster needed)
python3 ${CLAUDE_PLUGIN_ROOT}/skills/databricks-executor/scripts/databricks_exec.py \
  --profile exploration --warehouse-id "2af142a22622b7e2" --language sql \
  -c "SELECT * FROM catalog.schema.table LIMIT 10"

# If warehouse_id is in your profile, just specify language
python3 ${CLAUDE_PLUGIN_ROOT}/skills/databricks-executor/scripts/databricks_exec.py \
  --profile exploration --language sql -c "SHOW DATABASES"
```

### Execute Python/Scala/R (Cluster Required)

For Python, Scala, or R, you need a running cluster:

```bash
# Python on cluster
python3 ${CLAUDE_PLUGIN_ROOT}/skills/databricks-executor/scripts/databricks_exec.py \
  --profile exploration --cluster-id "abc-123-xyz" -c "print(spark.version)"

# Scala on cluster
python3 ${CLAUDE_PLUGIN_ROOT}/skills/databricks-executor/scripts/databricks_exec.py \
  --profile exploration --cluster-id "abc-123-xyz" --language scala -c "println(spark.version)"
```

## Backend Selection Logic

```
User wants to execute code
├── Language is SQL AND warehouse_id is set?
│   └── YES → SQL Statement API 2.0 (serverless, fast)
│   └── NO  → Command Execution API 2.0 (needs cluster)
├── Language is Python/Scala/R?
│   └── Command Execution API 2.0 (needs cluster)
└── Interactive REPL or file execution?
    └── Command Execution API 2.0 (needs cluster)
```

## Common Patterns

### Run Spark SQL Queries (Serverless)

```bash
# List databases
python3 ${CLAUDE_PLUGIN_ROOT}/skills/databricks-executor/scripts/databricks_exec.py \
  --profile exploration --language sql -c "SHOW DATABASES"

# Query data
python3 ${CLAUDE_PLUGIN_ROOT}/skills/databricks-executor/scripts/databricks_exec.py \
  --profile exploration --language sql \
  -c "SELECT * FROM ai_a.schema.table LIMIT 10"

# Get CSV output
python3 ${CLAUDE_PLUGIN_ROOT}/skills/databricks-executor/scripts/databricks_exec.py \
  --profile exploration --language sql --output-format csv \
  -c "SELECT * FROM table"

# Describe table
python3 ${CLAUDE_PLUGIN_ROOT}/skills/databricks-executor/scripts/databricks_exec.py \
  --profile exploration --language sql \
  -c "DESCRIBE TABLE EXTENDED catalog.schema.table"
```

### Run PySpark Code (Cluster Required)

```bash
# DataFrame operations
python3 ${CLAUDE_PLUGIN_ROOT}/skills/databricks-executor/scripts/databricks_exec.py \
  --cluster-id "abc-123" -c "spark.sql('SELECT 1 as col').show()"

# Read data
python3 ${CLAUDE_PLUGIN_ROOT}/skills/databricks-executor/scripts/databricks_exec.py \
  --cluster-id "abc-123" -c "df = spark.table('catalog.schema.table'); print(df.count())"

# Execute file
python3 ${CLAUDE_PLUGIN_ROOT}/skills/databricks-executor/scripts/databricks_exec.py \
  --cluster-id "abc-123" -f etl_script.py
```

### Use Different Profiles

```bash
# Use named profile (with warehouse_id configured)
python3 ${CLAUDE_PLUGIN_ROOT}/skills/databricks-executor/scripts/databricks_exec.py \
  --profile exploration --language sql -c "SELECT 1"

# Override warehouse on command line
python3 ${CLAUDE_PLUGIN_ROOT}/skills/databricks-executor/scripts/databricks_exec.py \
  --profile exploration --warehouse-id "different-warehouse" --language sql -c "SELECT 1"

# Full override
python3 ${CLAUDE_PLUGIN_ROOT}/skills/databricks-executor/scripts/databricks_exec.py \
  --host "https://workspace.azuredatabricks.net" \
  --token "dapi-xxx" \
  --warehouse-id "warehouse-id" \
  --language sql -c "SELECT 1"
```

### Output Formats

```bash
# Default text output
python3 ... --language sql -c "SELECT * FROM table"

# JSON output (for programmatic use)
python3 ... --language sql --output-format json -c "SELECT * FROM table"

# CSV output (for exports)
python3 ... --language sql --output-format csv -c "SELECT * FROM table" > output.csv
```

## Configuration in ~/.databrickscfg

Add `warehouse_id` to your profile to enable serverless SQL by default:

```ini
[exploration]
host         = https://dbc-xxx.cloud.databricks.com
auth_type    = databricks-cli
warehouse_id = 2af142a22622b7e2
# cluster_id = abc-123-xyz  # Optional, for Python/Scala/R
```

## Error Handling

Exit codes:
- `0` - Success
- `1` - Error (configuration, connection, or execution error)

Errors include:
- Configuration errors (missing host/token/warehouse/cluster)
- Connection errors (network issues)
- Execution errors (SQL errors, displayed with message)

## Resources

- **scripts/databricks_exec.py** - Main execution script
- **references/configuration.md** - Authentication and configuration guide
- **references/api-reference.md** - Databricks API details

## User Setup Requirements

Before using this skill, ensure the user has:

1. **Databricks workspace access** - URL like `https://adb-xxx.azuredatabricks.net`
2. **Personal access token** - Generated from workspace Settings → Developer → Access tokens
3. **For SQL:** SQL Warehouse ID (find in SQL Warehouses → your warehouse → URL)
4. **For Python/Scala/R:** Running cluster with appropriate permissions
5. **Configuration** - Via `~/.databrickscfg`, environment variables, or CLI args

Prompt user to configure if missing:

```
To use Databricks SQL execution (serverless), you need:
1. Your Databricks workspace URL (e.g., https://adb-xxx.azuredatabricks.net)
2. A personal access token (generate in Settings → Developer → Access tokens)
3. A SQL Warehouse ID (find in SQL Warehouses → your warehouse → URL or connection details)

For Python/Scala/R execution, you also need:
4. A cluster ID (find in Compute → your cluster → URL)

Would you like me to help you set up ~/.databrickscfg?
```
