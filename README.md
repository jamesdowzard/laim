# l-**AI**-m (liam)

A collection of Claude Code plugins for data engineering, cloud workflows, and developer productivity.

## Installation

### Step 1: Add the Marketplace

```shell
/plugin marketplace add liam-machine/laim
```

### Step 2: Install a Plugin

**Option A - Browse and install interactively:**
```shell
/plugin
```
Then select "Browse Plugins" and choose the plugin to install.

**Option B - Install directly:**
```shell
/plugin install databricks-executor@liam-machine/laim
```

### Step 3: Choose Installation Scope

When prompted, select a scope:

| Scope | Location | Use Case |
|-------|----------|----------|
| `user` | `~/.claude/settings.json` | Available in all projects |
| `project` | `.claude/settings.json` | Shared with team via git |
| `local` | `.claude/settings.local.json` | Personal, gitignored |

### Step 4: Restart Claude Code

Restart or start a new session to activate the plugin.

## Skills

Skills are auto-triggered based on context - Claude automatically uses them when relevant.

<details>
<summary><strong>databricks-executor</strong> - Execute code on Databricks clusters</summary>

### databricks-executor

Execute Python, SQL, Scala, or R code on Databricks clusters via the Execution Context API.

| Feature | Description |
|---------|-------------|
| Languages | Python, SQL, Scala, R |
| Auth | CLI args, env vars, `~/.databrickscfg` |
| Output | text, JSON, CSV |
| Modes | Single command, file execution, REPL |

**Quick Start:**
```bash
# Set credentials
export DATABRICKS_HOST="https://adb-xxx.azuredatabricks.net"
export DATABRICKS_TOKEN="dapi-xxx"
export DATABRICKS_CLUSTER_ID="cluster-id"

# Execute code
python3 ${CLAUDE_PLUGIN_ROOT}/skills/databricks-executor/scripts/databricks_exec.py -c "print(spark.version)"
```

See [configuration guide](databricks-executor-plugin/skills/databricks-executor/references/configuration.md) for detailed setup.

</details>

## Slash Commands

Slash commands are invoked explicitly with `/<command-name>`.

<details>
<summary><strong>repo-creator</strong> - Create and publish repos with <code>/repo</code></summary>

### repo-creator

Quickly create and publish repositories with the `/repo` command. Supports personal and work directories, GitHub publishing, and virtual environment setup.

| Feature | Description |
|---------|-------------|
| Usage | `/repo <repo-name> [work]` |
| GitHub | Automatic publishing via `gh` CLI |
| Environments | venv, conda, npm |
| Config | `~/.claude/repo-creator-config.json` |

**Quick Start:**
```bash
# Create a personal repo
/repo my-new-project

# Create a work repo
/repo my-work-project work
```

On first run, you'll be prompted to configure your personal and work project directories.

</details>

## License

MIT
