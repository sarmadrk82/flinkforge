# run_pipeline.py — FlinkForge v2.1 — CLEAN. GENERIC. PROFESSIONAL.

import yaml
from pathlib import Path
from pyflink.table import EnvironmentSettings, TableEnvironment


CONFIG_DIR = Path("/workspace/config")
SOURCES_DIR = CONFIG_DIR / "sources"
TARGETS_DIR = CONFIG_DIR / "targets"


def load_yaml(file_path: Path):
    return yaml.safe_load(file_path.read_text())


def build_with_clause(options: dict) -> str:
    lines = []
    for k, v in options.items():
        if isinstance(v, dict):
            for sk, sv in v.items():
                lines.append(f"  '{k}.{sk}' = '{sv}'")
        else:
            lines.append(f"  '{k}' = '{v}'")
    return ",\n".join(lines)


def create_source(t_env, src_config):
    name = src_config["name"]
    print(f"Creating source: `{name}`")

    cols = []
    for col in src_config.get("schema", []):
        col_name = col["name"]
        col_type = col.get("type", "STRING").upper()
        cols.append(f"  `{col_name}` {col_type}")

    if not cols:
        raise ValueError(f"Source `{name}` has no schema defined")

    column_sql = ",\n".join(cols)

    with_options = {"connector": src_config["connector"]}
    if "path" in src_config:
        with_options["path"] = src_config["path"]
    if "format" in src_config:
        with_options["format"] = src_config["format"]
    if "format_options" in src_config:
        with_options.update(src_config["format_options"])

    with_clause = build_with_clause(with_options)

    sql = f"""CREATE TABLE {name} (
{column_sql}
) WITH (
{with_clause}
);"""

    print(sql)
    t_env.execute_sql(sql)

def create_sink(t_env, sink_config):
    name = sink_config["name"]
    print(f"Creating sink: `{name}` (connector: {sink_config.get('connector', 'unknown')})")

    # Use schema from YAML — if present
    schema = sink_config.get("schema", [])
    if not schema:
        raise ValueError(f"Sink `{name}` must have schema defined in YAML")

    cols = []
    for col in schema:
        col_name = col["name"]
        col_type = col.get("type", "STRING").upper()
        cols.append(f"  `{col_name}` {col_type}")

    column_sql = ",\n".join(cols)
    print(f"  Using schema from YAML ({len(schema)} columns)")

    # Build WITH clause — fully generic
    with_options = {}
    if "connector" in sink_config:
        with_options["connector"] = sink_config["connector"]
    if "connection" in sink_config:
        conn = sink_config["connection"]
        for k, v in conn.items():
            with_options["table-name" if k == "table_name" else k] = v
    for k, v in sink_config.items():
        if k not in ["name", "schema", "connection", "connector"]:
            with_options[k] = v

    with_clause = build_with_clause(with_options)

    sql = f"""CREATE TABLE {name} (
{column_sql}
) WITH (
{with_clause}
);"""

    print(sql)
    t_env.execute_sql(sql)





def main(pipeline_file: str):
    pipeline_path = Path(pipeline_file)
    pipeline = load_yaml(pipeline_path)

    t_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())

    # Create all sources
    for src_name in pipeline["sources"]:
        src_config = load_yaml(SOURCES_DIR / f"{src_name}.yaml")
        create_source(t_env, src_config)

    # Create all sinks
    for sink_name in pipeline["targets"]:
        sink_config = load_yaml(TARGETS_DIR / f"{sink_name}.yaml")
        create_sink(t_env, sink_config)

    # Execute pipeline SQL — supports string or list
    pipeline_sqls = pipeline["pipeline_sql"]
    if isinstance(pipeline_sqls, str):
        pipeline_sqls = [pipeline_sqls.strip()]
    else:
        pipeline_sqls = [sql.strip() for sql in pipeline_sqls if sql.strip()]

    if not pipeline_sqls:
        raise ValueError("No pipeline_sql defined")

    print(f"\nEXECUTING {len(pipeline_sqls)} PIPELINE STATEMENT(S)...")
    for i, sql in enumerate(pipeline_sqls, 1):
        print(f"\n--- Statement {i}/{len(pipeline_sqls)} ---")
        print(sql)
        t_env.execute_sql(sql).wait()

    print(f"\nAll {len(pipeline_sqls)} statements executed successfully.")
    print("FlinkForge v2 — IT'S ALIVE. Clean. Generic. Professional.")


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Error: Missing pipeline YAML file")
        print("Usage: python run_pipeline.py <path-to-pipeline.yaml>")
        print("Example: python run_pipeline.py /workspace/config/pipelines/customers_csv_to_crm.yaml")
        sys.exit(1)

    pipeline_file = sys.argv[1]
    if not Path(pipeline_file).exists():
        print(f"Error: Pipeline file not found: {pipeline_file}")
        sys.exit(1)

    print(f"Starting FlinkForge pipeline: {pipeline_file}")
    main(pipeline_file)


