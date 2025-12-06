# run_pipeline.py — FLINKFORGE V3 — BULLETPROOF FINAL (Dec 6 2025)

import sys
from pathlib import Path
import yaml
from pyflink.table import EnvironmentSettings, TableEnvironment
from src.core.table_factory import create_source_table, create_sink_table

CONFIG_DIR    = Path("/workspace/config")
SOURCES_DIR   = CONFIG_DIR / "sources"
TARGETS_DIR   = CONFIG_DIR / "targets"
PIPELINES_DIR = CONFIG_DIR / "pipelines"

def load_yaml(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def main(pipeline_file: str) -> None:
    pipeline_path = Path(pipeline_file)
    if not pipeline_path.exists():
        print(f"Pipeline not found: {pipeline_file}")
        sys.exit(1)

    pipeline = load_yaml(pipeline_path)

    env_settings = EnvironmentSettings.new_instance().in_batch_mode().build()
    t_env = TableEnvironment.create(env_settings)

    print(f"\nFlinkForge v3 → {pipeline_path.name}")
    print("=" * 70)

    # SOURCES
    for src_name in pipeline.get("sources", []):
        src_path = SOURCES_DIR / f"{src_name}.yaml"
        src_config = load_yaml(src_path)
        table_name = src_config.get("table_name", src_name)  # fallback!
        print(f"Creating source → {table_name}")
        src_config["table_name"] = table_name  # ensure it's set
        create_source_table(t_env, src_config)

    # SINKS
    for sink_name in pipeline.get("targets", []):
        sink_path = TARGETS_DIR / f"{sink_name}.yaml"
        sink_config = load_yaml(sink_path)
        table_name = sink_config.get("table_name", sink_name)
        print(f"Creating sink   → {table_name}")
        sink_config["table_name"] = table_name
        create_sink_table(t_env, sink_config)

    # PIPELINE SQL
    sqls = pipeline["pipeline_sql"]
    if isinstance(sqls, str):
        sqls = [sqls]

    print(f"\nExecuting {len(sqls)} statement(s):\n")
    for i, sql in enumerate(sqls, 1):
        print(f"Statement {i}")
        print("-" * 60)
        print(sql.strip())
        print("-" * 60)
        t_env.execute_sql(sql.strip()).wait()

    print("\nFLINKFORGE V3 — SUCCESS. ROWS LANDED. GO CHECK POSTGRES.")
    print("=" * 70)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python run_pipeline.py <pipeline.yaml>")
        sys.exit(1)
    main(sys.argv[1])