# run_pipeline.py — FlinkForge v3 — CLEAN. PROFESSIONAL. SCALABLE.
from pathlib import Path
from pyflink.table import EnvironmentSettings, TableEnvironment
from src.core.table_factory import (
    load_yaml, create_source_ddl, create_sink_ddl
)

CONFIG_DIR = Path("/workspace/config")
SOURCES_DIR = CONFIG_DIR / "sources"
TARGETS_DIR = CONFIG_DIR / "targets"
PIPELINES_DIR = CONFIG_DIR / "pipelines"


def main(pipeline_file: str):
    pipeline_path = Path(pipeline_file)
    if not pipeline_path.exists():
        raise FileNotFoundError(f"Pipeline not found: {pipeline_file}")

    pipeline = load_yaml(pipeline_path)

    # Use batch mode if you want bounded execution (faster for files)
    env_settings = EnvironmentSettings.new_instance().in_batch_mode().build()
    t_env = TableEnvironment.create(env_settings)

    print(f"\nStarting FlinkForge v3 → {pipeline_file}\n")

    # Create sources
    for src_name in pipeline["sources"]:
        src_config = load_yaml(SOURCES_DIR / f"{src_name}.yaml")
        ddl = create_source_ddl(src_config)
        print(f"Creating source: {src_name}")
        print(ddl)
        t_env.execute_sql(ddl)

    # Create sinks
    for sink_name in pipeline["targets"]:
        sink_config = load_yaml(TARGETS_DIR / f"{sink_name}.yaml")
        ddl = create_sink_ddl(sink_config)
        print(f"Creating sink: {sink_name}")
        print(ddl)
        t_env.execute_sql(ddl)

    # Execute pipeline SQL
    sqls = pipeline["pipeline_sql"]
    if isinstance(sqls, str):
        sqls = [sqls]

    print(f"\nEXECUTING {len(sqls)} PIPELINE STATEMENT(S)...\n")
    for i, sql in enumerate(sqls, 1):
        print(f"--- Statement {i} ---")
        print(sql.strip())
        t_env.execute_sql(sql.strip()).wait()

    print("\nFlinkForge v3 — DONE. Clean. Scalable. Beautiful.")


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python run_pipeline.py <pipeline.yaml>")
        sys.exit(1)
    main(sys.argv[1])