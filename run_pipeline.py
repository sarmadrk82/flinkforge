# run_pipeline.py — FINAL: Fully generic, connector-driven, multi-target
import os
import yaml
import requests
from pathlib import Path
from pyflink.table import EnvironmentSettings, TableEnvironment

CONFIG_DIR = Path(__file__).parent / "config"
OLLAMA_AVAILABLE = False

try:
    requests.get("http://localhost:11434/api/tags", timeout=2)
    OLLAMA_AVAILABLE = True
except:
    pass

def ai_convert(prompt: str, model: str = "llama3.1:8b") -> str:
    if not OLLAMA_AVAILABLE:
        return ""
    try:
        resp = requests.post("http://localhost:11434/api/generate", json={
            "model": model, "prompt": prompt, "stream": False,
            "options": {"temperature": 0.0}
        }, timeout=15)
        resp.raise_for_status()
        return resp.json()["response"].strip()
    except:
        return ""

def convert_schema_with_ai(schema_list: list) -> str:
    raw = "\n".join(f"- `{f['name']}`: {f.get('type','STRING')} {'NOT NULL' if not f.get('nullable',True) else ''}"
                    for f in schema_list)
    prompt = f"Convert to Flink SQL columns (backticks, proper types):\n{raw}\nOutput ONLY comma-separated list:"
    result = ai_convert(prompt)
    return result if result and result.count("`") >= len(schema_list) else fallback_schema(schema_list)

def fallback_schema(schema_list: list) -> str:
    # Safe minimal mapping
    lines = []
    for f in schema_list:
        t = str(f.get("type","STRING")).upper()
        if "VARCHAR2" in t or "VARCHAR" in t:
            length = re.search(r"\((\d+)", t)
            length = length.group(1) if length else "100"
            flink_t = f"VARCHAR({length})"
        elif "NUMBER" in t or "DECIMAL" in t or "NUMERIC" in t:
            flink_t = "DECIMAL(18,2)"
        else:
            flink_t = {"DATE":"DATE", "TIMESTAMP":"TIMESTAMP(3)", "INT":"BIGINT"}.get(t.split("(")[0], "STRING")
        not_null = " NOT NULL" if not f.get("nullable", True) else ""
        lines.append(f"  `{f['name']}` {flink_t}{not_null}")
    return ",\n".join(lines)

def build_with_clause(cfg: dict) -> str:
    connector = cfg["connector"]
    if connector == "filesystem":
        return f"'connector' = 'filesystem',\n'path' = '{cfg['path']}',\n'format' = '{cfg.get('format','csv')}'"
    elif connector == "kafka":
        props = cfg.get("properties", {})
        props_str = ",\n".join(f"'{k}' = '{v}'" for k,v in props.items())
        return f"'connector' = 'kafka',\n'topic' = '{cfg['topic']}',\n'format' = '{cfg.get('format','json')}',\n{props_str}"
    elif connector == "jdbc":
        conn = cfg["connection"]
        return f"'connector' = 'jdbc',\n'url' = '{conn['url']}',\n'table-name' = '{conn['table_name']}',\n'username' = '{conn['username']}',\n'password' = '{conn['password']}'"
    else:
        # Fallback: assume all keys are WITH options
        opts = cfg.get("with", cfg)
        return ",\n".join(f"'{k}' = '{v}'" for k,v in opts.items() if k not in ["name","connector","schema","connection"])

def main(pipeline_file: str):
    pipeline = yaml.safe_load(Path(pipeline_file).read_text())
    
    sources = [yaml.safe_load((CONFIG_DIR / "sources" / f"{s}.yaml").read_text()) for s in pipeline["sources"]]
    targets = [yaml.safe_load((CONFIG_DIR / "targets" / f"{t}.yaml").read_text()) for t in pipeline["targets"]]
    
    t_env = TableEnvironment.create(EnvironmentSettings.new_instance().in_batch_mode().build())

    # Create all sources
    for src in sources:
        columns = convert_schema_with_ai(src["schema"])
        with_clause = build_with_clause(src)
        ddl = f"CREATE TEMPORARY TABLE `{src['name']}` (\n{columns}\n) WITH (\n{with_clause}\n);"
        print(f"Creating source: `{src['name']}` ({src['connector']})")
        t_env.execute_sql(ddl)

    # Create all targets
    for tgt in targets:
        columns = convert_schema_with_ai(tgt["schema"])
        with_clause = build_with_clause(tgt)
        ddl = f"CREATE TEMPORARY TABLE `{tgt['name']}` (\n{columns}\n) WITH (\n{with_clause}\n);"
        print(f"Creating target: `{tgt['name']}` ({tgt['connector']})")
        t_env.execute_sql(ddl)

    # Execute pipeline_sql array (1:1 with targets)
    sql_statements = pipeline["pipeline_sql"]
    if not isinstance(sql_statements, list):
        sql_statements = [sql_statements]

    if len(sql_statements) != len(targets):
        raise ValueError(f"pipeline_sql has {len(sql_statements)} statements but {len(targets)} targets")

    for i, raw_sql in enumerate(sql_statements):
        final_sql = raw_sql.strip().rstrip(";")
        if OLLAMA_AVAILABLE:
            final_sql = ai_convert(f"Convert to Flink SQL (backtick tables):\n{raw_sql}", model="defog/sqlcoder-7b-2") or final_sql
        print(f"\nExecuting → `{targets[i]['name']}`")
        print(final_sql)
        t_env.execute_sql(final_sql).wait()

    print(f"\nPipeline '{pipeline['name']}' completed successfully!")

if __name__ == "__main__":
    import sys, re
    pipeline = sys.argv[1] if len(sys.argv) > 1 else "config/pipelines/transaction_enrichment.yaml"
    main(pipeline)


