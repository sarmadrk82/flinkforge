# app/core/table_factory.py
# The One True Table Factory — supports ANY source/sink Flink supports
# CSV, JSON, Parquet, Kafka, JDBC, upsert-kafka, print, etc.
# Schema inference only when needed, per connector type
# Zero assumptions, maximum power

import yaml
import ollama
import jaydebeapi
import os
from typing import Dict, List, Any

def _csv_infer_schema(file_path: str, sample_rows: int = 8) -> List[Dict]:
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Sample file not found: {file_path}")
    
    with open(file_path, 'r') as f:
        header = f.readline().strip().split(',')
        samples = [f.readline().strip().split(',') for _ in range(sample_rows)]
    
    context = "\n".join(f"{h}: {', '.join(row[i] for row in samples if i < len(row))}" for i, h in enumerate(header))
    prompt = f"""You are a Flink SQL schema expert.
Infer the correct Flink SQL data types (use VARCHAR(n), BIGINT, INT, DOUBLE, BOOLEAN, TIMESTAMP(3), etc.)
Also infer reasonable lengths for VARCHAR and whether nullable (default true unless all samples non-empty).
Output ONLY valid YAML list of dicts like - name: col, type: TYPE, length: n, nullable: true/false:
No extra text.

{context}
"""
    response = ollama.generate(model='llama3.1:8b', prompt=prompt)['response']
    return yaml.safe_load(response)

def _jdbc_discover_schema(jdbc_url: str, table_name: str, username: str, password: str) -> List[Dict]:
    conn = jaydebeapi.connect(
        "org.postgresql.Driver",
        jdbc_url,
        [username, password],
        "/opt/flink/lib/postgresql-42.7.4.jar"
    )
    curs = conn.cursor()
    curs.execute(f"""
        SELECT column_name, data_type, character_maximum_length, is_nullable = 'YES' as nullable
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        ORDER BY ordinal_position
    """)
    rows = curs.fetchall()
    curs.close()
    conn.close()

    raw = "\n".join(f"{r[0]}: {r[1]} (max_len={r[2] or 'null'}, nullable={r[3]})" for r in rows)
    prompt = f"""Convert this PostgreSQL schema to Flink SQL types (VARCHAR(n), BIGINT, etc.).
Use actual lengths for VARCHAR. Nullable as bool.
Output ONLY clean YAML list of dicts like - name: col, type: TYPE, length: n, nullable: true/false:
No extra text.

{raw}
"""
    response = ollama.generate(model='llama3.1:8b', prompt=prompt)['response']
    return yaml.safe_load(response)

# ← ONLY REPLACE _infer_schema function with this one
def _infer_schema(config: Dict, is_source: bool = True) -> List[Dict]:
    # FIXED: Properly detect explicit schema (list of dicts with name/type)
    if 'schema' in config and isinstance(config['schema'], list) and config['schema']:
        raw_fields = config['schema']
        fields = []
        for f in raw_fields:
            if isinstance(f, dict) and 'name' in f:
                field = {'name': f['name'], 'type': str(f['type']).upper()}
                if 'nullable' in f:
                    field['nullable'] = bool(f['nullable'])
                fields.append(field)
        if fields:
            return fields

    # ← If no explicit schema → then infer (CSV, JDBC, etc.)
    connector = config.get('connector') or config.get('with', {}).get('connector')
    if not connector:
        raise ValueError("No connector specified")

    if connector == 'filesystem':
        format_type = (config.get('format') or config.get('with', {}).get('format', 'csv')).lower()
        path = config.get('path') or config.get('with', {}).get('path')
        if format_type == 'csv' and path:
            path = path.replace('file://', '')
            return _csv_infer_schema(path)
        return []

    elif connector == 'jdbc':
        # ... jdbc logic
        pass

    return []

def _build_columns_ddl(fields: List[Dict]) -> str:
    if not fields:
        return ''
    cols = []
    for f in fields:
        t = f['type'].upper()
        if 'length' in f and 'VAR' in t:
            t += f"({f['length']})"
        null_clause = "NOT NULL" if not f.get('nullable', True) else ""
        cols.append(f"  `{f['name']}` {t} {null_clause}".strip())
    return "(\n" + ",\n".join(cols) + "\n)"

def _build_with_clause(config: Dict) -> str:
    with_opts = config.get('with', {}).copy()

    # 1. Merge top-level connector/path/format
    for key in ['connector', 'path', 'format']:
        if key in config and key not in with_opts:
            with_opts[key] = config[key]
    if 'path' in config:
        with_opts.setdefault('connector', 'filesystem')

    # 2. Merge format_options (CSV, etc.)
    if 'format_options' in config:
        for k, v in config['format_options'].items():
            with_opts[k] = str(v)

    # 3. CRITICAL: Merge connection block for JDBC
    if 'connection' in config:
        conn = config['connection']
        # Direct mapping
        if 'url' in conn:
            with_opts['url'] = conn['url']
        if 'table' in conn:
            with_opts['table-name'] = conn['table']
        elif 'table-name' in conn:
            with_opts['table-name'] = conn['table-name']
        if 'username' in conn:
            with_opts['username'] = conn['username']
        if 'password' in conn:
            with_opts['password'] = conn['password']
        # Copy any other connection props
        for k, v in conn.items():
            if k not in ['url', 'table', 'table-name', 'username', 'password']:
                with_opts[k] = v

    if not with_opts.get('connector'):
        raise ValueError("No connector defined in final WITH clause")

    # FINAL: Always quote keys properly
    items = []
    for k, v in with_opts.items():
        key_str = f"'{k}'"
        value_str = f"'{str(v)}'"
        items.append(f"  {key_str} = {value_str}")

    return ",\n".join(items)


def create_source_table(t_env, source_config: Dict[str, Any]) -> str:
    table_name = source_config['table_name']
    fields = _infer_schema(source_config, is_source=True)
    columns_ddl = _build_columns_ddl(fields)
    with_clause = _build_with_clause(source_config)
    
    ddl = f"""
CREATE TEMPORARY TABLE `{table_name}` {columns_ddl} WITH (
  {with_clause}
)
""".strip()
    
    t_env.execute_sql(ddl)
    return table_name

def create_sink_table(t_env, sink_config: Dict[str, Any]) -> str:
    table_name = sink_config['table_name']
    fields = _infer_schema(sink_config, is_source=False)
    columns_ddl = _build_columns_ddl(fields)
    with_clause = _build_with_clause(sink_config)
    
    ddl = f"""
CREATE TEMPORARY TABLE `{table_name}` {columns_ddl} WITH (
  {with_clause}
)
""".strip()
    
    t_env.execute_sql(ddl)
    return table_name