import yaml
from pathlib import Path
from typing import Dict, List

def load_yaml(file_path: Path) -> Dict:
    return yaml.safe_load(file_path.read_text())

def build_with_clause(options: dict) -> str:
    """
    Generates proper Flink SQL WITH clause with required single quotes.
    Handles nested options like 'csv.ignore-parse-errors'.
    """
    lines = []
    for k, v in options.items():
        if isinstance(v, dict):
            for sk, sv in v.items():
                lines.append(f"  '{k}.{sk}' = '{sv}'")
        else:
            lines.append(f"  '{k}' = '{v}'")
    return ",\n".join(lines) + ("\n" if lines else "")
    
def create_source_ddl(src_config: Dict) -> str:
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
    
    return f"""CREATE TABLE {name} (
{column_sql}
) WITH (
{with_clause}
);"""

def create_sink_ddl(sink_config: Dict) -> str:
    name = sink_config["name"]
    print(f"Creating sink: `{name}` (connector: {sink_config.get('connector', 'unknown')})")
    
    schema = sink_config.get("schema", [])
    if not schema:
        raise ValueError(f"Sink `{name}` must have schema defined in YAML")
        
    cols = [f"  `{col['name']}` {col.get('type', 'STRING').upper()}" for col in schema]
    column_sql = ",\n".join(cols)
    
    with_options = {"connector": sink_config["connector"]}
    if "connection" in sink_config:
        conn = sink_config["connection"]
        for k, v in conn.items():
            with_options["table-name" if k == "table_name" else k] = v
    for k, v in sink_config.items():
        if k not in ["name", "schema", "connection", "connector"]:
            with_options[k] = v

    with_clause = build_with_clause(with_options)
    
    return f"""CREATE TABLE {name} (
{column_sql}
) WITH (
{with_clause}
);"""
