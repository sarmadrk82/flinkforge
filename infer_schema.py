# infer_schema.py — FINAL WORKING VERSION (AI schema inference)
import pandas as pd
import yaml
from pathlib import Path
import sys
import ollama

# THIS IS THE ONLY LINE THAT MATTERS
client = ollama.Client(host='http://ollama:11434')   # ← forces connection to our container

def infer_with_ai(df: pd.DataFrame, table_name: str):
    prompt = f"""
You are a senior data engineer. Generate a perfect Flink source YAML schema for this table: `{table_name}`

Sample data (first 5 rows):
{df.head(5).to_csv(index=False)}

Rules:
- Output ONLY valid YAML
- Use backticks around column names
- Use proper Flink types: VARCHAR(n), BIGINT, BOOLEAN, DECIMAL(18,6), TIMESTAMP
- Estimate safe VARCHAR lengths
- No markdown, no explanations

Example:
schema:
  - name: id
    type: BIGINT
  - name: email
    type: VARCHAR(100)
"""

    try:
        response = client.generate(model='llama3.2:3b', prompt=prompt)
        yaml_text = response['response'].strip()

        # Clean AI garbage
        if "```yaml" in yaml_text:
            yaml_text = yaml_text.split("```yaml", 1)[1].split("```", 1)[0]
        elif "```" in yaml_text:
            yaml_text = yaml_text.split("```", 1)[1].rsplit("```", 1)[0]

        return yaml.safe_load(yaml_text)
    except Exception as e:
        print("AI failed, using pandas fallback...")
        schema = []
        for col in df.columns:
            if pd.api.types.is_integer_dtype(df[col]):
                t = "BIGINT"
            elif pd.api.types.is_float_dtype(df[col]):
                t = "DECIMAL(18,6)"
            elif pd.api.types.is_bool_dtype(df[col]):
                t = "BOOLEAN"
            else:
                length = max(50, int(df[col].astype(str).str.len().max() * 1.3))
                t = f"VARCHAR({length})"
            schema.append({"name": col, "type": t})
        return {"schema": schema}

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python infer_schema.py <file.csv> [table_name]")
        sys.exit(1)

    file_path = sys.argv[1]
    table_name = sys.argv[2] if len(sys.argv) > 2 else Path(file_path).stem

    print(f"Inferring schema for {table_name} using Llama 3.2...")
    df = pd.read_csv(file_path)
    schema_dict = infer_with_ai(df, table_name)

    config = {
        "name": table_name,
        "connector": "filesystem",
        "path": f"file:///workspace/data/{Path(file_path).name}",
        "format": "csv",
        "format_options": {
            "csv.ignore-parse-errors": "true",
            "csv.field-delimiter": ","
        },
        "schema": schema_dict.get("schema", schema_dict)
    }

    out_path = Path("/workspace/config/sources") / f"{table_name}.yaml"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(yaml.dump(config, sort_keys=False))
    print(f"Success! Saved to {out_path}")

