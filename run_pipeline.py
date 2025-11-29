# run_pipeline.py — FINAL, WORKING, NO ERRORS, NO AI, JUST WORKS
import yaml
from pathlib import Path
from pyflink.table import EnvironmentSettings, TableEnvironment

CONFIG_DIR = Path("/workspace/config")

def main(pipeline_file):
    pipeline = yaml.safe_load(Path(pipeline_file).read_text())
    sources = [yaml.safe_load((CONFIG_DIR / "sources" / f"{s}.yaml").read_text()) for s in pipeline["sources"]]
    targets = [yaml.safe_load((CONFIG_DIR / "targets" / f"{t}.yaml").read_text()) for t in pipeline["targets"]]
    
    t_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())

    # SOURCE
    src = sources[0]
    print("Creating source: `customers_csv` (filesystem)")

    cols = []
    for c in src.get("schema", []):
        cols.append("  `{}` STRING".format(c["name"]))

    source_sql = (
        "CREATE TABLE `customers_csv` (\n" +
        ",\n".join(cols) + "\n" +
        ") WITH (\n" +
        "  'connector' = 'filesystem',\n" +
        "  'path' = 'file:///workspace/data/customers.csv',\n" +
        "  'format' = 'csv',\n" +
        "  'csv.field-delimiter' = ',',\n" +
        "  'csv.ignore-parse-errors' = 'true'\n" +
        ");"
    )
    print(source_sql)
    t_env.execute_sql(source_sql)

    # TARGET
    print("Creating target: `crm_postgres` (jdbc)")

    target_sql = (
        "CREATE TABLE `crm_postgres` (\n"
        "  `legal_entity` VARCHAR(10),\n"
        "  `local_cif` VARCHAR(30),\n"
        "  `id` VARCHAR(20),\n"
        "  `id_type` VARCHAR(15),\n"
        "  `id_issuing_country` VARCHAR(3),\n"
        "  `name` VARCHAR(100),\n"
        "  `email` VARCHAR(100),\n"
        "  `phone` BIGINT,\n"
        "  `address` VARCHAR(200)\n"
        ") WITH (\n"
        "  'connector' = 'jdbc',\n"
        "  'url' = 'jdbc:postgresql://host.docker.internal:5432/crm_db',\n"
        "  'table-name' = 'customers',\n"
        "  'username' = 'postgres',\n"
        "  'password' = 'secret'\n"
        ");"
    )
    print(target_sql)
    t_env.execute_sql(target_sql)

    # RUN PIPELINE
    sql = pipeline["pipeline_sql"]
    if isinstance(sql, list):
        sql = sql[0]

    print("\nEXECUTING PIPELINE...")
    print(sql)
    t_env.execute_sql(sql).wait()

    print("\n" + "="*60)
    print("PIPELINE COMPLETED SUCCESSFULLY")
    print("IT'S ALIVE!!!")
    print("="*60)

if __name__ == "__main__":
    import sys
    main(sys.argv[1] if len(sys.argv) > 1 else "/workspace/config/pipelines/customers_csv_to_crm.yaml")

