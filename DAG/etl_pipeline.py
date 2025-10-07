from __future__ import annotations
import os, glob, shutil
from pathlib import Path
from datetime import datetime

import pandas as pd
import yaml
import pendulum
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from airflow.decorators import dag, task
from airflow import AirflowException

tz = pendulum.timezone("America/Chicago")

# load env
load_dotenv("/opt/airflow/.env") or load_dotenv(".env")

DATA_DIR    = os.getenv("DATA_DIR", "/opt/airflow/data")
ARCHIVE_DIR = os.getenv("ARCHIVE_DIR", "/opt/airflow/data_archive")

PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASSWORD")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB   = os.getenv("PG_DB")
ENGINE_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"

EXPECTED = [
    "customers.csv","customers.json","customers.yaml",
    "products.csv","products.json","products.yaml",
    "orders.csv","orders.json","orders.yaml",
]

default_args = {"owner": "oscar", "retries": 0}

@dag(
    dag_id="etl_pipeline",
    schedule="0 2 * * *",   # runs daily at 2:00 AM CT
    start_date=pendulum.datetime(2025, 1, 1, tz=tz),  
    catchup=False,           
    tags=["etl","mvp"]
)
def etl_pipeline():

    
    @task
    def file_validation():
        missing = []
        for name in EXPECTED:
            p = Path(DATA_DIR) / name
            if not p.exists() or p.stat().st_size == 0:
                missing.append(str(p))
        if missing:
            raise AirflowException(f"Missing/empty files: {missing}")

        
        for n in ["customers.csv","products.csv","orders.csv"]:
            _ = pd.read_csv(Path(DATA_DIR)/n).head(1)
        for n in ["customers.json","products.json","orders.json"]:
            _ = pd.read_json(Path(DATA_DIR)/n, orient="records").head(1)
        for n in ["customers.yaml","products.yaml","orders.yaml"]:
            with open(Path(DATA_DIR)/n, "r", encoding="utf-8") as f:
                items = yaml.safe_load(f)
                if not isinstance(items, list) or not items:
                    raise AirflowException(f"Bad YAML structure: {n}")
        return "files_ok"


    @task
    def db_setup():
        engine = create_engine(ENGINE_URL)
        schema_path = Path("/opt/airflow/sql/schema.sql")
        if not schema_path.exists():
            
            schema_path = Path.cwd().parent / "sql" / "schema.sql"
        ddl = schema_path.read_text(encoding="utf-8")

        #create table statements
        with engine.begin() as conn:
            for stmt in [s.strip() for s in ddl.split(";") if s.strip()]:
                conn.execute(text(stmt))
            conn.execute(text("TRUNCATE TABLE orders RESTART IDENTITY CASCADE"))
            conn.execute(text("TRUNCATE TABLE products RESTART IDENTITY CASCADE"))
            conn.execute(text("TRUNCATE TABLE customers RESTART IDENTITY CASCADE"))
        return "db_ready"

    
    @task
    def extract_csv() -> dict:
        return {Path(p).name: p for p in glob.glob(str(Path(DATA_DIR) / "*.csv"))}

    @task
    def extract_json() -> dict:
        return {Path(p).name: p for p in glob.glob(str(Path(DATA_DIR) / "*.json"))}

    @task
    def extract_yaml() -> dict:
        return {Path(p).name: p for p in glob.glob(str(Path(DATA_DIR) / "*.yaml"))}

    def _df_from_any(path: Path) -> pd.DataFrame:
        if path.suffix == ".csv":
            return pd.read_csv(path)
        if path.suffix == ".json":
            return pd.read_json(path, orient="records")
        if path.suffix in (".yaml",".yml"):
            with open(path, "r", encoding="utf-8") as f:
                return pd.DataFrame(yaml.safe_load(f))
        raise ValueError(f"Unsupported file type: {path}")

    
    @task
    def load_customers(csv_map: dict, json_map: dict, yaml_map: dict) -> int:
        engine = create_engine(ENGINE_URL)
        path = Path(csv_map.get("customers.csv") or json_map.get("customers.json") or yaml_map.get("customers.yaml"))
        df = _df_from_any(path)
        df.to_sql("customers", engine, if_exists="append", index=False, method="multi", chunksize=1000)
        return len(df)

    @task
    def load_products(csv_map: dict, json_map: dict, yaml_map: dict) -> int:
        engine = create_engine(ENGINE_URL)
        path = Path(csv_map.get("products.csv") or json_map.get("products.json") or yaml_map.get("products.yaml"))
        df = _df_from_any(path)
        df.to_sql("products", engine, if_exists="append", index=False, method="multi", chunksize=1000)
        return len(df)

    @task
    def load_orders(csv_map: dict, json_map: dict, yaml_map: dict) -> int:
        engine = create_engine(ENGINE_URL)
        path = Path(csv_map.get("orders.csv") or json_map.get("orders.json") or yaml_map.get("orders.yaml"))
        df = _df_from_any(path)
        df.to_sql("orders", engine, if_exists="append", index=False, method="multi", chunksize=1000)
        return len(df)

    
    @task
    def validate(_: dict) -> dict:
        engine = create_engine(ENGINE_URL)
        with engine.connect() as conn:
            cust = conn.execute(text("SELECT COUNT(*) FROM customers")).scalar_one()
            prod = conn.execute(text("SELECT COUNT(*) FROM products")).scalar_one()
            ords = conn.execute(text("SELECT COUNT(*) FROM orders")).scalar_one()
            missing_fk = conn.execute(text("""
                SELECT COUNT(*) FROM orders o
                LEFT JOIN customers c ON c.customer_id = o.customer_id
                WHERE c.customer_id IS NULL
            """)).scalar_one()
        if missing_fk > 0:
            raise AirflowException(f"FK check failed: {missing_fk} orders without valid customer_id")
        if min(cust, prod, ords) < 1000:
            raise AirflowException("Row count check failed (expect ≥ 1000 per table)")
        return {"customers": cust, "products": prod, "orders": ords}

    
    @task
    def completion(stats: dict) -> str:
        Path(ARCHIVE_DIR).mkdir(parents=True, exist_ok=True)
        for f in Path(DATA_DIR).glob("*.*"):
            shutil.move(str(f), str(Path(ARCHIVE_DIR) / f.name))
        return f"success: {stats}"

    # ---- Task Instances ----
    fv = file_validation()
    db = db_setup()

    csvs = extract_csv()
    jsons = extract_json()
    yams = extract_yaml()

    lc = load_customers(csvs, jsons, yams)
    lp = load_products(csvs, jsons, yams)
    lo = load_orders(csvs, jsons, yams)

    v   = validate({"customers": lc, "products": lp, "orders": lo})
    done = completion(v)

    # --- dependencies ---
    [fv, db] >> lc
    [fv, db] >> lp
    [fv, db] >> lo
    lc >> lo
    lc >> v
    lp >> v
    lo >> v
    v >> done

    

etl_pipeline()
