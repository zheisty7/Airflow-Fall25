import psycopg2
import pandas as pd
import yaml
import io
import requests

DB_CONFIG = {
    "host": "postgres",
    "port": "5432",
    "database": "airflow",
    "user": "airflow",
    "password": "airflow"
}

BASE_URL = "https://raw.githubusercontent.com/ofrausto3/etl-airflow-poc/97b225915671682da293753988b65879ebd07193/data/"

FILES = {
    "customers": ["customers.csv", "customers.json", "customers.yaml"],
    "products": ["products.csv", "products.json", "products.yaml"],
    "orders": ["orders.csv", "orders.json", "orders.yaml"]
}

PK_MAPPING = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id"
}

def fetch_file(file_name):
    url = BASE_URL + file_name
    response = requests.get(url)
    response.raise_for_status()
    return response.text

def load_file(conn, file_name, table_name):
    print(f"Loading {file_name} into {table_name}...")
    if file_name.endswith(".csv"):
        df = pd.read_csv(io.StringIO(fetch_file(file_name)))
    elif file_name.endswith(".json"):
        df = pd.read_json(io.StringIO(fetch_file(file_name)))
    elif file_name.endswith(".yaml") or file_name.endswith(".yml"):
        data = yaml.safe_load(fetch_file(file_name))
        df = pd.DataFrame(data)
    else:
        raise ValueError("Unsupported file type")

    cur = conn.cursor()
    cols = ",".join(df.columns)
    placeholders = ",".join(["%s"] * len(df.columns))
    pk_col = PK_MAPPING[table_name]
    sql = f"""
    INSERT INTO {table_name} ({cols}) VALUES ({placeholders})
    ON CONFLICT ({pk_col}) DO NOTHING
    """
    for _, row in df.iterrows():
        cur.execute(sql, tuple(row))
    conn.commit()
    cur.close()
    print(f"✅ Loaded {len(df)} records into {table_name} from {file_name}")

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    for table, file_list in FILES.items():
        for file_name in file_list:
            load_file(conn, file_name, table)
    conn.close()
    print("All data loaded successfully!")

if __name__ == "__main__":
    main()
