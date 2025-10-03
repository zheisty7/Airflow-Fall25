import psycopg2
import pandas as pd
import yaml
import io
import requests

# Database connection configuration
DB_CONFIG = {
    "host": "localhost",
    "port": "5433",
    "database": "postgres",
    "user": "postgres",
    "password": "mysecretpassword"
}

# Base URL for GitHub raw data files
BASE_URL = "https://raw.githubusercontent.com/ofrausto3/etl-airflow-poc/97b225915671682da293753988b65879ebd07193/data/"

# Files to load for each table
FILES = {
    "customers": ["customers.csv", "customers.json", "customers.yaml"],
    "products": ["products.csv", "products.json", "products.yaml"],
    "orders": ["orders.csv", "orders.json", "orders.yaml"]
}

# Primary key column for each table (used for conflict handling)
PK_MAPPING = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id"
}

def fetch_file(file_name):
    """Fetch file content from GitHub."""
    url = BASE_URL + file_name
    response = requests.get(url)
    response.raise_for_status()
    return response.text

def load_file(conn, file_name, table_name):
    """Load data from a file into a PostgreSQL table."""
    print(f"Loading {file_name} into {table_name}...")

    # Read file into a pandas DataFrame based on file type
    if file_name.endswith(".csv"):
        df = pd.read_csv(io.StringIO(fetch_file(file_name)))
    elif file_name.endswith(".json"):
        df = pd.read_json(io.StringIO(fetch_file(file_name)))
    elif file_name.endswith(".yaml") or file_name.endswith(".yml"):
        data = yaml.safe_load(fetch_file(file_name))
        df = pd.DataFrame(data)
    else:
        raise ValueError("Unsupported file type")

    # Prepare SQL insert statement with ON CONFLICT DO NOTHING
    cur = conn.cursor()
    cols = ",".join(df.columns)
    placeholders = ",".join(["%s"] * len(df.columns))
    pk_col = PK_MAPPING[table_name]
    sql = f"""
    INSERT INTO {table_name} ({cols}) VALUES ({placeholders})
    ON CONFLICT ({pk_col}) DO NOTHING
    """

    # Insert each row into the database
    for _, row in df.iterrows():
        cur.execute(sql, tuple(row))
    conn.commit()
    cur.close()

    print(f"✅ Loaded {len(df)} records into {table_name} from {file_name}")

def main():
    """Connect to the database and load all files for all tables."""
    conn = psycopg2.connect(**DB_CONFIG)
    for table, file_list in FILES.items():
        for file_name in file_list:
            load_file(conn, file_name, table)
    conn.close()
    print("All data loaded successfully!")

if __name__ == "__main__":
    main()
