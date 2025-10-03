# Airflow-Fall25
Develop a ETL pipeline that processes customer, product, and order data from CSV, JSON, and YAML files into a PostgreSQL database using Apache Airflow

load_data.py - data is connected to Oscar’s GitHub, and is loaded into our PostgresSQL database
schema.sql - SQL script creating our database tables
Folder also contains the downloaded data from Oscar’s repository, but it can be ignored

Steps to set up (MAC) 

Create the database through input:
docker exec -it project_one psql -U postgres -f /path/to/schema.sql

Run data loading script
python3 load_data.py 
