CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(20),
    city VARCHAR(100),
    region VARCHAR(100),
    country VARCHAR(100)
);

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price NUMERIC(10,2)
);

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    product_id INT REFERENCES products(product_id),
    quantity INT,
    order_date DATE
);

CREATE TABLE etl_audit (
    audit_id SERIAL PRIMARY KEY,
    table_name VARCHAR(50),
    file_name VARCHAR(255),
    row_count INT,
    load_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
