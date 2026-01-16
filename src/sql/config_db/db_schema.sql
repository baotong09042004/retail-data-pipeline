CREATE TABLE stores (
    store_id SERIAL PRIMARY KEY,
    country VARCHAR(100),
    city VARCHAR(100),
    store_name VARCHAR(255),
    number_of_employees INT,
    zip_code VARCHAR(20),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    create_date DATE,
    update_date DATE,
    is_active BOOLEAN
);

CREATE TABLE employees (
    employee_id SERIAL PRIMARY KEY, 
    store_id INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    position VARCHAR(100),
    create_date DATE,
    update_date DATE,
    is_active BOOLEAN,
    FOREIGN KEY (store_id) REFERENCES stores(store_id)
);

CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    telephone VARCHAR(50),
    city VARCHAR(100),
    country VARCHAR(100),
    gender CHAR(1),
    date_of_birth DATE,
    create_date DATE,
    update_date DATE 
);

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY, 
    category VARCHAR(100),
    sub_category VARCHAR(100),
    description_en VARCHAR(500),
    color VARCHAR(100),
    sizes VARCHAR(100),
    production_cost DECIMAL(10, 2),
    create_date DATE,
    update_date DATE,
    is_active BOOLEAN
);

CREATE TABLE transactions (
    invoice_id VARCHAR(50),
    line INT,
    customer_id INT,
    product_id INT,
    size VARCHAR(10),
    color VARCHAR(50),
    unit_price DECIMAL(10, 2),
    quantity INT,
    transaction_time TIMESTAMP,
    discount DECIMAL(10, 2),
    line_total DECIMAL(12, 2),
    store_id INT,
    employee_id INT,
    sku VARCHAR(100),
    transaction_type VARCHAR(50),
    payment_method VARCHAR(50),
    invoice_total DECIMAL(12, 2),
    PRIMARY KEY (invoice_id, line),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (store_id) REFERENCES stores(store_id),
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id)
);

CREATE INDEX idx_customer_id ON transactions(customer_id);
CREATE INDEX idx_product_id ON transactions(product_id);
CREATE INDEX idx_store_id ON transactions(store_id);
CREATE INDEX idx_employee_store ON employees(store_id);
CREATE INDEX idx_transaction_time ON transactions(transaction_time);