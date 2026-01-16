CREATE SEQUENCE seq_employee_key START 1;

CREATE TABLE dim_date (
    date_id INT PRIMARY KEY,       
    full_date DATE,                 
    day INT,
    month INT,
    year INT,
    quarter INT,                    -- Quý (1, 2, 3, 4)
    day_of_week INT,                -- 1 (Chủ nhật) -> 7 (Thứ 7) 
    day_name VARCHAR(20),           -- 'Monday', 'Tuesday'
    month_name VARCHAR(20),         -- 'January'...
    is_weekend BOOLEAN              -- TRUE nếu là T7, CN 
);

CREATE TABLE dim_stores (
    store_id INT PRIMARY KEY,       
    store_name VARCHAR(255),
    city VARCHAR(100),
    country VARCHAR(100),
    zip_code VARCHAR(20),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    create_date DATE,
    update_date DATE,
    is_active BOOLEAN
);

CREATE TABLE dim_products (
    product_id INT PRIMARY KEY,
    category VARCHAR(100),
    sub_category VARCHAR(100),
    description VARCHAR(500),       
    base_cost DECIMAL(10, 2),
    size VARCHAR(100),
    color VARCHAR(100),
    create_date DATE,
    update_date DATE,
    is_active BOOLEAN
);

CREATE TABLE dim_customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(255),
    email VARCHAR(255),
    telephone VARCHAR(50),
    city VARCHAR(100),
    country VARCHAR(100),
    gender CHAR(1),
    date_of_birth DATE,
    create_date DATE,
    update_date DATE,
);

-- 5. Dimension Employees
CREATE TABLE dim_employees (
    employee_key INTEGER PRIMARY KEY DEFAULT nextval('seq_employee_key'), 
    employee_id INT,              
    name VARCHAR(255),
    position VARCHAR(100),
    store_id INT,
    start_date DATE,
    end_date DATE,
    is_current BOOLEAN
);

CREATE TABLE fact_sales (
    invoice_id VARCHAR(50),         
    
    transaction_type VARCHAR(50), 
    payment_method VARCHAR(50),    
    sku VARCHAR(100),
    color VARCHAR(50),              
    size VARCHAR(10),              
    quantity INT,
    unit_price DECIMAL(10, 2),      -- Giá bán ra
    discount DECIMAL(10, 2),        -- Tiền giảm giá
    line_total DECIMAL(12, 2),      -- Thành tiền sau giảm giá
    
    cost_amount DECIMAL(12, 2),     -- = dim_products.production_cost * quantity
    profit_amount DECIMAL(12, 2),   -- = line_total - (production_cost * quantity)
    transaction_time TIMESTAMP,              

    line_id INT,                   
    date_id INT,                    
    customer_id INT,               
    store_id INT,                  
    product_id INT,                 
    employee_id INT,               
);