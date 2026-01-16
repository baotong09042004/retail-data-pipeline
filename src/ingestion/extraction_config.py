from src.common.config import settings

BUCKET = settings.MINIO_BUCKET

TABLES = [
    "customers",
    "products",
    "stores",
    "employees",
]

TRANSACTION_BASE_QUERY = """
    SELECT 
        invoice_id, line, customer_id, product_id, 
        size, color, unit_price, quantity, 
        TO_CHAR(transaction_time, 'YYYY-MM-DD HH24:MI:SS') as date,
        discount, line_total, store_id, employee_id, 
        sku, transaction_type, payment_method, invoice_total
    FROM transactions
"""

def get_full_load_config():
    """
    Trả về config Full Load
    """
    configs = []
    
    for table in TABLES:
        configs.append({
            "table": table,
            "query": f"SELECT * FROM {table}",
            "output_base": f"s3://{BUCKET}/raw/{table}",
        })

    configs.append({
        "table": "transactions",
        "query": TRANSACTION_BASE_QUERY, 
        "output_base": f"s3://{BUCKET}/raw/transactions",
    })
    
    return configs

def get_batch_config(extract_date: str):
    """
    Trả về config theo ngày chỉ định.
        Args:
            extract_date -- Ngày cần lấy dữ liệu (Định dạng: 'YYYY-MM-DD')
    """
    configs = []
    
    for table in TABLES:
        query = f"""
            SELECT * FROM {table} 
            WHERE update_date = '{extract_date}'::DATE 
        """
        
        configs.append({
            "table": table,
            "query": query,
            "output_base": f"s3://{BUCKET}/raw/{table}",
        })

    # 2. Transactions (Chỉ lấy giao dịch trong ngày đó)
    # Nối thêm mệnh đề WHERE vào cái query gốc
    trans_query = f"""
        {TRANSACTION_BASE_QUERY}
        WHERE transaction_time::DATE = '{extract_date}'::DATE
    """
    
    configs.append({
        "table": "transactions",
        "query": trans_query,
        "output_base": f"s3://{BUCKET}/raw/transactions",
    })
    
    return configs