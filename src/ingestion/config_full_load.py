from src.common.config import settings

BUCKET = settings.MINIO_BUCKET

TABLES = [
    "customers",
    "products",
    "stores",
    "employees",
    "transactions",
]

EXTRACTION_CONFIG = [
    {
        "table": table_name,
        "query": f"SELECT * FROM {table_name}",
        "output_base": f"s3://{BUCKET}/raw/{table_name}",
        "extract_mode": "snapshot"
    }
    for table_name in TABLES if table_name != "transactions"
]

EXTRACTION_CONFIG.append({
    "table": "transactions",
    "query": """
        SELECT 
            invoice_id,
            line,
            customer_id, 
            product_id,
            size,
            color,
            unit_price, 
            quantity,
            TO_CHAR(date, 'YYYY-MM-DD HH24:MI:SS') as date,
            discount,
            line_total,
            store_id,
            employee_id, 
            sku,
            transaction_type,
            payment_method,
            invoice_total
        FROM transactions
    """,  
    "output_base": f"s3://{BUCKET}/raw/transactions",
    "extract_mode": "snapshot"
})