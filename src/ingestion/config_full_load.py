from src.common.config import settings

BUCKET = settings.MINIO_BUCKET

TABLE_LIST = [
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
    for table_name in TABLE_LIST
]