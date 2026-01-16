import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(BASE_DIR))

from src.common.database_conection import PostgresDB
from src.common.logger import get_logger, setup_logger
from src.common.file_utils import get_files        

setup_logger()
logger = get_logger(__name__)

DATA_DIR = BASE_DIR / "data"


INGESTION_MAPPING = [
    {
        "source": "customers.csv", 
        "table": "customers", 
        "pk": ["customer_id"] 
    },
    {
        "source": "stores.csv",    
        "table": "stores",    
        "pk": ["store_id"]
    },
    {
        "source": "employees.csv", 
        "table": "employees", 
        "pk": ["employee_id"] 
    },
    {
        "source": "products.csv",  
        "table": "products",  
        "pk": ["product_id"]
    },
    {
        "source": "transactions",  
        "table": "transactions", 
        "pk": ["invoice_id", "line"] 
    },
]

def ingest_safe_deduplicate(cur, table, file_path, pk_cols):
    """
    Hàm load dữ liệu.
    1. Tạo bảng tạm rỗng (Không ràng buộc).
    2. Đổ CSV vào bảng tạm.
    3. Lọc dòng trùng và Insert vào bảng thật.
    Args:
        cur: con trỏ kết nối DB
        table (str): tên bảng đích
        file_path (str): đường dẫn file CSV
        pk_cols (list): danh sách cột khóa chính
"""
    temp_table = f"{table}_staging_temp"
    
    pk_str = ",".join(pk_cols)

    cur.execute(f"DROP TABLE IF EXISTS {temp_table}")
    cur.execute(f"CREATE TEMP TABLE {temp_table} (LIKE {table} EXCLUDING CONSTRAINTS)")

    with open(file_path, 'r', encoding='utf-8') as f:
        sql = f"COPY {temp_table} FROM STDIN WITH CSV HEADER DELIMITER ','"
        cur.copy_expert(sql, f)

    clean_sql = f"""
        INSERT INTO {table}
        SELECT DISTINCT ON ({pk_str}) *
        FROM {temp_table}
        ON CONFLICT ({pk_str}) DO NOTHING;
    """
    cur.execute(clean_sql)
    
    rows = cur.rowcount
    logger.info(f"Đã merge: {rows} dòng mới.")
    
    cur.execute(f"DROP TABLE IF EXISTS {temp_table}")

def run_pipeline():

    logger.info("Bắt đầu load dữ liệu vào Postgres")

    tasks_to_run = INGESTION_MAPPING

    # Nếu có tham số truyền vào (ví dụ: python main.py customers orders)
    if len(sys.argv) > 1:
        requested_tables = sys.argv[1:] 
        
        tasks_to_run = [
            item for item in INGESTION_MAPPING 
            if item['table'] in requested_tables
        ] 

    db = PostgresDB()

    try:
        with db.get_connection() as cur:
            
            for item in tasks_to_run:
                source = item['source']
                table = item['table']
                pk_cols = item['pk']
                
                logger.info(f"Xử lý bảng: {table}")
                
                files = get_files(DATA_DIR, source, "csv")
                
                if not files:
                    logger.warning(f"Không tìm thấy data cho {source}")
                    continue

                for f in files:
                    ingest_safe_deduplicate(cur, table, f, pk_cols)
                    
        logger.info("Kết thúc load dữ liệu vào Postgres")

    except Exception:
        logger.error("Pipeline gãy!", exc_info=True)

if __name__ == "__main__":
    run_pipeline()