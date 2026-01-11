from pathlib import Path
from src.common.logger import setup_logger, get_logger
from src.common.duckdb_connection import DuckDB
import duckdb
import os



setup_logger()
logger = get_logger(__name__)

def read_sql_file(sql_path):
    
    if not sql_path.exists():
        logger.error(f"Không tìm thấy file SQL: {sql_path}")
        raise FileNotFoundError(f"Missing file: {sql_path}")
    
    logger.info(f"Đang đọc file: {sql_path.name}...")
    
    with open(sql_path, "r", encoding="utf-8") as f:
        return f.read()

if __name__ == "__main__":
    
    duckdb_client = DuckDB()
    try:
        datawarehouse_schema_path = Path(__file__).resolve().parent / "datawarehouse.sql"
        schema_query = read_sql_file(datawarehouse_schema_path)
        logger.info("Bắt đầu khởi tạo cấu trúc bảng cho Data Warehouse...")
        
        with duckdb_client.get_connection() as conn:
            conn.execute(schema_query)
            
        logger.info("Khởi tạo Schema Data Warehouse thành công!")

    except Exception as err:
        logger.error("Lỗi hệ thống khi tạo bảng Data Warehouse!", exc_info=True)