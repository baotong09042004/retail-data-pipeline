from pathlib import Path
from src.common.database_conection import PostgresDB
from src.common.logger import setup_logger, get_logger


setup_logger()
logger = get_logger(__name__)

def read_sql_file(filename):
    sql_path = Path(__file__).resolve().parent / filename
    
    if not sql_path.exists():
        logger.error(f"Không tìm thấy file SQL: {sql_path}")
        raise FileNotFoundError(f"Missing file: {sql_path}")
    
    logger.info(f"Đang đọc file: {sql_path.name}...")
    
    with open(sql_path, "r", encoding="utf-8") as f:
        return f.read()

if __name__ == "__main__":
    db = PostgresDB()
    
    try:
        sql_query = read_sql_file("db_schema.sql")
        logger.info("Bắt đầu khởi tạo cấu trúc bảng...")
        
        with db.get_connection() as cursor:
            cursor.execute(sql_query)
            
        logger.info("Khởi tạo Schema thành công!")

    except Exception as err:
        logger.error("Lỗi hệ thống khi tạo bảng!", exc_info=True)