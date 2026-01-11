import duckdb
from contextlib import contextmanager
from pathlib import Path
from src.common.logger import setup_logger, get_logger
from src.common.config import settings  

setup_logger()
logger = get_logger(__name__)

class DuckDB:
    def __init__(self, db_path=None):
        if db_path is None:
            # Đường dẫn mặc định tới file DB local
            base_dir = Path(__file__).resolve().parent.parent.parent
            self.db_path = str(base_dir / "duckdb" / "datawarehouse.duckdb")
        else:
            self.db_path = str(db_path)

    @contextmanager
    def get_connection(self):
        """
        Context manager quản lý kết nối DuckDB Local.
        Tự động cấu hình MinIO (S3) mỗi khi mở kết nối.
        """
        conn = None
        try:
            conn = duckdb.connect(self.db_path)
            
            # CẤU HÌNH ĐỂ DUCKDB ĐỌC ĐƯỢC MINIO ---
            # Cài đặt extension httpfs (Bắt buộc để đọc S3/MinIO)
            conn.execute("INSTALL httpfs; LOAD httpfs;")
            
            endpoint = settings.MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
            
            query = f"""
                SET s3_region='us-east-1';
                SET s3_endpoint='{endpoint}';
                SET s3_access_key_id='{settings.MINIO_ACCESS_KEY}';
                SET s3_secret_access_key='{settings.MINIO_SECRET_KEY}';
                SET s3_use_ssl=false;
                SET s3_url_style='path';
            """
            conn.execute(query)
            
            yield conn
            
        except Exception as e:
            logger.error(f"Lỗi kết nối DuckDB: {e}")
            raise e
            
        finally:
            if conn:
                conn.close()