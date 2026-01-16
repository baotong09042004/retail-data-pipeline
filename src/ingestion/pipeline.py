import sys
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))

from src.common.logger import get_logger, setup_logger
from src.ingestion.extraction_config import get_full_load_config
from src.ingestion.extractors.postgres import stream_postgres_data
from src.ingestion.loaders.minio import upload_direct_to_minio

setup_logger()
logger = get_logger(__name__)

def generate_s3_path(base_path, date_partition):
    """Sinh đường dẫn partition theo ngày"""
    return (
        f"{base_path}/"
        f"year={date_partition.year}/month={date_partition.month}/day={date_partition.day}"
    )

def run_ingestion_pipeline(run_date, EXTRACTION_CONFIG):
    """
    Chạy pipeline ingestion từ Postgres -> MinIO.
        Args:
            run_date  -- Ngày chạy pipeline (dùng để tạo partition) 
            EXTRACTION_CONFIG  -- Cấu hình extraction 
    """
    logger.info("BẮT ĐẦU INGESTION")

    for config in EXTRACTION_CONFIG:
        table = config['table']
        logger.info(f"Processing: {table}")
        
        # Chuẩn bị đường dẫn đích (MinIO)
        base_s3_path = generate_s3_path(config['output_base'], run_date)
        
        # Gọi Extractor
        data_stream = stream_postgres_data(config['query'])
        
        # Vòng lặp: Có cục data nào -> Bắn luôn cục đó
        for i, chunk in enumerate(data_stream):
                        
            # Tạo đường dẫn file đích
            file_s3_path = f"{base_s3_path}/part_{i}.parquet"
            logger.info(f"   -> Streaming chunk {i} to: {file_s3_path}")
            
            upload_direct_to_minio(chunk, file_s3_path)

        logger.info(f"Xong bảng: {table}")

    logger.info("KẾT THÚC INGESTION.")

if __name__ == "__main__":
    extraction_config = get_full_load_config()
    run_date = datetime.now().date()
    run_ingestion_pipeline(run_date, extraction_config)