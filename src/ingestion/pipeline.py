import sys
from pathlib import Path
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))

from src.common.logger import get_logger, setup_logger
from config_full_load import EXTRACTION_CONFIG
from src.ingestion.extractors.postgres import stream_postgres_data
from src.ingestion.loaders.minio import upload_direct_to_minio

setup_logger()
logger = get_logger(__name__)

def generate_s3_path(base_path, mode):
    """Sinh đường dẫn partition theo ngày"""
    today = datetime.now()
    return (
        f"{base_path}/"
        f"load_type={mode}/"
        f"year={today.year}/month={today.month}/day={today.day}"
    )

def run_ingestion_pipeline():
    logger.info("BẮT ĐẦU INGESTION")

    for config in EXTRACTION_CONFIG:
        table = config['table']
        logger.info(f"Processing: {table}")
        
        # Chuẩn bị đường dẫn đích (MinIO)
        base_s3_path = generate_s3_path(config['output_base'], config['extract_mode'])
        
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
    run_ingestion_pipeline()