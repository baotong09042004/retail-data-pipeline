from src.common.config import settings
from src.common.logger import get_logger

logger = get_logger(__name__)

def upload_direct_to_minio(dataframe, s3_path):
    """
    Nhận DataFrame -> Bắn thẳng lên MinIO qua mạng.
    """
    try:
        dataframe.to_parquet(
            s3_path,
            index=False,
            storage_options=settings.STORAGE_OPTIONS
        )
        return True
    except Exception as e:
        logger.error(f"Upload failed: {s3_path} | Error: {e}")
        raise e