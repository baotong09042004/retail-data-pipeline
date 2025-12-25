from pathlib import Path
from typing import List
from src.common.logger import setup_logger, get_logger

setup_logger()
logger=get_logger(__name__)

def get_files(base_dir: Path, source_name: str, format="Null") -> List[Path]:
    """
    Lấy danh sách file từ thư mục hoặc file cụ thể.
    """
    try:
        if not base_dir or not source_name:
            logger.warning("Base dir hoặc Source name bị rỗng.")
            return []

        path = base_dir / source_name

        if not path.exists():
            logger.warning(f"Đường dẫn không tồn tại: {path}")
            return []

        if path.is_file():
            return [path]

        if path.is_dir():
            try:
                files = list(path.glob(f"*.{format}"))
                
                if not files:
                    logger.warning(f"Folder rỗng : {path}")
                
                return files

            except PermissionError:
                logger.error(f"Không có quyền truy cập folder: {path}")
                return []
                
            except OSError as e:
                logger.error(f"Lỗi hệ điều hành (I/O) khi quét folder {path}: {e}")
                return []

        return []

    except Exception as e:
        logger.error(f"Lỗi không xác định trong get_files: {e}", exc_info=True)
        return []