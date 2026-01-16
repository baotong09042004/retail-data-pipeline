import sys
from datetime import datetime, timedelta
from pathlib import Path

# Setup đường dẫn
BASE_DIR = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(BASE_DIR))

from src.common.duckdb_connection import DuckDB
from src.common.spark_utils import get_spark_session
from src.common.logger import setup_logger, get_logger
from src.common.config import settings
from src.processing.transformation import transform_data 

setup_logger()
logger = get_logger(__name__)

def get_raw_s3_path(table_name, run_date=None):
    """
    Lấy đường dẫn file RAW (Input).
        Arg:
            table_name (str): Tên bảng nguồn
            run_date (datetime): Ngày chạy ETL (mặc định: ngày hiện tại)
        Return:
            str: Đường dẫn S3 đến file RAW
    """
    if run_date is None:
        run_date = datetime.now()

    year = run_date.year
    month = run_date.month
    day = run_date.day
    
    # Ví dụ: s3a://datalake/raw/transactions/load_type=snapshot/year=2026/month=1/day=8
    return (
        f"s3a://{settings.MINIO_BUCKET}/raw/{table_name}/"
        f"load_type=snapshot/"
        f"year={year}/month={month}/day={day}"
    )

def get_processed_s3_path(dw_table, run_date=None):
    """
    Lấy đường dẫn file PROCESSED.
        Arg:
            dw_table (str): Tên bảng đích trong DW
            run_date (datetime): Ngày chạy ETL (mặc định: ngày hiện tại)
        Return:
            tuple: (str, str) Đường dẫn S3 cho Spark và DuckDB
    """
    if run_date is None:
        run_date = datetime.now()
        
    partition = f"year={run_date.year}/month={run_date.month}/day={run_date.day}"
    
    spark_path = f"s3a://{settings.MINIO_BUCKET}/processed/{dw_table}/{partition}"
    
    duckdb_path = f"s3://{settings.MINIO_BUCKET}/processed/{dw_table}/{partition}/*.parquet"
    
    return spark_path, duckdb_path

def run_etl_job(spark, duck_client, source_table, dw_table, execution_date=None):
    
    raw_path = get_raw_s3_path(source_table, execution_date)
    staging_spark, staging_duckdb = get_processed_s3_path(dw_table, execution_date)    

    try:
        logger.info(f"START JOB: {source_table} -> {dw_table}")
        
        # ---SPARK READ (RAW) ---
        logger.info(f"Spark reading from: {raw_path}")

        try:
            df_spark = spark.read.parquet(raw_path)
        except Exception as e:
            if "Path does not exist" in str(e):
                logger.warning(f"Không tìm thấy data ngày {execution_date} cho bảng {source_table}")
                return 
            raise e

        logger.info(f"Transforming & Writing to Staging: {staging_spark}")
        
        df_clean = transform_data(df_spark, dw_table)
        
        df_clean.coalesce(1).write \
            .mode("overwrite") \
            .parquet(staging_spark)
            
        # ---DUCKDB LOAD---
        logger.info(f"DuckDB Bulk Loading from: {staging_duckdb}")
        
        with duck_client.get_connection() as conn:
            
            # Lấy danh sách cột từ Spark để tạo câu query chuẩn
            columns = df_clean.columns
            cols_str = ", ".join(columns)
            
            # COPY từ MinIO vào DuckDB
            query = f"""
                INSERT INTO {dw_table} ({cols_str}) 
                SELECT {cols_str} 
                FROM read_parquet('{staging_duckdb}')
            """
            conn.execute(query)
            
            # Log kết quả
            count = conn.execute(f"SELECT count(*) FROM {dw_table}").fetchone()[0]
            logger.info(f"FINISHED {dw_table}. Total rows: {count}")

    except Exception as e:
        logger.error(f"FAILED {dw_table}: {e}")
        raise e

if __name__ == "__main__":
    spark = get_spark_session("ETL Staging Pattern")
    
    spark.sparkContext.setLogLevel("ERROR")
    
    duck_db = DuckDB()

    run_date = datetime.now() - timedelta(days=1)

    jobs = [
        ("stores", "dim_stores"),
        ("products", "dim_products"),
        ("customers", "dim_customers"),
        ("employees", "dim_employees"),
        ("transactions", "fact_sales"),
    ]    
    try:
        for src, dest in jobs:
            run_etl_job(spark, duck_db, src, dest, execution_date=run_date)
            
    finally:
        spark.stop()
        logger.info("DONE.")