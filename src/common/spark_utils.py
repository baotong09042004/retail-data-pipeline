from pyspark.sql import SparkSession
from src.common.config import settings

def get_spark_session(app_name="Spark App"):
    """
    Khởi tạo Spark Session với cấu hình MinIO S3 chuẩn.
    """
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.s3a.endpoint", settings.MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", settings.MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", settings.MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .getOrCreate()