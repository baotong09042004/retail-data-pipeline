import pandas as pd
from src.common.database_conection import PostgresDB
import warnings

warnings.filterwarnings('ignore', category=UserWarning)

def stream_postgres_data(query, chunk_size=100000):
    """
    Generator function: Đọc DB và bắn ra từng cục DataFrame (RAM).
    """
    db = PostgresDB()
    with db.get_connection() as cur:
        conn = cur.connection
        for chunk in pd.read_sql(query, conn, chunksize=chunk_size):
            yield chunk