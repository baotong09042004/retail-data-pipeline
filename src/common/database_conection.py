import psycopg2
from contextlib import contextmanager
from src.common.config import settings

class PostgresDB:

    @contextmanager
    def get_connection(self):
        """Context manager để quản lý connection và cursor"""
        conn = None
        cur = None
        try:
            
            conn = psycopg2.connect(
                host=settings.DB_HOST,
                port=settings.DB_PORT,
                database=settings.DB_NAME,
                user=settings.DB_USER,
                password=settings.DB_PASSWORD
            )
            
            cur = conn.cursor()
            yield cur
            conn.commit()
            
        except Exception as e:
            if conn:
                conn.rollback()
            raise  e
            
        finally:
            try:
                if cur:
                    cur.close()
            finally:
                if conn:
                    conn.close()