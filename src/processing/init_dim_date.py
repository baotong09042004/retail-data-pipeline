import sys
import duckdb
from pathlib import Path
from src.common.duckdb_connection import DuckDB


duckdb_client = DuckDB()
query = """
        DELETE FROM dim_date;

        INSERT INTO dim_date
        SELECT
            CAST(strftime(datum, '%Y%m%d') AS INT) as date_id,
            datum as full_date,
            day(datum) as day,
            month(datum) as month,
            year(datum) as year,
            quarter(datum) as quarter,
            
            -- 1 (CN) -> 7 (T7)
            dayofweek(datum) + 1 as day_of_week,
            
            dayname(datum) as day_name,
            
            monthname(datum) as month_name,
            
            -- is_weekend (0=Sunday, 6=Saturday)
            CASE 
                WHEN dayofweek(datum) IN (0, 6) THEN TRUE 
                ELSE FALSE 
            END as is_weekend

        FROM 
            generate_series(DATE '2022-01-01', DATE '2026-12-31', INTERVAL 1 DAY) as t(datum);
        """
with duckdb_client.get_connection() as conn:
    conn.execute(query)
    count = conn.execute("SELECT count(*) FROM dim_date").fetchone()[0]
    print(f"Đã sinh xong! Tổng cộng: {count} ngày.")
