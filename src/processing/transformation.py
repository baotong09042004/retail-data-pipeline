from pyspark.sql.functions import col, lit, year, month, dayofmonth


def transform_data(df, table_name):
    """
    hàm transform dữ liệu từ Data Lake sang Data Warehouse.
        Arg:
            df (DataFrame): DataFrame Spark đầu vào
            table_name (str): Tên bảng đích trong Data Warehouse
        Return:
            DataFrame: DataFrame Spark đã được transform theo luật của từng bảng
    """

    if table_name == "dim_stores":
        return df.select(
            col("store_id"),
            col("store_name"),
            col("city"),
            col("country"),
            col("zip_code"),
            col("latitude"),
            col("longitude"),
            lit("2023-01-01").cast("date").alias("create_date"),
            lit(None).cast("date").alias("update_date"),
            lit(True).cast("boolean").alias("is_active")
        )

    elif table_name == "dim_products":
        return df.select(
            col("product_id"),
            col("category"),
            col("sub_category"),
            # Map tên cột: Source -> DW
            col("description_en").alias("description"),
            col("production_cost").alias("base_cost"),
            col("sizes").alias("size"), 
            col("color"),
            # Thêm cột
            lit("2023-01-01").cast("date").alias("create_date"),
            lit(None).cast("date").alias("update_date"),
            lit(True).cast("boolean").alias("is_active")
        )

    elif table_name == "dim_customers":
        return df.select(
            col("customer_id"),
            col("name"),
            col("email"),
            col("telephone"),
            col("city"),
            col("country"),
            col("gender"),
            col("date_of_birth")
        )

    elif table_name == "dim_employees":
        return df.select(
            col("employee_id"),
            col("name"),
            col("position"),
            col("store_id"),
            lit("2023-01-01").cast("date").alias("start_date"),
            lit(None).cast("date").alias("end_date"),
            lit(True).cast("boolean").alias("is_current")
        )

    elif table_name == "fact_sales":
        
        df_processed = df.withColumn(
            "real_timestamp", 
            col("date").cast("timestamp")
        )

        # Tính date_id dựa trên cái timestamp vừa convert
        df_final = df_processed.withColumn(
            "date_id", 
            (year("real_timestamp") * 10000) + (month("real_timestamp") * 100) + dayofmonth("real_timestamp")
        )

        return df_final.select(
            col("invoice_id"),
            col("transaction_type"),
            col("payment_method"),
            col("sku"),
            col("color"),
            col("size"), 
            col("quantity"),
            col("unit_price"),
            col("discount"),
            col("line_total"),
            
          
            lit(0).alias("cost_amount"),
            lit(0).alias("profit_amount"),
            
            col("real_timestamp").alias("date"), 
            
            col("line").alias("line_id"), 
            col("date_id"), 
            col("customer_id"),
            col("store_id"),
            col("product_id"),
            col("employee_id") 
        )
        
    else:
        raise ValueError(f"Không tìm thấy luật transform cho bảng: {table_name}")

