import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col


def sanitize_column_name(name):
    """
    Убираем спецсимволы из названий колонок для совместимости с Parquet/Iceberg.
    Пример: 'Province/State' -> 'Province_State'
    """
    return (
        name.strip()
        .replace("/", "_")
        .replace(" ", "_")
        .replace("-", "_")
    )


def main():
    spark = SparkSession.builder \
        .appName("Covid Processing S3") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.hadoop.hive.metastore.client.capability.check", "false") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hive") \
        .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # --- АРГУМЕНТЫ ---
    if len(sys.argv) < 3:
        raise ValueError(
            "Usage: process_covid_s3.py <S3_INPUT_PATH> <TARGET_TABLE>"
        )

    s3_input_path = sys.argv[1]
    target_table = sys.argv[2]  # iceberg.raw.daily_reports

    print("=== START PROCESSING ===")
    print(f"Input S3 path: {s3_input_path}")
    print(f"Target Iceberg table: {target_table}")

    try:
        # 1. Чтение CSV из S3
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(s3_input_path)

        # 2. Нормализация имён колонок
        df_clean = df.select(
            *[col(c).alias(sanitize_column_name(c)) for c in df.columns]
        )

        # 3. Технические поля
        df_final = df_clean \
            .withColumn("source_file", lit(s3_input_path)) \
            .withColumn("ingestion_ts", current_timestamp())

        # 4. Гарантируем существование namespace (RAW)
        spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.raw")

        # 5. Проверяем существование таблицы
        table_exists = spark.catalog.tableExists("iceberg.raw.daily_reports")

        writer = df_final.writeTo(target_table) \
            .using("iceberg") \
            .tableProperty("format-version", "2")

        if table_exists:
            writer.append()
            print(f"SUCCESS: Data appended to {target_table}")
        else:
            writer.create()
            print(f"SUCCESS: Table {target_table} created and data written")

        print("=== PROCESSING FINISHED SUCCESSFULLY ===")

    except Exception as e:
        print("ERROR: Processing failed")
        raise e

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
