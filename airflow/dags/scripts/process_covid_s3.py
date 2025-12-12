import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col

def sanitize_column_name(name):
    """
    Убираем спецсимволы из названий колонок для совместимости с Parquet/Iceberg.
    Пример: 'Province/State' -> 'Province_State'
    """
    return name.strip().replace("/", "_").replace(" ", "_").replace("-", "_")

def main():
    # Инициализация Spark Session
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

    # --- ЧТЕНИЕ АРГУМЕНТОВ ---
    if len(sys.argv) < 3:
        raise ValueError("Usage: process_covid_s3.py <S3_INPUT_PATH> <TARGET_TABLE>")

    s3_input_path = sys.argv[1] # Например: s3a://covid-daily-reports-csv/year=2020/...
    target_table = sys.argv[2]  # Например: raw_catalog.covid.daily_reports

    print(f"--- START PROCESSING ---")
    print(f"Input S3: {s3_input_path}")
    print(f"Target Iceberg: {target_table}")

    try:
        # 1. Читаем CSV силами Spark из S3 (Распределенное чтение)
        # option("inferSchema", "true") заставляет Spark определить типы данных (int, string, double)
        df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(s3_input_path)

        # 2. Чистим имена колонок
        new_columns = [col(c).alias(sanitize_column_name(c)) for c in df.columns]
        df_renamed = df.select(*new_columns)

        # 3. Добавляем технические поля (Auditing)
        df_final = df_renamed \
            .withColumn("source_file", lit(s3_input_path)) \
            .withColumn("ingestion_ts", current_timestamp())

        # 4. Пишем в Iceberg
        writer = df_final.writeTo(target_table) \
            .using("iceberg") \
            .tableProperty("format-version", "2")

        # Проверяем, существует ли таблица в каталоге
        table_exists = spark.catalog.tableExists(target_table)

        if table_exists:
            # Если таблица существует, просто добавляем данные (APPEND)
            writer.append()
            print(f"SUCCESS: Данные добавлены в существующую таблицу {target_table}.")
        else:
            # Если таблица не существует, создаем ее (CREATE)
            writer.create()
            print(f"SUCCESS: Таблица {target_table} создана и данные записаны.")

            print(f"SUCCESS: Processed {s3_input_path}")

    except Exception as e:
        print(f"ERROR: Failed processing {s3_input_path}")
        # Пробрасываем ошибку, чтобы Airflow знал, что таск упал
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()