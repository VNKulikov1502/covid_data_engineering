import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, coalesce, lit,
    count, current_timestamp, to_date, regexp_extract
)

def main():
    if len(sys.argv) < 2:
        raise ValueError("Usage: build_ods_daily_stats.py <REPORT_DATE>")

    report_date = sys.argv[1]

    # --- Инициализация Spark ---
    spark = SparkSession.builder \
        .appName("Build ODS Daily Country Stats") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.iceberg.type", "hive") \
        .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083") \
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse/") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()

    # --- Чтение RAW слоя ---
    raw_df = spark.table("iceberg.raw.daily_reports")

    # --- Извлекаем дату отчета из имени файла ---
    df = raw_df.withColumn(
        "report_date",
        to_date(regexp_extract(col("source_file"), r"(\d{4}-\d{2}-\d{2})", 1))
    ).filter(col("report_date") == report_date)

    # --- Агрегация по стране с обработкой NULL ---
    ods_df = df.groupBy(
        "report_date",
        col("country_region").alias("country")
    ).agg(
        _sum(coalesce(col("confirmed"), lit(0))).alias("confirmed"),
        _sum(coalesce(col("deaths"), lit(0))).alias("deaths"),
        _sum(coalesce(col("recovered"), lit(0))).alias("recovered"),
        _sum(coalesce(col("active"), lit(0))).alias("active"),
        count("*").alias("source_records_cnt")
    ).withColumn(
        "ingestion_ts", current_timestamp()
    )

    # --- Создание namespace ODS если не существует ---
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.ods")

    target_table = "iceberg.ods.daily_country_stats"

    writer = ods_df.writeTo(target_table) \
        .using("iceberg") \
        .partitionedBy(col("report_date"))

    try:
        # Если таблица уже существует, перезаписываем партицию по дате
        spark.table(target_table)
        writer.overwritePartitions()
    except:
        # Если нет — создаем
        writer.create()

    print(f"ODS layer updated for {report_date}")

    spark.stop()

if __name__ == "__main__":
    main()