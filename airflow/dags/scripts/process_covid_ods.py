import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, coalesce, lit, when,
    count, current_timestamp, to_date, regexp_extract
)

def main():
    if len(sys.argv) < 2:
        raise ValueError("Usage: build_ods_daily_stats.py <REPORT_DATE>")

    report_date = sys.argv[1]
    print(f"--- Processing ODS (Overwrite Partitions) for date: {report_date} ---")

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

    raw_df = spark.table("iceberg.raw.daily_reports")

    df_filtered = raw_df.withColumn(
        "report_date",
        to_date(regexp_extract(col("source_file"), r"(\d{4}-\d{2}-\d{2})", 1))
    ).filter(col("report_date") == report_date)

    if df_filtered.count() == 0:
        print(f"Skipping: No data found in RAW for {report_date}")
        spark.stop()
        return

    df_normalized = df_filtered.withColumn("country_normalized",
                                           when(col("country_region") == "US", "United States")
                                           .when(col("country_region") == "Korea, South", "Korea, Rep.")
                                           .when(col("country_region") == "Taiwan*", "Taiwan")
                                           .when(col("country_region") == "Hong Kong", "Hong Kong SAR, China")
                                           .when(col("country_region") == "Iran (Islamic Republic of)", "Iran, Islamic Rep.")
                                           .when(col("country_region") == "Russia", "Russian Federation")
                                           .when(col("country_region") == "Mainland China", "China")
                                           .when(col("country_region") == "Turkey", "Turkiye")
                                           .when(col("country_region") == "Vietnam", "Viet Nam")
                                           .when(col("country_region") == "Burma", "Myanmar")
                                           .when(col("country_region") == "Slovakia", "Slovak Republic")
                                           .when(col("country_region") == "Kyrgyzstan", "Kyrgyz Republic")
                                           .when(col("country_region") == "Egypt", "Egypt, Arab Rep.")
                                           .when(col("country_region") == "Iran", "Iran, Islamic Rep.")
                                           .when(col("country_region") == "Venezuela", "Venezuela, RB")
                                           .otherwise(col("country_region"))
                                           )

    ods_df = df_normalized.groupBy(
        "report_date",
        col("country_normalized").alias("country_region")
    ).agg(
        _sum(coalesce(col("confirmed"), lit(0))).alias("confirmed"),
        _sum(coalesce(col("deaths"), lit(0))).alias("deaths"),
        _sum(coalesce(col("recovered"), lit(0))).alias("recovered"),
        _sum(coalesce(col("active"), lit(0))).alias("active"),
        count("*").alias("source_records_cnt")
    ).withColumn(
        "ingestion_ts", current_timestamp()
    )

    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.ods")
    target_table = "iceberg.ods.daily_country_stats"

    print(f"Writing to {target_table} using overwritePartitions...")

    writer = ods_df.writeTo(target_table) \
        .using("iceberg") \
        .partitionedBy(col("report_date")) \
        .tableProperty("format-version", "2") \
        .tableProperty("write.parquet.compression-codec", "snappy")

    try:
        spark.table(target_table)
        writer.overwritePartitions()
        print(f"SUCCESS: Partition {report_date} overwritten.")
    except:
        writer.create()
        print(f"SUCCESS: Table created and partition {report_date} written.")

    spark.stop()

if __name__ == "__main__":
    main()