import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sha2, upper, trim, lit, concat, year
)

def main():
    if len(sys.argv) < 2:
        raise ValueError("Usage: process_covid_dds.py <REPORT_DATE>")

    report_date = sys.argv[1]
    print(f"--- Processing DDS for date: {report_date} ---")

    spark = SparkSession.builder \
        .appName("Build DDS Covid Star Schema") \
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

    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.dds")
    print(f"DDS processing for {report_date}")

    pop_raw = spark.table("iceberg.raw.country_population")

    dim_location = pop_raw.select(
        sha2(concat(upper(trim(col("country"))), col("year").cast("string")), 256).alias("location_key"),
        col("country").alias("country_name"),
        col("year").alias("population_year"),
        col("population")
    ).distinct()

    dim_location.writeTo("iceberg.dds.dim_location") \
        .using("iceberg") \
        .tableProperty("format-version", "2") \
        .createOrReplace()

    print("DDS: dim_location (Historical Population) updated.")

    ods_df = spark.table("iceberg.ods.daily_country_stats") \
        .filter(col("report_date") == report_date)

    if ods_df.count() == 0:
        print(f"No data in ODS for {report_date}. Exiting.")
        spark.stop()
        return

    ods_enriched = ods_df.withColumn("report_year", year(col("report_date")))

    dim_df = spark.table("iceberg.dds.dim_location")

    fact_df = ods_enriched.join(
        dim_df,
        (ods_enriched.country_region == dim_df.country_name) &
        (ods_enriched.report_year == dim_df.population_year),
        "left"
    ).select(
        col("report_date"),
        col("location_key"),
        col("confirmed"),
        col("deaths"),
        col("recovered"),
        col("active"),
        col("ingestion_ts")
    )

    missing_count = fact_df.filter(col("location_key").isNull()).count()
    if missing_count > 0:
        print(f"WARNING: {missing_count} records failed to join (Missing Population for Year)!")

    target_fact = "iceberg.dds.fact_covid"

    writer = fact_df.writeTo(target_fact) \
        .using("iceberg") \
        .partitionedBy(col("report_date")) \
        .tableProperty("format-version", "2")

    try:
        spark.table(target_fact)
        writer.overwritePartitions()
        print(f"DDS: Fact partition {report_date} overwritten.")
    except Exception as e:
        print(f"Error during overwrite, creating table: {e}")
        writer.create()
        print(f"DDS: Fact table created.")

    spark.stop()

if __name__ == "__main__":
    main()